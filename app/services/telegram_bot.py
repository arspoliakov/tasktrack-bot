from __future__ import annotations

from datetime import datetime, timedelta
from html import escape
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import GoogleAccount, User, UserStatus
from app.security import StateSigner
from app.services.deepinfra import DeepInfraClient
from app.services.google_calendar import GoogleCalendarService
from app.services.parser import TaskParser


class TelegramBotService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = get_settings()
        self.bot = Bot(
            self.settings.telegram_bot_token,
            default=DefaultBotProperties(parse_mode="HTML"),
        )
        self.dispatcher = Dispatcher()
        self.session_factory = session_factory
        self.signer = StateSigner()
        self.calendar_service = GoogleCalendarService()
        self.deepinfra = DeepInfraClient()
        self.parser = TaskParser(self.deepinfra)
        self.pending_suggestions: dict[int, dict] = {}
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.dispatcher.message.register(self.cmd_start, Command("start"))
        self.dispatcher.message.register(self.cmd_help, Command("help"))
        self.dispatcher.message.register(self.cmd_approve, Command("approve"))
        self.dispatcher.message.register(self.cmd_block, Command("block"))
        self.dispatcher.message.register(self.cmd_pending, Command("pending"))
        self.dispatcher.message.register(self.cmd_users, Command("users"))
        self.dispatcher.message.register(self.handle_voice, F.voice)
        self.dispatcher.message.register(self.handle_text, F.text)

    async def start(self) -> None:
        await self.dispatcher.start_polling(self.bot)

    async def stop(self) -> None:
        await self.bot.session.close()

    def _tz(self) -> ZoneInfo:
        return ZoneInfo(self.settings.default_timezone)

    def _format_dt(self, value: datetime) -> str:
        return value.astimezone(self._tz()).strftime("%d.%m %H:%M")

    def _ensure_tz(self, value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=self._tz())

    @staticmethod
    def _is_yes(text: str) -> bool:
        return text.strip().lower() in {"да", "ага", "ок", "окей", "давай", "yes", "создавай"}

    @staticmethod
    def _is_no(text: str) -> bool:
        return text.strip().lower() in {"нет", "неа", "отмена", "cancel", "no"}

    async def _get_or_create_user(self, message: Message, session: AsyncSession) -> User:
        telegram_id = message.from_user.id
        result = await session.execute(select(User).where(User.telegram_id == telegram_id))
        user = result.scalar_one_or_none()
        if user:
            user.username = message.from_user.username
            user.first_name = message.from_user.first_name
            return user

        status = UserStatus.ADMIN.value if telegram_id == self.settings.admin_telegram_id else UserStatus.PENDING.value
        approved_by = telegram_id if status == UserStatus.ADMIN.value else None
        approved_at = datetime.utcnow() if status == UserStatus.ADMIN.value else None
        user = User(
            telegram_id=telegram_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            status=status,
            approved_by=approved_by,
            approved_at=approved_at,
        )
        session.add(user)
        await session.flush()
        return user

    async def _load_user_by_telegram_id(self, telegram_id: int, session: AsyncSession) -> User | None:
        result = await session.execute(select(User).where(User.telegram_id == telegram_id))
        return result.scalar_one_or_none()

    async def _get_google_account(self, user_id: int, session: AsyncSession) -> GoogleAccount:
        return (await session.execute(select(GoogleAccount).where(GoogleAccount.user_id == user_id))).scalar_one()

    def _connect_url(self, telegram_id: int) -> str:
        state = self.signer.dumps({"telegram_id": telegram_id})
        return self.calendar_service.build_authorize_url(state)

    async def _access_message(self, user: User) -> str:
        if user.status == UserStatus.BLOCKED.value:
            return "Доступ заблокирован. Если это ошибка, напиши администратору."
        if user.status == UserStatus.PENDING.value:
            return (
                "Пока доступ не одобрен.\n"
                f"Твой Telegram ID: <code>{user.telegram_id}</code>\n"
                "Отправь его администратору, и он откроет доступ."
            )
        if not user.google_connected:
            connect_url = self._connect_url(user.telegram_id)
            return (
                "Остался последний шаг — подключить Google Calendar.\n"
                "Это нужно, чтобы я мог смотреть твое расписание, подсказывать свободные окна и создавать события.\n"
                f"<a href=\"{connect_url}\">Подключить Google Calendar</a>"
            )
        return ""

    async def _ensure_access(self, message: Message, session: AsyncSession) -> User | None:
        user = await self._get_or_create_user(message, session)
        await session.commit()
        if user.status not in (UserStatus.ADMIN.value, UserStatus.APPROVED.value):
            await message.answer(await self._access_message(user))
            return None
        if not user.google_connected:
            await message.answer(await self._access_message(user))
            return None
        return user

    async def _answer_today_schedule(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
    ) -> None:
        now = datetime.now(self._tz())
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=day_start,
            time_max=day_end,
            timezone=self.settings.default_timezone,
            limit=20,
        )
        if not events:
            await message.answer("На сегодня в календаре пусто. Если хочешь, можем что-нибудь запланировать.")
            return

        lines = []
        for event in events[:8]:
            start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
            summary = escape(event.get("summary") or "Без названия")
            lines.append(f"{start.strftime('%H:%M') if start else 'весь день'} — {summary}")
        await message.answer("На сегодня у тебя вот что:\n" + "\n".join(lines))

    async def _answer_next_event(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
    ) -> None:
        now = datetime.now(self._tz())
        event = await self.calendar_service.get_next_event(
            access_token=access_token,
            refresh_token=refresh_token,
            timezone=self.settings.default_timezone,
        )
        if not event:
            await message.answer("Пока ничего ближайшего не вижу. Выглядит так, будто день свободный.")
            return

        start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
        end = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "end")
        summary = escape(event.get("summary") or "Событие без названия")

        if start and end and start <= now <= end:
            await message.answer(f"Сейчас у тебя идет «{summary}» до {end.strftime('%H:%M')}.")
            return
        if start:
            await message.answer(f"Дальше у тебя «{summary}» в {self._format_dt(start)}.")
            return
        await message.answer(f"Дальше у тебя событие «{summary}».")

    async def _handle_pending_suggestion(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
    ) -> bool:
        pending = self.pending_suggestions.get(message.from_user.id)
        if not pending:
            return False

        text = message.text or ""
        if self._is_yes(text):
            option = pending["options"][0]
            link = await self.calendar_service.create_event(
                access_token=access_token,
                refresh_token=refresh_token,
                title=pending["title"],
                description=pending["description"],
                start_iso=option["start_iso"],
                end_iso=option["end_iso"],
                timezone=self.settings.default_timezone,
            )
            self.pending_suggestions.pop(message.from_user.id, None)
            await message.answer(
                f"Супер, поставил на {option['label']}.\n"
                f"<a href=\"{link}\">Открыть событие в Google Calendar</a>"
            )
            return True

        if self._is_no(text):
            self.pending_suggestions.pop(message.from_user.id, None)
            await message.answer("Окей, не создаю. Напиши другое время, и я попробую еще раз.")
            return True

        return False

    async def _route_intent(
        self,
        *,
        message: Message,
        text: str,
        access_token: str | None,
        refresh_token: str,
    ) -> None:
        routing = await self.parser.classify_intent(text)
        intent = routing.get("intent", "other")

        if intent == "today_schedule":
            await self._answer_today_schedule(
                message=message,
                access_token=access_token,
                refresh_token=refresh_token,
            )
            return

        if intent == "next_event":
            await self._answer_next_event(
                message=message,
                access_token=access_token,
                refresh_token=refresh_token,
            )
            return

        if intent == "general_help":
            await message.answer(
                "Я могу создать событие, подсказать планы на сегодня и сказать, что у тебя дальше по календарю.\n"
                "Например:\n"
                "• «созвон завтра в 15:00 на час»\n"
                "• «что у меня сегодня»\n"
                "• «что дальше»"
            )
            return

        if intent == "clarify":
            await message.answer(routing.get("clarification_question") or "Не до конца понял. Ты хочешь создать событие или посмотреть расписание?")
            return

        if intent == "create_event":
            await self._process_event_request(
                message=message,
                text=text,
                refresh_token=refresh_token,
                access_token=access_token,
            )
            return

        await self._friendly_fallback(message, text)

    async def _friendly_fallback(self, message: Message, text: str) -> None:
        prompt = [
            {
                "role": "system",
                "content": (
                    "Ты дружелюбный русскоязычный ассистент по календарю в Telegram. "
                    "Отвечай коротко, тепло, на ты, без канцелярита. "
                    "Не выдумывай факты и не говори, что у тебя нет доступа к календарю, если тебя об этом прямо не спросили. "
                    "Скажи, что ты можешь: создать событие, подсказать планы на сегодня, сказать что дальше по календарю. "
                    "Предложи пользователю сформулировать запрос проще, с примерами. "
                    "Если пользователь жалуется на распознавание, извинись и коротко скажи, что теперь слушаешь по-русски. "
                    "Отвечай только по-русски."
                ),
            },
            {"role": "user", "content": text},
        ]
        reply = await self.deepinfra.chat_text(prompt, temperature=0.4)
        await message.answer(reply)

    async def cmd_start(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._get_or_create_user(message, session)
            await session.commit()
            if user.status == UserStatus.ADMIN.value:
                await message.answer(
                    "Ты админ.\n"
                    "Вот что умею:\n"
                    "• создавать события\n"
                    "• подсказывать, что у тебя сегодня\n"
                    "• говорить, что дальше по календарю\n\n"
                    "Команды админа:\n"
                    "/approve &lt;telegram_id&gt;\n"
                    "/block &lt;telegram_id&gt;\n"
                    "/pending\n"
                    "/users"
                )
                return

            access_message = await self._access_message(user)
            if access_message:
                await message.answer(access_message)
                return

            await message.answer(
                "Готово, все подключено.\n"
                "Можешь писать или отправлять голосовые вроде:\n"
                "• «созвон завтра в 15:00 на час»\n"
                "• «что у меня сегодня»\n"
                "• «что дальше»"
            )

    async def cmd_help(self, message: Message) -> None:
        await message.answer(
            "Я могу создать событие, подсказать планы на сегодня и сказать, что у тебя дальше по календарю."
        )

    async def cmd_approve(self, message: Message, command: CommandObject) -> None:
        if message.from_user.id != self.settings.admin_telegram_id:
            await message.answer("Только админ может одобрять пользователей.")
            return
        if not command.args or not command.args.isdigit():
            await message.answer("Используй: /approve &lt;telegram_id&gt;")
            return

        target_tg_id = int(command.args)
        async with self.session_factory() as session:
            user = await self._load_user_by_telegram_id(target_tg_id, session)
            if not user:
                user = User(
                    telegram_id=target_tg_id,
                    status=UserStatus.APPROVED.value,
                    approved_by=self.settings.admin_telegram_id,
                    approved_at=datetime.utcnow(),
                )
                session.add(user)
            else:
                user.status = UserStatus.APPROVED.value
                user.approved_by = self.settings.admin_telegram_id
                user.approved_at = datetime.utcnow()
            await session.commit()

        connect_url = self._connect_url(target_tg_id)
        await message.answer(f"Пользователь <code>{target_tg_id}</code> одобрен.")
        await self.bot.send_message(
            target_tg_id,
            "Доступ открыт.\n"
            "Теперь подключи Google Calendar — это нужно, чтобы я мог смотреть твое расписание и создавать события.\n"
            f"<a href=\"{connect_url}\">Подключить Google Calendar</a>",
        )

    async def cmd_block(self, message: Message, command: CommandObject) -> None:
        if message.from_user.id != self.settings.admin_telegram_id:
            await message.answer("Только админ может блокировать пользователей.")
            return
        if not command.args or not command.args.isdigit():
            await message.answer("Используй: /block &lt;telegram_id&gt;")
            return

        target_tg_id = int(command.args)
        async with self.session_factory() as session:
            user = await self._load_user_by_telegram_id(target_tg_id, session)
            if not user:
                await message.answer("Пользователь не найден.")
                return
            user.status = UserStatus.BLOCKED.value
            await session.commit()

        await message.answer(f"Пользователь <code>{target_tg_id}</code> заблокирован.")
        await self.bot.send_message(target_tg_id, "Доступ к боту заблокирован.")

    async def cmd_pending(self, message: Message) -> None:
        if message.from_user.id != self.settings.admin_telegram_id:
            await message.answer("Команда только для админа.")
            return
        async with self.session_factory() as session:
            result = await session.execute(select(User).where(User.status == UserStatus.PENDING.value))
            users = result.scalars().all()
        if not users:
            await message.answer("Ожидающих пользователей нет.")
            return
        lines = [f"<code>{u.telegram_id}</code> | @{u.username or '-'} | {escape(u.first_name or '-')}" for u in users]
        await message.answer("Pending:\n" + "\n".join(lines))

    async def cmd_users(self, message: Message) -> None:
        if message.from_user.id != self.settings.admin_telegram_id:
            await message.answer("Команда только для админа.")
            return
        async with self.session_factory() as session:
            result = await session.execute(select(User).order_by(User.created_at.desc()).limit(20))
            users = result.scalars().all()
        if not users:
            await message.answer("Пользователей пока нет.")
            return
        lines = [
            f"<code>{u.telegram_id}</code> | {u.status} | google={'yes' if u.google_connected else 'no'} | @{u.username or '-'}"
            for u in users
        ]
        await message.answer("Последние пользователи:\n" + "\n".join(lines))

    async def handle_text(self, message: Message) -> None:
        if (message.text or "").startswith("/"):
            return

        async with self.session_factory() as session:
            user = await self._ensure_access(message, session)
            if not user:
                return
            google_account = await self._get_google_account(user.id, session)

        if await self._handle_pending_suggestion(
            message=message,
            access_token=google_account.access_token,
            refresh_token=google_account.refresh_token,
        ):
            return

        await self._route_intent(
            message=message,
            text=message.text or "",
            access_token=google_account.access_token,
            refresh_token=google_account.refresh_token,
        )

    async def handle_voice(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._ensure_access(message, session)
            if not user:
                return
            google_account = await self._get_google_account(user.id, session)

        await message.answer("Сек, расшифровываю голосовое...")
        file = await self.bot.get_file(message.voice.file_id)
        file_bytes = await self.bot.download_file(file.file_path)
        transcript = await self.deepinfra.transcribe("voice.ogg", file_bytes.read())
        await message.answer(f"Вот что я услышал:\n<blockquote>{escape(transcript)}</blockquote>")
        await self._route_intent(
            message=message,
            text=transcript,
            access_token=google_account.access_token,
            refresh_token=google_account.refresh_token,
        )

    async def _process_event_request(
        self,
        *,
        message: Message,
        text: str,
        refresh_token: str,
        access_token: str | None,
    ) -> None:
        parsed = await self.parser.parse_event(text)
        if parsed.get("needs_clarification"):
            await message.answer(parsed.get("clarification_question") or "Нужно чуть точнее понять дату или время.")
            return
        if not parsed.get("should_create"):
            await self._friendly_fallback(message, text)
            return

        timezone = parsed.get("timezone") or self.settings.default_timezone
        start_at = self._ensure_tz(datetime.fromisoformat(parsed["start_iso"]))
        end_at = self._ensure_tz(datetime.fromisoformat(parsed["end_iso"]))
        conflicts = await self.calendar_service.find_conflicts(
            access_token=access_token,
            refresh_token=refresh_token,
            start_at=start_at,
            end_at=end_at,
            timezone=timezone,
        )
        if conflicts:
            conflict = conflicts[0]
            conflict_start = self.calendar_service.parse_event_datetime(conflict, timezone, "start")
            conflict_title = escape(conflict.get("summary") or "другая встреча")
            suggestions = await self.calendar_service.suggest_free_slots(
                access_token=access_token,
                refresh_token=refresh_token,
                desired_start=start_at,
                desired_end=end_at,
                timezone=timezone,
            )
            if suggestions:
                options = [
                    {
                        "label": self._format_dt(slot_start),
                        "start_iso": slot_start.isoformat(),
                        "end_iso": slot_end.isoformat(),
                    }
                    for slot_start, slot_end in suggestions
                ]
                self.pending_suggestions[message.from_user.id] = {
                    "title": parsed.get("title") or "Новое событие",
                    "description": parsed.get("description") or text,
                    "options": options,
                }
                time_text = conflict_start.strftime("%H:%M") if conflict_start else "это время"
                tail = f" Если удобнее, еще есть {options[1]['label']}." if len(options) > 1 else ""
                await message.answer(
                    f"Смотри, в {time_text} у тебя уже стоит «{conflict_title}».\n"
                    f"Зато могу поставить на {options[0]['label']}.{tail}\n"
                    "Если ок — просто напиши «да»."
                )
                return
            await message.answer(
                "В это время уже есть событие, и рядом я не нашел нормального свободного окна. Подскажи другое время."
            )
            return

        link = await self.calendar_service.create_event(
            access_token=access_token,
            refresh_token=refresh_token,
            title=parsed.get("title") or "Новое событие",
            description=parsed.get("description") or text,
            start_iso=start_at.isoformat(),
            end_iso=end_at.isoformat(),
            timezone=timezone,
        )
        await message.answer(
            "Готово, событие создал.\n"
            f"<a href=\"{link}\">Открыть в Google Calendar</a>"
        )
