from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject
from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import ConversationState, GoogleAccount, User, UserStatus
from app.security import StateSigner
from app.services.deepinfra import DeepInfraClient
from app.services.google_calendar import GoogleCalendarService
from app.services.parser import TaskParser


logger = logging.getLogger(__name__)


class ActionCallback(CallbackData, prefix="act"):
    action: str
    option: int | None = None


class TelegramBotService:
    MEMORY_LIMIT = 20
    SUGGESTION_PAGE_SIZE = 3
    STATE_TTL_DAYS = 7

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
        self.dispatcher.callback_query.register(self.handle_action_callback, ActionCallback.filter())

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

    @staticmethod
    def _is_reaction(text: str) -> bool:
        cleaned = text.strip().lower()
        if len(cleaned) <= 18:
            return True
        reactions = (
            "и все",
            "и всё",
            "серьезно",
            "серьёзно",
            "офигеть",
            "капец",
            "жесть",
            "да понял",
            "понял уже",
            "лол",
            "мда",
            "ну ок",
            "ясно",
        )
        return any(item in cleaned for item in reactions)

    @staticmethod
    def _loads_json(payload: str | None, default: Any) -> Any:
        if not payload:
            return default
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return default

    @staticmethod
    def _dumps_json(payload: Any) -> str | None:
        if payload is None:
            return None
        return json.dumps(payload, ensure_ascii=False)

    async def _get_state(self, telegram_id: int, session: AsyncSession) -> ConversationState:
        result = await session.execute(select(ConversationState).where(ConversationState.telegram_id == telegram_id))
        state = result.scalar_one_or_none()
        if state:
            if state.updated_at and state.updated_at.replace(tzinfo=None) < datetime.utcnow() - timedelta(days=self.STATE_TTL_DAYS):
                state.memory_json = "[]"
                state.pending_confirmation_json = None
                state.pending_suggestions_json = None
                await session.commit()
            return state

        state = ConversationState(telegram_id=telegram_id)
        session.add(state)
        await session.flush()
        return state

    async def _remember(
        self,
        telegram_id: int,
        role: str,
        content: str,
        session: AsyncSession,
    ) -> None:
        state = await self._get_state(telegram_id, session)
        history = self._loads_json(state.memory_json, [])
        history.append({"role": role, "content": content})
        state.memory_json = self._dumps_json(history[-self.MEMORY_LIMIT :]) or "[]"
        await session.commit()

    async def _memory_prompt(self, telegram_id: int, session: AsyncSession) -> str:
        state = await self._get_state(telegram_id, session)
        history = self._loads_json(state.memory_json, [])
        if not history:
            return ""
        return "\n".join(f"{item['role']}: {item['content']}" for item in history[-12:])

    async def _get_pending_confirmation(self, telegram_id: int, session: AsyncSession) -> dict[str, Any] | None:
        state = await self._get_state(telegram_id, session)
        return self._loads_json(state.pending_confirmation_json, None)

    async def _set_pending_confirmation(
        self,
        telegram_id: int,
        payload: dict[str, Any] | None,
        session: AsyncSession,
    ) -> None:
        state = await self._get_state(telegram_id, session)
        state.pending_confirmation_json = self._dumps_json(payload)
        await session.commit()

    async def _get_pending_suggestions(self, telegram_id: int, session: AsyncSession) -> dict[str, Any] | None:
        state = await self._get_state(telegram_id, session)
        pending = self._loads_json(state.pending_suggestions_json, None)
        if pending and "offset" not in pending:
            pending["offset"] = 0
        return pending

    async def _set_pending_suggestions(
        self,
        telegram_id: int,
        payload: dict[str, Any] | None,
        session: AsyncSession,
    ) -> None:
        state = await self._get_state(telegram_id, session)
        state.pending_suggestions_json = self._dumps_json(payload)
        await session.commit()

    async def _clear_pending_state(self, telegram_id: int, session: AsyncSession) -> None:
        state = await self._get_state(telegram_id, session)
        state.pending_confirmation_json = None
        state.pending_suggestions_json = None
        await session.commit()

    def _current_suggestion_options(self, pending: dict[str, Any]) -> list[dict[str, Any]]:
        options = pending.get("options", [])
        offset = pending.get("offset", 0)
        return options[offset : offset + self.SUGGESTION_PAGE_SIZE]

    def _confirm_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Создать", callback_data=ActionCallback(action="confirm_create").pack()),
                    InlineKeyboardButton(text="Отмена", callback_data=ActionCallback(action="cancel_create").pack()),
                ]
            ]
        )

    def _suggestion_keyboard(self, pending: dict[str, Any]) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        visible = self._current_suggestion_options(pending)
        offset = pending.get("offset", 0)
        total = len(pending.get("options", []))

        for local_index, option in enumerate(visible):
            rows.append(
                [
                    InlineKeyboardButton(
                        text=option["label"],
                        callback_data=ActionCallback(action="pick_suggestion", option=offset + local_index).pack(),
                    )
                ]
            )

        if offset + self.SUGGESTION_PAGE_SIZE < total:
            rows.append(
                [InlineKeyboardButton(text="Еще варианты", callback_data=ActionCallback(action="more_suggestions").pack())]
            )

        rows.append([InlineKeyboardButton(text="Отмена", callback_data=ActionCallback(action="cancel_create").pack())])
        return InlineKeyboardMarkup(inline_keyboard=rows)

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
            reply = await self._access_message(user)
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return None
        if not user.google_connected:
            reply = await self._access_message(user)
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return None
        return user

    async def _answer_today_schedule(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
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
            reply = "На сегодня календарь пуст. Если хочешь, можем что-нибудь запланировать."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        lines = []
        for event in events[:8]:
            start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
            summary = escape(event.get("summary") or "Без названия")
            lines.append(f"{start.strftime('%H:%M') if start else 'весь день'} — {summary}")
        reply = "На сегодня у тебя вот что:\n" + "\n".join(lines)
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)

    async def _answer_next_event(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
    ) -> None:
        now = datetime.now(self._tz())
        event = await self.calendar_service.get_next_event(
            access_token=access_token,
            refresh_token=refresh_token,
            timezone=self.settings.default_timezone,
        )
        if not event:
            reply = "Пока ничего ближайшего не вижу. Похоже, день свободный."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
        end = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "end")
        summary = escape(event.get("summary") or "Событие без названия")

        if start and end and start <= now <= end:
            reply = f"Сейчас у тебя идет «{summary}» до {end.strftime('%H:%M')}."
        elif start:
            reply = f"Дальше у тебя «{summary}» в {self._format_dt(start)}."
        else:
            reply = f"Дальше у тебя событие «{summary}»."
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)

    async def _create_calendar_entry(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        title: str,
        description: str,
        start_iso: str,
        end_iso: str,
        timezone: str,
    ) -> str:
        return await self.calendar_service.create_event(
            access_token=access_token,
            refresh_token=refresh_token,
            title=title,
            description=description,
            start_iso=start_iso,
            end_iso=end_iso,
            timezone=timezone,
        )

    async def _handle_pending_suggestion(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
    ) -> bool:
        pending = await self._get_pending_suggestions(message.from_user.id, session)
        if not pending:
            return False

        text = message.text or ""
        visible = self._current_suggestion_options(pending)
        if self._is_yes(text) and visible:
            option = visible[0]
            link = await self._create_calendar_entry(
                access_token=access_token,
                refresh_token=refresh_token,
                title=pending["title"],
                description=pending["description"],
                start_iso=option["start_iso"],
                end_iso=option["end_iso"],
                timezone=self.settings.default_timezone,
            )
            await self._set_pending_suggestions(message.from_user.id, None, session)
            reply = f"Супер, поставил на {option['label']}.\n<a href=\"{link}\">Открыть событие в Google Calendar</a>"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", f"Поставил событие на {option['label']}.", session)
            return True

        if self._is_no(text):
            await self._set_pending_suggestions(message.from_user.id, None, session)
            reply = "Окей, не создаю. Напиши другое время, и я попробую еще раз."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True

        return False

    async def _handle_pending_confirmation_update(
        self,
        *,
        message: Message,
        session: AsyncSession,
    ) -> bool:
        pending = await self._get_pending_confirmation(message.from_user.id, session)
        if not pending:
            return False

        text = (message.text or "").strip()
        if not text or self._is_yes(text) or self._is_no(text):
            return False

        revised = await self.parser.revise_event_draft(
            draft_title=pending["title"],
            draft_description=pending["description"],
            draft_start_iso=pending["start_iso"],
            draft_end_iso=pending["end_iso"],
            user_message=text,
        )
        if revised.get("needs_clarification"):
            reply = revised.get("clarification_question") or "Не до конца понял, как поправить черновик."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True

        timezone = revised.get("timezone") or pending["timezone"]
        start_at = self._ensure_tz(datetime.fromisoformat(revised["start_iso"]))
        end_at = self._ensure_tz(datetime.fromisoformat(revised["end_iso"]))
        title = revised.get("title") or pending["title"]
        description = revised.get("description") or pending["description"]

        await self._set_pending_confirmation(
            message.from_user.id,
            {
                "title": title,
                "description": description,
                "start_iso": start_at.isoformat(),
                "end_iso": end_at.isoformat(),
                "timezone": timezone,
            },
            session,
        )
        reply = (
            "Обновил черновик:\n"
            f"• {escape(title)}\n"
            f"• {self._format_dt(start_at)} — {self._format_dt(end_at)}\n\n"
            "Если все так, жми кнопку ниже."
        )
        await message.answer(reply, reply_markup=self._confirm_keyboard())
        await self._remember(message.from_user.id, "assistant", f"Обновил черновик {title} на {self._format_dt(start_at)}.", session)
        return True

    async def _contextual_reply(self, message: Message, text: str, session: AsyncSession) -> bool:
        if not self._is_reaction(text):
            return False

        memory = await self._memory_prompt(message.from_user.id, session)
        if not memory:
            return False

        prompt = [
            {
                "role": "system",
                "content": (
                    "Ты дружелюбный русскоязычный календарный ассистент в Telegram. "
                    "Пользователь только что отреагировал на предыдущий ответ короткой репликой. "
                    "Ответь коротко, по-человечески, на ты, продолжая текущий контекст. "
                    "Не начинай заново перечислять расписание, если тебя об этом не попросили. "
                    "Не извиняйся без причины и не выдумывай факты."
                ),
            },
            {"role": "user", "content": f"Recent conversation:\n{memory}\n\nLast user reaction:\n{text}"},
        ]
        reply = await self.deepinfra.chat_text(prompt, temperature=0.5)
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)
        return True

    async def _route_intent(
        self,
        *,
        message: Message,
        text: str,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
    ) -> None:
        if await self._contextual_reply(message, text, session):
            return

        routing_input = text
        memory = await self._memory_prompt(message.from_user.id, session)
        if memory:
            routing_input = f"Recent conversation:\n{memory}\n\nCurrent message:\n{text}"

        routing = await self.parser.classify_intent(routing_input)
        intent = routing.get("intent", "other")
        logger.info("intent_detected user=%s intent=%s text=%r", message.from_user.id, intent, text[:120])

        if intent == "today_schedule":
            await self._answer_today_schedule(
                message=message,
                access_token=access_token,
                refresh_token=refresh_token,
                session=session,
            )
            return

        if intent == "next_event":
            await self._answer_next_event(
                message=message,
                access_token=access_token,
                refresh_token=refresh_token,
                session=session,
            )
            return

        if intent == "general_help":
            reply = (
                "Я могу создать событие, подсказать планы на сегодня и сказать, что у тебя дальше по календарю.\n"
                "Например:\n"
                "• «созвон завтра в 15:00 на час»\n"
                "• «что у меня сегодня»\n"
                "• «что дальше»"
            )
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        if intent == "clarify":
            reply = routing.get("clarification_question") or "Не до конца понял. Ты хочешь создать событие или посмотреть расписание?"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        if intent == "create_event":
            await self._process_event_request(
                message=message,
                text=text,
                refresh_token=refresh_token,
                access_token=access_token,
                session=session,
            )
            return

        if intent == "cancel_event":
            await self._process_cancel_request(
                message=message,
                text=text,
                refresh_token=refresh_token,
                access_token=access_token,
                session=session,
            )
            return

        await self._friendly_fallback(message, text, session)

    async def _friendly_fallback(self, message: Message, text: str, session: AsyncSession) -> None:
        prompt = [
            {
                "role": "system",
                "content": (
                    "Ты дружелюбный русскоязычный ассистент по календарю в Telegram. "
                    "Отвечай коротко, тепло, на ты, без канцелярита. "
                    "Не выдумывай факты и не говори, что у тебя нет доступа к календарю, если тебя об этом не спрашивали. "
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
        await self._remember(message.from_user.id, "assistant", reply, session)

    async def cmd_start(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._get_or_create_user(message, session)
            await session.commit()
            if user.status == UserStatus.ADMIN.value:
                reply = (
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
                await message.answer(reply)
                await self._remember(message.from_user.id, "assistant", "Показал админские команды.", session)
                return

            access_message = await self._access_message(user)
            if access_message:
                await message.answer(access_message)
                await self._remember(message.from_user.id, "assistant", access_message, session)
                return

            reply = (
                "Готово, все подключено.\n"
                "Можешь писать или отправлять голосовые вроде:\n"
                "• «созвон завтра в 15:00 на час»\n"
                "• «что у меня сегодня»\n"
                "• «что дальше»"
            )
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)

    async def cmd_help(self, message: Message) -> None:
        async with self.session_factory() as session:
            reply = "Я могу создать событие, подсказать планы на сегодня и сказать, что у тебя дальше по календарю."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)

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
                session=session,
            ):
                return

            if await self._handle_pending_confirmation_update(message=message, session=session):
                return

            text = message.text or ""
            await self._remember(message.from_user.id, "user", text, session)
            await self._route_intent(
                message=message,
                text=text,
                access_token=google_account.access_token,
                refresh_token=google_account.refresh_token,
                session=session,
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
            await self._remember(message.from_user.id, "user", transcript, session)
            await self._route_intent(
                message=message,
                text=transcript,
                access_token=google_account.access_token,
                refresh_token=google_account.refresh_token,
                session=session,
            )

    async def handle_action_callback(self, callback: CallbackQuery, callback_data: ActionCallback) -> None:
        user_id = callback.from_user.id
        async with self.session_factory() as session:
            user = await self._load_user_by_telegram_id(user_id, session)
            if not user:
                await callback.answer("Пользователь не найден", show_alert=True)
                return
            google_account = await self._get_google_account(user.id, session)

            if callback_data.action == "cancel_create":
                await self._clear_pending_state(user_id, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                reply = "Окей, ничего не создаю."
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", reply, session)
                await callback.answer()
                return

            if callback_data.action == "confirm_create":
                pending = await self._get_pending_confirmation(user_id, session)
                if not pending:
                    await callback.answer("У меня уже нет этого черновика.", show_alert=True)
                    return
                link = await self._create_calendar_entry(
                    access_token=google_account.access_token,
                    refresh_token=google_account.refresh_token,
                    title=pending["title"],
                    description=pending["description"],
                    start_iso=pending["start_iso"],
                    end_iso=pending["end_iso"],
                    timezone=pending["timezone"],
                )
                await self._set_pending_confirmation(user_id, None, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                reply = f"Готово, событие создал.\n<a href=\"{link}\">Открыть в Google Calendar</a>"
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", f"Создал событие {pending['title']}.", session)
                logger.info("event_created user=%s title=%r start=%s", user_id, pending["title"], pending["start_iso"])
                await callback.answer()
                return

            if callback_data.action == "more_suggestions":
                pending = await self._get_pending_suggestions(user_id, session)
                if not pending:
                    await callback.answer("Подсказки уже устарели.", show_alert=True)
                    return
                total = len(pending.get("options", []))
                next_offset = pending.get("offset", 0) + self.SUGGESTION_PAGE_SIZE
                if next_offset >= total:
                    next_offset = 0
                pending["offset"] = next_offset
                await self._set_pending_suggestions(user_id, pending, session)
                visible = self._current_suggestion_options(pending)
                if not visible:
                    await callback.answer("Больше вариантов пока не нашел.", show_alert=True)
                    return
                first = visible[0]["label"]
                text = (
                    "Вот еще свободные слоты рядом:\n"
                    f"• {visible[0]['label']}"
                    + (f"\n• {visible[1]['label']}" if len(visible) > 1 else "")
                    + (f"\n• {visible[2]['label']}" if len(visible) > 2 else "")
                )
                await callback.message.edit_text(text, reply_markup=self._suggestion_keyboard(pending))
                await callback.answer(f"Показал варианты, начиная с {first}")
                return

            if callback_data.action == "pick_suggestion":
                pending = await self._get_pending_suggestions(user_id, session)
                if not pending:
                    await callback.answer("Подсказка уже устарела.", show_alert=True)
                    return
                index = callback_data.option or 0
                options = pending.get("options", [])
                if index >= len(options):
                    await callback.answer("Не нашел такой вариант.", show_alert=True)
                    return
                option = options[index]
                link = await self._create_calendar_entry(
                    access_token=google_account.access_token,
                    refresh_token=google_account.refresh_token,
                    title=pending["title"],
                    description=pending["description"],
                    start_iso=option["start_iso"],
                    end_iso=option["end_iso"],
                    timezone=self.settings.default_timezone,
                )
                await self._set_pending_suggestions(user_id, None, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                reply = f"Супер, поставил на {option['label']}.\n<a href=\"{link}\">Открыть событие в Google Calendar</a>"
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", f"Поставил событие на {option['label']}.", session)
                logger.info("event_created_from_suggestion user=%s title=%r start=%s", user_id, pending["title"], option["start_iso"])
                await callback.answer()

    async def _process_event_request(
        self,
        *,
        message: Message,
        text: str,
        refresh_token: str,
        access_token: str | None,
        session: AsyncSession,
    ) -> None:
        memory = await self._memory_prompt(message.from_user.id, session)
        parsing_input = text if not memory else f"Recent conversation:\n{memory}\n\nCurrent message:\n{text}"
        parsed = await self.parser.parse_event(parsing_input)
        if parsed.get("needs_clarification"):
            reply = parsed.get("clarification_question") or "Нужно чуть точнее понять дату или время."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return
        if not parsed.get("should_create"):
            await self._friendly_fallback(message, text, session)
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
                count=12,
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
                pending = {
                    "title": parsed.get("title") or "Новое событие",
                    "description": parsed.get("description") or text,
                    "options": options,
                    "offset": 0,
                }
                await self._set_pending_suggestions(message.from_user.id, pending, session)
                visible = self._current_suggestion_options(pending)
                time_text = conflict_start.strftime("%H:%M") if conflict_start else "это время"
                tail = f" Еще рядом есть {visible[1]['label']}." if len(visible) > 1 else ""
                reply = (
                    f"Смотри, в {time_text} у тебя уже стоит «{conflict_title}».\n"
                    f"Зато могу поставить на {visible[0]['label']}.{tail}\n"
                    "Выбирай вариант кнопкой ниже."
                )
                await message.answer(reply, reply_markup=self._suggestion_keyboard(pending))
                await self._remember(message.from_user.id, "assistant", reply, session)
                logger.info("conflict_detected user=%s requested=%s suggestions=%s", message.from_user.id, start_at.isoformat(), len(options))
                return
            reply = "В это время уже есть событие, и рядом я не нашел нормального свободного окна. Подскажи другое время."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        title = parsed.get("title") or "Новое событие"
        description = parsed.get("description") or text
        await self._set_pending_confirmation(
            message.from_user.id,
            {
                "title": title,
                "description": description,
                "start_iso": start_at.isoformat(),
                "end_iso": end_at.isoformat(),
                "timezone": timezone,
            },
            session,
        )
        reply = (
            "Понял так:\n"
            f"• {escape(title)}\n"
            f"• {self._format_dt(start_at)} — {self._format_dt(end_at)}\n\n"
            "Если все так, жми кнопку ниже."
        )
        await message.answer(reply, reply_markup=self._confirm_keyboard())
        await self._remember(message.from_user.id, "assistant", f"Предложил создать {title} на {self._format_dt(start_at)}.", session)

    async def _process_cancel_request(
        self,
        *,
        message: Message,
        text: str,
        refresh_token: str,
        access_token: str | None,
        session: AsyncSession,
    ) -> None:
        parsed = await self.parser.parse_cancel_request(text)
        if parsed.get("needs_clarification"):
            reply = parsed.get("clarification_question") or "Уточни, какое именно событие убрать."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return
        if not parsed.get("should_cancel"):
            await self._friendly_fallback(message, text, session)
            return

        timezone = parsed.get("timezone") or self.settings.default_timezone
        time_min = self._ensure_tz(datetime.fromisoformat(parsed["date_from_iso"]))
        time_max = self._ensure_tz(datetime.fromisoformat(parsed["date_to_iso"]))
        title_query = (parsed.get("title_query") or "").strip().lower()

        events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=time_min,
            time_max=time_max,
            timezone=timezone,
            limit=30,
        )

        matches = []
        for event in events:
            summary = (event.get("summary") or "").lower()
            if not title_query or title_query in summary:
                matches.append(event)

        if not matches:
            reply = "Не нашел подходящее событие в календаре. Попробуй точнее назвать его или дату."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        if len(matches) > 1:
            options = []
            for event in matches[:5]:
                start = self.calendar_service.parse_event_datetime(event, timezone, "start")
                options.append(f"{start.strftime('%d.%m %H:%M') if start else 'весь день'} — {escape(event.get('summary') or 'Без названия')}")
            reply = (
                "Нашел несколько похожих событий:\n"
                + "\n".join(f"• {item}" for item in options)
                + "\n\nНапиши точнее, какое удалить."
            )
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        target = matches[0]
        await self.calendar_service.delete_event(
            access_token=access_token,
            refresh_token=refresh_token,
            event_id=target["id"],
        )
        title = escape(target.get("summary") or "событие")
        reply = f"Готово, удалил «{title}» из календаря."
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)
        logger.info("event_deleted user=%s title=%r", message.from_user.id, target.get("summary"))
