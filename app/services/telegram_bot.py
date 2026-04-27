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
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import ConversationState, GoogleAccount, SelectionState, UsageEvent, User, UserStatus
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
        self.dispatcher.message.register(self.cmd_commands, Command("commands"))
        self.dispatcher.message.register(self.cmd_approve, Command("approve"))
        self.dispatcher.message.register(self.cmd_block, Command("block"))
        self.dispatcher.message.register(self.cmd_pending, Command("pending"))
        self.dispatcher.message.register(self.cmd_users, Command("users"))
        self.dispatcher.message.register(self.cmd_stats, Command("stats"))
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

    def _format_time(self, value: datetime) -> str:
        return value.astimezone(self._tz()).strftime("%H:%M")

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

    async def _get_selection_state(self, telegram_id: int, session: AsyncSession) -> dict[str, Any] | None:
        result = await session.execute(select(SelectionState).where(SelectionState.telegram_id == telegram_id))
        state = result.scalar_one_or_none()
        if not state:
            return None
        return self._loads_json(state.payload_json, None)

    async def _set_selection_state(
        self,
        telegram_id: int,
        payload: dict[str, Any] | None,
        session: AsyncSession,
    ) -> None:
        result = await session.execute(select(SelectionState).where(SelectionState.telegram_id == telegram_id))
        state = result.scalar_one_or_none()
        if payload is None:
            if state:
                await session.delete(state)
                await session.commit()
            return
        if not state:
            state = SelectionState(telegram_id=telegram_id, payload_json=self._dumps_json(payload) or "{}")
            session.add(state)
        else:
            state.payload_json = self._dumps_json(payload) or "{}"
        await session.commit()

    async def _log_usage(
        self,
        session: AsyncSession,
        telegram_id: int,
        kind: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        session.add(
            UsageEvent(
                telegram_id=telegram_id,
                kind=kind,
                payload_json=self._dumps_json(payload),
            )
        )
        await session.commit()

    def _current_suggestion_options(self, pending: dict[str, Any]) -> list[dict[str, Any]]:
        options = pending.get("options", [])
        offset = pending.get("offset", 0)
        return options[offset : offset + self.SUGGESTION_PAGE_SIZE]

    def _describe_pending(self, pending: dict[str, Any]) -> str:
        if pending.get("items"):
            lines: list[str] = []
            for index, item in enumerate(pending["items"], start=1):
                title = escape(item.get("title") or "Без названия")
                start_iso = item.get("start_iso")
                end_iso = item.get("end_iso")
                if start_iso and end_iso:
                    start_at = self._ensure_tz(datetime.fromisoformat(start_iso))
                    end_at = self._ensure_tz(datetime.fromisoformat(end_iso))
                    lines.append(f"{index}. {title} — {self._format_dt(start_at)} — {self._format_dt(end_at)}")
                else:
                    lines.append(f"{index}. {title}")
            return "\n".join(lines)

        title = escape(pending.get("title") or "Без названия")
        start_iso = pending.get("start_iso")
        end_iso = pending.get("end_iso")
        if start_iso and end_iso:
            start_at = self._ensure_tz(datetime.fromisoformat(start_iso))
            end_at = self._ensure_tz(datetime.fromisoformat(end_iso))
            return f"• {title}\n• {self._format_dt(start_at)} — {self._format_dt(end_at)}"
        return f"• {title}"

    def _free_windows_text(self, now: datetime, day_end: datetime, events: list[dict]) -> str:
        busy_ranges: list[tuple[datetime, datetime]] = []
        for event in events:
            start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
            end = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "end")
            if start and end and end > now:
                busy_ranges.append((max(start, now), end))

        busy_ranges.sort(key=lambda item: item[0])
        cursor = now
        windows: list[str] = []
        for start, end in busy_ranges:
            if start > cursor:
                windows.append(f"{self._format_time(cursor)}–{self._format_time(start)}")
            if end > cursor:
                cursor = end
        if cursor < day_end:
            windows.append(f"{self._format_time(cursor)}–{self._format_time(day_end)}")
        return ", ".join(windows[:3])

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

        if pending.get("requested_start_iso") and pending.get("requested_end_iso"):
            rows.append(
                [
                    InlineKeyboardButton(
                        text="Поставить поверх",
                        callback_data=ActionCallback(action="allow_overlap").pack(),
                    )
                ]
            )

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

    def _selection_keyboard(self, options: list[dict[str, Any]]) -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton(text=option["label"], callback_data=ActionCallback(action="pick_selection", option=index).pack())]
            for index, option in enumerate(options[:5])
        ]
        rows.append([InlineKeyboardButton(text="Отмена", callback_data=ActionCallback(action="cancel_create").pack())])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _user_help_text(self) -> str:
        return (
            "Вот что я умею:\n"
            "/start — показать стартовое сообщение\n"
            "/help — показать все команды и примеры\n\n"
            "Что можно писать обычным текстом или голосом:\n"
            "• создать событие: «созвон завтра в 15:00 на час»\n"
            "• перенести событие: «перенеси созвон с Колей на 16:00»\n"
            "• удалить событие: «отмени сегодняшнюю встречу с Колей»\n"
            "• планы на день: «что у меня сегодня»\n"
            "• ближайшее событие: «что дальше»\n"
            "• реакция на черновик: «не в 15, а в 16»\n"
        )

    def _admin_help_text(self) -> str:
        return (
            self._user_help_text()
            + "\nКоманды админа:\n"
            "/approve &lt;telegram_id&gt; — одобрить пользователя\n"
            "/block &lt;telegram_id&gt; — заблокировать пользователя\n"
            "/pending — список ожидающих доступа\n"
            "/users — последние пользователи\n"
            "/stats — короткая статистика по использованию"
        )

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
        free_windows = self._free_windows_text(now, day_end, events)
        prefix = f"На сегодня у тебя {len(events)} {'событие' if len(events) == 1 else 'события' if 2 <= len(events) <= 4 else 'событий'}:\n"
        suffix = f"\n\nСвободные окна дальше сегодня: {free_windows}" if free_windows else ""
        reply = prefix + "\n".join(lines) + suffix
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
        after_events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=(end or now),
            time_max=now + timedelta(days=1),
            timezone=self.settings.default_timezone,
            limit=2,
        )
        follow_up = ""
        if len(after_events) > 1:
            after_next = after_events[1]
            after_next_start = self.calendar_service.parse_event_datetime(after_next, self.settings.default_timezone, "start")
            after_next_summary = escape(after_next.get("summary") or "что-то еще")
            if after_next_start:
                follow_up = f" Потом еще «{after_next_summary}» в {self._format_time(after_next_start)}."

        if start and end and start <= now <= end:
            reply = f"Сейчас у тебя идет «{summary}» до {end.strftime('%H:%M')}.{follow_up}"
        elif start:
            reply = f"Дальше у тебя «{summary}» в {self._format_dt(start)}.{follow_up}"
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
        if pending.get("mode") == "create_batch":
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
                "mode": pending.get("mode", "create"),
                "event_id": pending.get("event_id"),
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
        await self._log_usage(session, message.from_user.id, f"intent:{intent}")

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
                "Я могу создать событие, перенести существующее, подсказать планы на сегодня и сказать, что у тебя дальше по календарю.\n"
                "Например:\n"
                "• «созвон завтра в 15:00 на час»\n"
                "• «перенеси созвон с Колей на 16:00»\n"
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

        if intent == "update_event":
            await self._process_update_request(
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
                "• «перенеси созвон с Колей на 16:00»\n"
                "• «что у меня сегодня»\n"
                "• «что дальше»"
            )
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)

    async def cmd_help(self, message: Message) -> None:
        async with self.session_factory() as session:
            reply = "Я могу создать событие, перенести существующее, подсказать планы на сегодня и сказать, что у тебя дальше по календарю."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)

    async def cmd_commands(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._load_user_by_telegram_id(message.from_user.id, session)
            reply = self._admin_help_text() if user and user.status == UserStatus.ADMIN.value else self._user_help_text()
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

    async def cmd_stats(self, message: Message) -> None:
        if message.from_user.id != self.settings.admin_telegram_id:
            await message.answer("Команда только для админа.")
            return
        async with self.session_factory() as session:
            total_users = (await session.execute(select(func.count()).select_from(User))).scalar_one()
            total_connected = (
                await session.execute(select(func.count()).select_from(User).where(User.google_connected.is_(True)))
            ).scalar_one()
            rows = (
                await session.execute(
                    select(UsageEvent.kind, func.count())
                    .group_by(UsageEvent.kind)
                    .order_by(func.count().desc())
                )
            ).all()
        lines = [f"• {kind}: {count}" for kind, count in rows[:12]]
        reply = (
            f"Пользователей: {total_users}\n"
            f"С подключенным Google: {total_connected}\n\n"
            "События и интенты:\n"
            + ("\n".join(lines) if lines else "Пока пусто.")
        )
        await message.answer(reply)

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
                await self._set_selection_state(user_id, None, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                reply = "Окей, ничего не создаю."
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", reply, session)
                await callback.answer()
                return

            if callback_data.action == "pick_selection":
                selection = await self._get_selection_state(user_id, session)
                if not selection:
                    await callback.answer("Выбор уже устарел.", show_alert=True)
                    return
                index = callback_data.option or 0
                options = selection.get("options", [])
                if index >= len(options):
                    await callback.answer("Не нашел такой вариант.", show_alert=True)
                    return
                option = options[index]
                mode = selection.get("mode")
                await self._set_selection_state(user_id, None, session)
                await callback.message.edit_reply_markup(reply_markup=None)

                if mode == "cancel":
                    await self.calendar_service.delete_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=option["event_id"],
                    )
                    reply = f"Готово, удалил «{escape(option['title'])}» из календаря."
                    await callback.message.answer(reply)
                    await self._remember(user_id, "assistant", reply, session)
                    await self._log_usage(
                        session,
                        user_id,
                        "event_deleted",
                        {"title": option["title"], "event_id": option["event_id"]},
                    )
                    await callback.answer()
                    return

                if mode == "update":
                    pending = {
                        "mode": "update",
                        "event_id": option["event_id"],
                        "title": selection["new_title"] or option["title"],
                        "description": selection["new_description"] or option.get("current_description", ""),
                        "start_iso": selection["new_start_iso"] or option.get("current_start_iso", ""),
                        "end_iso": selection["new_end_iso"] or option.get("current_end_iso", ""),
                        "timezone": selection["timezone"],
                    }
                    await self._set_pending_confirmation(user_id, pending, session)
                    reply = (
                        f"Понял, обновляем «{escape(option['title'])}»:\n"
                        f"{self._describe_pending(pending)}\n\n"
                        "Если все так, жми кнопку ниже."
                    )
                    await callback.message.answer(reply, reply_markup=self._confirm_keyboard())
                    await self._remember(user_id, "assistant", reply, session)
                    await callback.answer()
                    return

            if callback_data.action == "confirm_create":
                pending = await self._get_pending_confirmation(user_id, session)
                if not pending:
                    await callback.answer("У меня уже нет этого черновика.", show_alert=True)
                    return
                mode = pending.get("mode", "create")
                if mode == "create_batch":
                    created: list[tuple[str, str]] = []
                    skipped: list[str] = []
                    for item in pending.get("items", []):
                        item_timezone = item.get("timezone") or self.settings.default_timezone
                        start_at = self._ensure_tz(datetime.fromisoformat(item["start_iso"]))
                        end_at = self._ensure_tz(datetime.fromisoformat(item["end_iso"]))
                        conflicts = await self.calendar_service.find_conflicts(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            start_at=start_at,
                            end_at=end_at,
                            timezone=item_timezone,
                        )
                        if conflicts:
                            skipped.append(f"• {escape(item['title'])} — {self._format_dt(start_at)}")
                            continue

                        link = await self._create_calendar_entry(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            title=item["title"],
                            description=item["description"],
                            start_iso=item["start_iso"],
                            end_iso=item["end_iso"],
                            timezone=item_timezone,
                        )
                        created.append((item["title"], link))

                    await self._set_pending_confirmation(user_id, None, session)
                    await callback.message.edit_reply_markup(reply_markup=None)
                    lines: list[str] = []
                    if created:
                        lines.append("Готово, создал вот это:")
                        lines.extend(f"• <a href=\"{link}\">{escape(title)}</a>" for title, link in created)
                    if skipped:
                        lines.append("")
                        lines.append("Эти события пока пропустил, потому что их слот уже занят:")
                        lines.extend(skipped)
                    if not created and skipped:
                        lines.append("")
                        lines.append("Если хочешь, можем отдельно подобрать для них свободное время.")
                    reply = "\n".join(lines) if lines else "Похоже, тут пока нечего создавать."
                    await callback.message.answer(reply)
                    await self._remember(
                        user_id,
                        "assistant",
                        f"Обработал пакет из {len(pending.get('items', []))} событий: создал {len(created)}, пропустил {len(skipped)}.",
                        session,
                    )
                    await self._log_usage(
                        session,
                        user_id,
                        "event_batch_processed",
                        {"created": len(created), "skipped": len(skipped)},
                    )
                    await callback.answer()
                    return
                if mode == "update":
                    link = await self.calendar_service.update_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=pending["event_id"],
                        title=pending.get("title"),
                        description=pending.get("description"),
                        start_iso=pending.get("start_iso"),
                        end_iso=pending.get("end_iso"),
                        timezone=pending["timezone"],
                    )
                else:
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
                reply = (
                    f"Готово, событие {'обновил' if mode == 'update' else 'создал'}.\n"
                    f"<a href=\"{link}\">Открыть в Google Calendar</a>"
                )
                await callback.message.answer(reply)
                await self._remember(
                    user_id,
                    "assistant",
                    f"{'Обновил' if mode == 'update' else 'Создал'} событие {pending['title']}.",
                    session,
                )
                logger.info(
                    "%s user=%s title=%r start=%s",
                    "event_updated" if mode == "update" else "event_created",
                    user_id,
                    pending["title"],
                    pending.get("start_iso"),
                )
                await self._log_usage(
                    session,
                    user_id,
                    "event_updated" if mode == "update" else "event_created",
                    {"title": pending["title"], "start_iso": pending.get("start_iso")},
                )
                await callback.answer()
                return

            if callback_data.action == "allow_overlap":
                pending = await self._get_pending_suggestions(user_id, session)
                if not pending:
                    await callback.answer("Подсказка уже устарела.", show_alert=True)
                    return
                mode = pending.get("mode", "create")
                requested_start_iso = pending.get("requested_start_iso")
                requested_end_iso = pending.get("requested_end_iso")
                timezone = pending.get("timezone") or self.settings.default_timezone
                if not requested_start_iso or not requested_end_iso:
                    await callback.answer("Не нашел исходный слот.", show_alert=True)
                    return

                if mode == "update":
                    link = await self.calendar_service.update_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=pending["event_id"],
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=requested_start_iso,
                        end_iso=requested_end_iso,
                        timezone=timezone,
                    )
                    reply = f"Ок, перенес поверх на {self._format_dt(self._ensure_tz(datetime.fromisoformat(requested_start_iso)))}.\n<a href=\"{link}\">Открыть событие в Google Calendar</a>"
                    usage_kind = "event_updated_overlap"
                else:
                    link = await self._create_calendar_entry(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=requested_start_iso,
                        end_iso=requested_end_iso,
                        timezone=timezone,
                    )
                    reply = f"Ок, поставил поверх на {self._format_dt(self._ensure_tz(datetime.fromisoformat(requested_start_iso)))}.\n<a href=\"{link}\">Открыть событие в Google Calendar</a>"
                    usage_kind = "event_created_overlap"

                await self._set_pending_suggestions(user_id, None, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", reply, session)
                await self._log_usage(
                    session,
                    user_id,
                    usage_kind,
                    {"title": pending["title"], "start_iso": requested_start_iso},
                )
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
                mode = pending.get("mode", "create")
                if mode == "update":
                    link = await self.calendar_service.update_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=pending["event_id"],
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=option["start_iso"],
                        end_iso=option["end_iso"],
                        timezone=self.settings.default_timezone,
                    )
                else:
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
                verb = "перенес" if mode == "update" else "поставил"
                reply = f"Супер, {verb} на {option['label']}.\n<a href=\"{link}\">Открыть событие в Google Calendar</a>"
                await callback.message.answer(reply)
                await self._remember(
                    user_id,
                    "assistant",
                    f"{'Перенес' if mode == 'update' else 'Поставил'} событие на {option['label']}.",
                    session,
                )
                logger.info(
                    "%s user=%s title=%r start=%s",
                    "event_updated_from_suggestion" if mode == "update" else "event_created_from_suggestion",
                    user_id,
                    pending["title"],
                    option["start_iso"],
                )
                await self._log_usage(
                    session,
                    user_id,
                    "event_updated_from_suggestion" if mode == "update" else "event_created_from_suggestion",
                    {"title": pending["title"], "start_iso": option["start_iso"]},
                )
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
        parsed_multi = await self.parser.parse_events(parsing_input)
        if parsed_multi.get("needs_clarification"):
            reply = parsed_multi.get("clarification_question") or "Нужно чуть точнее понять дату или время."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        multi_events = parsed_multi.get("events") or []
        if parsed_multi.get("should_create") and len(multi_events) > 1:
            items: list[dict[str, Any]] = []
            for raw_item in multi_events:
                start_iso = raw_item.get("start_iso")
                end_iso = raw_item.get("end_iso")
                if not start_iso or not end_iso:
                    reply = "Похоже, в одном из событий не хватает времени. Давай уточним формулировку чуть точнее."
                    await message.answer(reply)
                    await self._remember(message.from_user.id, "assistant", reply, session)
                    return

                timezone = raw_item.get("timezone") or self.settings.default_timezone
                start_at = self._ensure_tz(datetime.fromisoformat(start_iso))
                end_at = self._ensure_tz(datetime.fromisoformat(end_iso))
                items.append(
                    {
                        "title": raw_item.get("title") or "Новое событие",
                        "description": raw_item.get("description") or text,
                        "start_iso": start_at.isoformat(),
                        "end_iso": end_at.isoformat(),
                        "timezone": timezone,
                    }
                )

            await self._set_pending_confirmation(
                message.from_user.id,
                {
                    "mode": "create_batch",
                    "items": items,
                },
                session,
            )
            reply = (
                "Понял так, нужно создать сразу несколько событий:\n"
                f"{self._describe_pending({'items': items})}\n\n"
                "Если всё так, жми кнопку ниже."
            )
            await message.answer(reply, reply_markup=self._confirm_keyboard())
            await self._remember(message.from_user.id, "assistant", f"Предложил создать {len(items)} событий одним пакетом.", session)
            return

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
                    "mode": "create",
                    "title": parsed.get("title") or "Новое событие",
                    "description": parsed.get("description") or text,
                    "requested_start_iso": start_at.isoformat(),
                    "requested_end_iso": end_at.isoformat(),
                    "timezone": timezone,
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
                    "Если хочешь, могу и поставить прямо поверх.\n"
                    "Выбирай вариант кнопкой ниже."
                )
                await message.answer(reply, reply_markup=self._suggestion_keyboard(pending))
                await self._remember(message.from_user.id, "assistant", reply, session)
                logger.info("conflict_detected user=%s requested=%s suggestions=%s", message.from_user.id, start_at.isoformat(), len(options))
                await self._log_usage(
                    session,
                    message.from_user.id,
                    "conflict_detected",
                    {"requested": start_at.isoformat(), "count": len(options)},
                )
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
                "mode": "create",
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

    async def _process_update_request(
        self,
        *,
        message: Message,
        text: str,
        refresh_token: str,
        access_token: str | None,
        session: AsyncSession,
    ) -> None:
        parsed = await self.parser.parse_update_request(text)
        if parsed.get("needs_clarification"):
            reply = parsed.get("clarification_question") or "Уточни, что именно хочешь перенести или изменить."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return
        if not parsed.get("should_update"):
            await self._friendly_fallback(message, text, session)
            return

        timezone = parsed.get("timezone") or self.settings.default_timezone
        search_from = self._ensure_tz(datetime.fromisoformat(parsed["search_from_iso"]))
        search_to = self._ensure_tz(datetime.fromisoformat(parsed["search_to_iso"]))
        title_query = (parsed.get("title_query") or "").strip().lower()

        events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=search_from,
            time_max=search_to,
            timezone=timezone,
            limit=30,
        )
        matches = []
        for event in events:
            summary = (event.get("summary") or "").lower()
            if not title_query or title_query in summary:
                matches.append(event)

        if not matches:
            reply = "Не нашел подходящее событие для переноса. Назови его чуть точнее."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        if len(matches) > 1:
            options = []
            for event in matches[:5]:
                start = self.calendar_service.parse_event_datetime(event, timezone, "start")
                options.append(
                    {
                        "event_id": event["id"],
                        "title": event.get("summary") or "Без названия",
                        "current_description": event.get("description") or "",
                        "current_start_iso": start.isoformat() if start else "",
                        "current_end_iso": (
                            self.calendar_service.parse_event_datetime(event, timezone, "end").isoformat()
                            if self.calendar_service.parse_event_datetime(event, timezone, "end")
                            else ""
                        ),
                        "label": f"{start.strftime('%d.%m %H:%M') if start else 'весь день'} — {escape(event.get('summary') or 'Без названия')}",
                    }
                )
            await self._set_selection_state(
                message.from_user.id,
                {
                    "mode": "update",
                    "timezone": timezone,
                    "new_title": parsed.get("new_title") or "",
                    "new_description": parsed.get("new_description") or "",
                    "new_start_iso": parsed.get("new_start_iso") or "",
                    "new_end_iso": parsed.get("new_end_iso") or "",
                    "options": options,
                },
                session,
            )
            reply = "Нашел несколько похожих событий для переноса. Выбери нужное кнопкой ниже."
            await message.answer(reply, reply_markup=self._selection_keyboard(options))
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        target = matches[0]
        current_title = target.get("summary") or "Событие без названия"
        current_description = target.get("description") or ""
        current_start = self.calendar_service.parse_event_datetime(target, timezone, "start")
        current_end = self.calendar_service.parse_event_datetime(target, timezone, "end")
        if not current_start or not current_end:
            reply = "Это событие выглядит нестандартно, я пока не умею его безопасно переносить."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        new_title = parsed.get("new_title") or current_title
        new_description = parsed.get("new_description") or current_description
        new_start_iso = parsed.get("new_start_iso") or current_start.isoformat()
        new_end_iso = parsed.get("new_end_iso") or current_end.isoformat()
        new_start = self._ensure_tz(datetime.fromisoformat(new_start_iso))
        new_end = self._ensure_tz(datetime.fromisoformat(new_end_iso))

        conflicts = await self.calendar_service.find_conflicts(
            access_token=access_token,
            refresh_token=refresh_token,
            start_at=new_start,
            end_at=new_end,
            timezone=timezone,
        )
        conflicts = [event for event in conflicts if event.get("id") != target.get("id")]
        if conflicts:
            conflict = conflicts[0]
            conflict_start = self.calendar_service.parse_event_datetime(conflict, timezone, "start")
            conflict_title = escape(conflict.get("summary") or "другая встреча")
            suggestions = await self.calendar_service.suggest_free_slots(
                access_token=access_token,
                refresh_token=refresh_token,
                desired_start=new_start,
                desired_end=new_end,
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
                    "mode": "update",
                    "event_id": target["id"],
                    "title": new_title,
                    "description": new_description,
                    "requested_start_iso": new_start.isoformat(),
                    "requested_end_iso": new_end.isoformat(),
                    "timezone": timezone,
                    "options": options,
                    "offset": 0,
                }
                await self._set_pending_suggestions(message.from_user.id, pending, session)
                visible = self._current_suggestion_options(pending)
                time_text = conflict_start.strftime("%H:%M") if conflict_start else "это время"
                tail = f" Еще рядом есть {visible[1]['label']}." if len(visible) > 1 else ""
                reply = (
                    f"Хотел перенести «{escape(current_title)}», но в {time_text} уже стоит «{conflict_title}».\n"
                    f"Могу вместо этого перенести на {visible[0]['label']}.{tail}\n"
                    "Выбирай вариант кнопкой ниже."
                )
                await message.answer(reply, reply_markup=self._suggestion_keyboard(pending))
                await self._remember(message.from_user.id, "assistant", reply, session)
                logger.info("update_conflict_detected user=%s event=%r requested=%s", message.from_user.id, current_title, new_start.isoformat())
                await self._log_usage(
                    session,
                    message.from_user.id,
                    "update_conflict_detected",
                    {"title": current_title, "requested": new_start.isoformat()},
                )
                return

            reply = "Нашел само событие, но в новое время уже есть пересечение. Подскажи другой слот."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        await self._set_pending_confirmation(
            message.from_user.id,
            {
                "mode": "update",
                "event_id": target["id"],
                "title": new_title,
                "description": new_description,
                "start_iso": new_start.isoformat(),
                "end_iso": new_end.isoformat(),
                "timezone": timezone,
            },
            session,
        )
        reply = (
            f"Понял так, обновляем «{escape(current_title)}»:\n"
            f"{self._describe_pending({'title': new_title, 'start_iso': new_start.isoformat(), 'end_iso': new_end.isoformat()})}\n\n"
            "Если все так, жми кнопку ниже."
        )
        await message.answer(reply, reply_markup=self._confirm_keyboard())
        await self._remember(message.from_user.id, "assistant", f"Предложил обновить {current_title} на {self._format_dt(new_start)}.", session)

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
                options.append(
                    {
                        "event_id": event["id"],
                        "title": event.get("summary") or "Без названия",
                        "label": f"{start.strftime('%d.%m %H:%M') if start else 'весь день'} — {escape(event.get('summary') or 'Без названия')}",
                    }
                )
            await self._set_selection_state(
                message.from_user.id,
                {
                    "mode": "cancel",
                    "options": options,
                },
                session,
            )
            reply = "Нашел несколько похожих событий. Выбери, что именно удалить."
            await message.answer(reply, reply_markup=self._selection_keyboard(options))
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
