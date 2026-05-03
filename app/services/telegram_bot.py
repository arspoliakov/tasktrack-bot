from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject
from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import ConversationState, EventReminder, GoogleAccount, SelectionState, TomorrowDigestDelivery, UndoAction, UsageEvent, User, UserPreference, UserStatus
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
        self.dispatcher.message.register(self.cmd_undo, Command("undo"))
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

    @staticmethod
    def _normalize_event_text(value: str) -> str:
        cleaned = value.lower().replace("ё", "е")
        cleaned = re.sub(r"[^a-zа-я0-9\s]", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        aliases = {
            "уник": "универ",
            "уника": "универ",
            "унику": "универ",
            "уником": "универ",
            "универ": "универ",
            "универа": "универ",
            "универу": "универ",
            "универс": "универ",
            "университет": "универ",
            "качалка": "зал",
            "качалку": "зал",
            "качалке": "зал",
            "зал": "зал",
            "зала": "зал",
            "зале": "зал",
            "треня": "тренировка",
            "тренька": "тренировка",
            "треню": "тренировка",
            "тренировка": "тренировка",
            "тренировки": "тренировка",
            "созвон": "созвон",
            "созвона": "созвон",
            "созвону": "созвон",
            "кол": "коля",
            "колей": "коля",
            "колю": "коля",
            "прогулка": "гулять",
            "прогулку": "гулять",
            "прогулки": "гулять",
            "погулять": "гулять",
            "погуляю": "гулять",
            "гулять": "гулять",
            "выставка": "выставка",
            "выставки": "выставка",
            "выставку": "выставка",
        }
        suffixes = (
            "иями",
            "ями",
            "ами",
            "иях",
            "ях",
            "ого",
            "ему",
            "ому",
            "ыми",
            "ими",
            "его",
            "ая",
            "яя",
            "ой",
            "ей",
            "ий",
            "ый",
            "ое",
            "ее",
            "ам",
            "ям",
            "ах",
            "ях",
            "ом",
            "ем",
            "ов",
            "ев",
            "ы",
            "и",
            "а",
            "я",
            "е",
            "у",
            "ю",
            "о",
        )
        tokens: list[str] = []
        for raw_token in cleaned.split():
            token = aliases.get(raw_token, raw_token)
            if token == raw_token and len(token) >= 5:
                for suffix in suffixes:
                    if token.endswith(suffix) and len(token) - len(suffix) >= 4:
                        token = token[: -len(suffix)]
                        break
            token = aliases.get(token, token)
            tokens.append(token)
        return " ".join(tokens)

    def _phrase_similarity(self, left: str, right: str) -> float:
        normalized_left = self._normalize_event_text(left)
        normalized_right = self._normalize_event_text(right)
        if not normalized_left or not normalized_right:
            return 0.0
        if normalized_left == normalized_right:
            return 1.0
        return SequenceMatcher(None, normalized_left, normalized_right).ratio()

    def _contains_phrase(self, text: str, phrases: tuple[str, ...], threshold: float = 0.84) -> bool:
        normalized_text = self._normalize_event_text(text)
        if not normalized_text:
            return False
        text_tokens = normalized_text.split()
        joined = " ".join(text_tokens)
        for phrase in phrases:
            normalized_phrase = self._normalize_event_text(phrase)
            if not normalized_phrase:
                continue
            if normalized_phrase in joined:
                return True
            phrase_tokens = normalized_phrase.split()
            if phrase_tokens and all(
                any(
                    token == candidate
                    or candidate.startswith(token)
                    or token.startswith(candidate)
                    or SequenceMatcher(None, token, candidate).ratio() >= 0.82
                    for candidate in text_tokens
                )
                for token in phrase_tokens
            ):
                return True
            if self._phrase_similarity(joined, normalized_phrase) >= threshold:
                return True
        return False

    @staticmethod
    def _extract_event_lookup_query(text: str) -> str | None:
        lowered = (text or "").strip()
        if not lowered:
            return None

        value = lowered
        value = re.sub(r"^(а\s+)?(что\s+насчет|что\s+насчёт|что\s+по|а\s+что\s+по)\s+", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\b(во\s+сколько|во\s+скоко|когда|на\s+когда)\b", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\b(сегодня|завтра|послезавтра)\b", "", value, flags=re.IGNORECASE)
        value = re.sub(r"^(а\s+)", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value).strip(" ?!.,-")
        return value or None

    def _looks_like_event_lookup(self, text: str) -> bool:
        cleaned = text.strip().lower()
        if not cleaned:
            return False
        if any(phrase in cleaned for phrase in ("а потом", "что дальше", "что потом", "после этого", "до этого")):
            return False
        if re.search(r"\b(во\s+сколько|во\s+скоко|когда|на\s+когда)\b", cleaned):
            return bool(self._extract_event_lookup_query(cleaned))
        if cleaned.startswith("а "):
            query = self._extract_event_lookup_query(cleaned)
            if query and len(query.split()) <= 5:
                return True
        return False

    def _extract_schedule_target_date(self, text: str, now: datetime) -> tuple[datetime, str]:
        cleaned = (text or "").lower().replace("ё", "е")
        if "послезавтра" in cleaned:
            target = now + timedelta(days=2)
            return target, "послезавтра"
        if "завтра" in cleaned:
            target = now + timedelta(days=1)
            return target, "завтра"
        if "сегодня" in cleaned or "седня" in cleaned:
            return now, "сегодня"

        weekday_forms = {
            0: ("понедельник", "понедельника", "понедельникe", "понедельнику"),
            1: ("вторник", "вторника", "вторнику"),
            2: ("среду", "среда", "среды", "среде"),
            3: ("четверг", "четверга", "четвергу"),
            4: ("пятницу", "пятница", "пятницы", "пятнице"),
            5: ("субботу", "суббота", "субботы", "субботе"),
            6: ("воскресенье", "воскресенья", "воскресенью"),
        }
        for weekday, forms in weekday_forms.items():
            if any(form in cleaned for form in forms):
                days_ahead = (weekday - now.weekday()) % 7
                if days_ahead == 0 and "на " in cleaned:
                    days_ahead = 7
                target = now + timedelta(days=days_ahead)
                label = forms[0]
                return target, label
        return now, "сегодня"

    def _event_match_score(self, query: str, summary: str) -> float:
        normalized_query = self._normalize_event_text(query)
        normalized_summary = self._normalize_event_text(summary)
        if not normalized_query:
            return 1.0
        if normalized_query in normalized_summary:
            return 1.0
        query_tokens = set(normalized_query.split())
        summary_tokens = set(normalized_summary.split())
        overlap = len(query_tokens & summary_tokens)
        token_score = overlap / max(1, len(query_tokens))
        similarity = SequenceMatcher(None, normalized_query, normalized_summary).ratio()
        prefix = normalized_query[:3]
        prefix_bonus = 0.15 if len(prefix) == 3 and normalized_summary.startswith(prefix) else 0.0
        return max(similarity, token_score + prefix_bonus)

    @staticmethod
    def _extract_after_event_query(text: str) -> str | None:
        lowered = (text or "").strip()
        if not lowered:
            return None

        quoted_match = re.search(r'после\s+["«](.+?)["»]', lowered, re.IGNORECASE)
        if quoted_match:
            return quoted_match.group(1).strip()

        match = re.search(r"после\s+(.+)", lowered, re.IGNORECASE)
        if not match:
            return None

        value = match.group(1).strip()
        value = re.sub(r"^(этой|этого|этот)\s+", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\b(сегодня|завтра|днем|днём|вечером|ночью|утром)\b", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value).strip(" ,.-")
        return value or None

    @staticmethod
    def _extract_before_event_query(text: str) -> str | None:
        lowered = (text or "").strip()
        if not lowered:
            return None

        quoted_match = re.search(r'до\s+["«](.+?)["»]', lowered, re.IGNORECASE)
        if quoted_match:
            return quoted_match.group(1).strip()

        match = re.search(r"до\s+(.+)", lowered, re.IGNORECASE)
        if not match:
            return None

        value = match.group(1).strip()
        value = re.sub(r"^(этой|этого|этот)\s+", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\b(сегодня|завтра|днем|днём|вечером|ночью|утром)\b", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value).strip(" ,.-")
        return value or None

    @staticmethod
    def _extract_last_assistant_anchor(memory: str) -> str | None:
        if not memory:
            return None
        assistant_lines = [line for line in memory.splitlines() if line.startswith("assistant:")]
        for line in reversed(assistant_lines):
            matches = re.findall(r"[«\"]([^»\"]+)[»\"]", line)
            if matches:
                return matches[0].strip()
        return None

    async def _resolve_relative_reference_from_calendar(
        self,
        *,
        text: str,
        base_start: datetime,
        base_end: datetime,
        access_token: str | None,
        refresh_token: str,
        timezone: str,
    ) -> tuple[datetime, datetime] | None:
        relation = None
        query = self._extract_after_event_query(text)
        if query:
            relation = "after"
        else:
            query = self._extract_before_event_query(text)
            if query:
                relation = "before"
        if not relation or not query:
            return None

        duration = base_end - base_start
        day_start = base_start.astimezone(self._tz()).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=day_start,
            time_max=day_end,
            timezone=timezone,
            limit=30,
        )
        matches = self._filter_matching_events(events, query)
        if not matches:
            return None

        anchor = matches[0]
        if relation == "after":
            anchor_point = self.calendar_service.parse_event_datetime(anchor, timezone, "end")
            if not anchor_point:
                return None
            return anchor_point, anchor_point + duration

        anchor_point = self.calendar_service.parse_event_datetime(anchor, timezone, "start")
        if not anchor_point:
            return None
        return anchor_point - duration, anchor_point

    def _filter_matching_events(self, events: list[dict], title_query: str) -> list[dict]:
        if not title_query:
            return events
        scored: list[tuple[float, dict]] = []
        for event in events:
            summary = event.get("summary") or ""
            score = self._event_match_score(title_query, summary)
            if score >= 0.45:
                scored.append((score, event))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [event for _, event in scored]

    def _apply_event_template(self, title: str, description: str) -> tuple[str, str]:
        normalized = self._normalize_event_text(title)
        if normalized in {"зал", "качалка", "тренировка"}:
            return "Зал", description
        if normalized in {"созвон", "митинг", "встреча"}:
            return "Созвон", description
        if normalized in {"универ", "университет"}:
            return "Универ", description
        return title, description

    def _ensure_tz(self, value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=self._tz())

    @staticmethod
    def _is_yes(text: str) -> bool:
        cleaned = text.strip().lower()
        if cleaned in {"да", "ага", "угу", "ок", "окей", "давай", "yes", "создавай", "ну да"}:
            return True
        return cleaned.startswith("да") or cleaned.startswith("ага") or cleaned.startswith("угу")

    @staticmethod
    def _is_no(text: str) -> bool:
        cleaned = text.strip().lower()
        if cleaned in {"нет", "неа", "отмена", "cancel", "no"}:
            return True
        return cleaned.startswith("не") and len(cleaned) <= 6

    @staticmethod
    def _is_reaction(text: str) -> bool:
        cleaned = text.strip().lower().replace("ё", "е")
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
            "вау",
            "ого",
            "жесть",
            "пон",
        )
        return cleaned in reactions or any(item == cleaned for item in reactions)

    @staticmethod
    def _is_undo_request(text: str) -> bool:
        cleaned = text.strip().lower()
        return cleaned in {
            "undo",
            "отмени последнее действие",
            "откати последнее действие",
            "верни последнее действие",
            "отмени последнее",
            "откати назад",
        }

    @staticmethod
    def _looks_like_create_event(text: str) -> bool:
        cleaned = text.strip().lower()
        time_words = (
            "сегодня",
            "завтра",
            "послезавтра",
            "в понедельник",
            "во вторник",
            "в среду",
            "в четверг",
            "в пятницу",
            "в субботу",
            "в воскресенье",
            "через ",
            "после ",
            "до ",
        )
        verb_words = (
            "созвон",
            "встреч",
            "зал",
            "трен",
            "стоматолог",
            "универ",
            "написать",
            "сходить",
            "позвонить",
            "погулять",
            "выставк",
            "врач",
            "маме",
            "создай",
            "добавь",
            "поставь",
            "запланируй",
        )
        has_time_marker = (
            bool(re.search(r"\b\d{1,2}[:.]\d{2}\b", cleaned))
            or bool(re.search(r"\b\d{1,2}\s*ч\b", cleaned))
            or bool(re.search(r"\b\d{1,2}\s+ма[йя]\b", cleaned))
            or any(word in cleaned for word in time_words)
        )
        has_event_hint = any(word in cleaned for word in verb_words)
        return has_time_marker and (has_event_hint or len(cleaned.split()) <= 8)

    def _looks_like_recurring_request(self, text: str) -> bool:
        cleaned = text.strip().lower()
        recurrence_markers = (
            "каждый ",
            "каждую ",
            "каждое ",
            "по понедельникам",
            "по вторникам",
            "по средам",
            "по четвергам",
            "по пятницам",
            "по субботам",
            "по воскресеньям",
            "каждый день",
            "каждый будний день",
            "ежедневно",
            "еженедельно",
            "повторя",
            "регулярно",
        )
        return any(marker in cleaned for marker in recurrence_markers) or self._contains_phrase(
            cleaned,
            (
                "каждый вторник",
                "каждую среду",
                "каждый четверг",
                "каждую пятницу",
                "каждый день",
                "каждый будний день",
                "ежедневно",
                "еженедельно",
            ),
            threshold=0.8,
        )

    def _fast_intent(self, text: str) -> str | None:
        cleaned = text.strip().lower()
        if not cleaned:
            return None
        if self._contains_phrase(
            cleaned,
            (
                "что у меня сегодня",
                "что сегодня",
                "че сегодня",
                "чо сегодня",
                "че седня",
                "чек седня",
                "планы на сегодня",
                "какие у меня планы",
                "что у меня в",
                "что у меня на",
                "что по планам сегодня",
                "что у меня на сегодня",
            ),
        ):
            return "today_schedule"
        if self._contains_phrase(
            cleaned,
            (
                "что дальше",
                "что потом",
                "а потом",
                "что сейчас",
                "что следующее",
                "что у меня дальше",
                "что у меня потом",
                "после этого",
                "до этого",
            ),
        ) or cleaned.startswith("а после ") or cleaned.startswith("а до ") or cleaned.startswith("после ") or cleaned.startswith("до "):
            return "next_event"
        if any(phrase in cleaned for phrase in ("перенеси", "сдвинь", "переименуй", "измени событие", "сделай на ")) and "последнее действие" not in cleaned:
            return "update_event"
        if any(phrase in cleaned for phrase in ("отмени", "удали", "убери", "сними")) and "последнее действие" not in cleaned:
            return "cancel_event"
        if "напом" in cleaned:
            return "set_reminder"
        if (
            any(phrase in cleaned for phrase in ("найди окно", "свободное время", "подбери время", "какое время есть", "куда влезет", "раскидай", "найди слот"))
            or cleaned.startswith("когда есть время")
            or ("какое время" in cleaned and "есть" in cleaned)
            or cleaned.startswith("найди ")
        ):
            return "plan_events"
        if self._looks_like_create_event(cleaned):
            return "create_event"
        if self._contains_phrase(cleaned, ("что умеешь", "хелп", "help", "команды", "что ты умеешь")):
            return "general_help"
        return None

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

    async def _record_undo_action(
        self,
        session: AsyncSession,
        telegram_id: int,
        kind: str,
        payload: dict[str, Any],
    ) -> None:
        session.add(
            UndoAction(
                telegram_id=telegram_id,
                kind=kind,
                payload_json=self._dumps_json(payload) or "{}",
            )
        )
        await session.commit()

    async def _get_last_undo_action(self, session: AsyncSession, telegram_id: int) -> UndoAction | None:
        result = await session.execute(
            select(UndoAction)
            .where(UndoAction.telegram_id == telegram_id, UndoAction.undone_at.is_(None))
            .order_by(UndoAction.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _schedule_event_reminder(
        self,
        *,
        session: AsyncSession,
        user_id: int,
        telegram_id: int,
        event_id: str,
        event_title: str,
        event_start_iso: str,
        minutes_before: int,
    ) -> None:
        remind_at = self._ensure_tz(datetime.fromisoformat(event_start_iso)) - timedelta(minutes=minutes_before)
        session.add(
            EventReminder(
                user_id=user_id,
                telegram_id=telegram_id,
                event_id=event_id,
                event_title=event_title,
                event_start_iso=event_start_iso,
                remind_at=remind_at,
                minutes_before=minutes_before,
            )
        )
        await session.commit()

    async def run_reminder_loop(self) -> None:
        while True:
            try:
                async with self.session_factory() as session:
                    now = datetime.now(self._tz())
                    result = await session.execute(
                        select(EventReminder).where(
                            EventReminder.sent_at.is_(None),
                            EventReminder.remind_at <= now,
                        )
                    )
                    reminders = result.scalars().all()
                    for reminder in reminders:
                        start_at = self._ensure_tz(datetime.fromisoformat(reminder.event_start_iso))
                        reply = (
                            f"Напоминалка: скоро «{escape(reminder.event_title)}».\n"
                            f"Старт в {self._format_dt(start_at)}."
                        )
                        await self.bot.send_message(reminder.telegram_id, reply)
                        reminder.sent_at = now
                    if reminders:
                        await session.commit()
                    await self._send_tomorrow_digests(session, now)
            except Exception:
                logger.exception("reminder_loop_failed")
            await asyncio.sleep(30)

    async def _send_tomorrow_digests(self, session: AsyncSession, now: datetime) -> None:
        if now.hour != 21:
            return

        tomorrow = (now + timedelta(days=1)).date()
        digest_date = tomorrow.isoformat()

        result = await session.execute(
            select(TomorrowDigestDelivery.user_id).where(TomorrowDigestDelivery.digest_for_date == digest_date)
        )
        already_sent = set(result.scalars().all())

        result = await session.execute(
            select(User, GoogleAccount)
            .join(GoogleAccount, GoogleAccount.user_id == User.id)
            .where(User.status.in_([UserStatus.APPROVED.value, UserStatus.ADMIN.value]), User.google_connected.is_(True))
        )
        recipients = result.all()

        for user, google_account in recipients:
            if user.id in already_sent:
                continue

            day_start = datetime.combine(tomorrow, datetime.min.time(), tzinfo=self._tz())
            day_end = day_start + timedelta(days=1)
            events = await self.calendar_service.list_events(
                access_token=google_account.access_token,
                refresh_token=google_account.refresh_token,
                time_min=day_start,
                time_max=day_end,
                timezone=self.settings.default_timezone,
                limit=20,
            )
            events = [
                event
                for event in events
                if (
                    (start := self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start"))
                    and day_start <= start < day_end
                )
            ]

            if not events:
                reply = f"Сводка на завтра, {tomorrow.strftime('%d.%m')}:\nПока пусто. Если хочешь, можем что-нибудь запланировать."
            else:
                lines = []
                for event in events[:10]:
                    start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
                    summary = escape(event.get("summary") or "Без названия")
                    lines.append(f"• {start.strftime('%H:%M') if start else 'весь день'} — {summary}")
                reply = f"Сводка на завтра, {tomorrow.strftime('%d.%m')}:\n" + "\n".join(lines)

            await self.bot.send_message(user.telegram_id, reply)
            session.add(
                TomorrowDigestDelivery(
                    user_id=user.id,
                    telegram_id=user.telegram_id,
                    digest_for_date=digest_date,
                )
            )

        await session.commit()

    def _window_hours_for_day_parts(self, day_parts: list[str], earliest_time: str | None, latest_time: str | None) -> tuple[int, int]:
        if earliest_time and latest_time:
            return int(earliest_time.split(":")[0]), int(latest_time.split(":")[0])
        if earliest_time and not latest_time:
            return int(earliest_time.split(":")[0]), 23
        parts = set(day_parts)
        if "morning" in parts:
            return 8, 12
        if "afternoon" in parts:
            return 12, 18
        if "evening" in parts:
            return 18, 23
        if "night" in parts:
            return 20, 23
        return 9, 23

    def _batch_result_lines(
        self,
        created: list[dict[str, Any]],
        skipped: list[str],
    ) -> list[str]:
        lines: list[str] = []
        if created:
            lines.append("Готово, уже создал вот это:")
            lines.extend(f"• <a href=\"{item['link']}\">{escape(item['title'])}</a>" for item in created)
        if skipped:
            if lines:
                lines.append("")
            lines.append("Это пока пропустил:")
            lines.extend(f"• {item}" for item in skipped)
        return lines

    def _planner_alternatives_text(self, alternatives_by_day: list[tuple[str, list[str]]]) -> str:
        if not alternatives_by_day:
            return ""
        lines = ["Ещё рядом нашёл такие варианты:"]
        for day_label, options in alternatives_by_day[:3]:
            if not options:
                continue
            lines.append(f"• {day_label}: " + ", ".join(options[:3]))
        return "\n".join(lines)

    async def _continue_batch_suggestion_flow(
        self,
        *,
        session: AsyncSession,
        telegram_id: int,
        pending: dict[str, Any],
        created_entry: dict[str, Any] | None = None,
        skipped_current: bool = False,
    ) -> dict[str, Any]:
        created = list(pending.get("batch_created", []))
        skipped = list(pending.get("batch_skipped", []))
        if created_entry:
            created.append(created_entry)
        if skipped_current:
            skipped.append(f"{pending['title']} — {self._format_dt(self._ensure_tz(datetime.fromisoformat(pending['requested_start_iso'])))}")

        queue = list(pending.get("batch_queue", []))
        if queue:
            next_pending = queue[0]
            next_pending["batch_queue"] = queue[1:]
            next_pending["batch_created"] = created
            next_pending["batch_skipped"] = skipped
            await self._set_pending_suggestions(telegram_id, next_pending, session)
            created_lines = self._batch_result_lines(created, skipped)
            prefix = "\n".join(created_lines)
            if prefix:
                prefix += "\n\n"
            conflict_dt = self._format_dt(self._ensure_tz(datetime.fromisoformat(next_pending["requested_start_iso"])))
            visible = self._current_suggestion_options(next_pending)
            tail = f" Еще рядом есть {visible[1]['label']}." if len(visible) > 1 else ""
            reply = (
                f"{prefix}Теперь по «{escape(next_pending['title'])}» есть конфликт на {conflict_dt}.\n"
                f"Могу поставить на {visible[0]['label']}.{tail}\n"
                "Выбирай вариант кнопкой ниже."
            )
            return {"done": False, "reply": reply, "pending": next_pending}

        await self._set_pending_suggestions(telegram_id, None, session)
        return {"done": True, "reply": "\n".join(self._batch_result_lines(created, skipped)), "created": created, "skipped": skipped}

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

    @staticmethod
    def _humanize_rrule(rule: str) -> str:
        if not rule:
            return "повтор по расписанию"

        normalized = rule.upper()
        day_names = {
            "MO": ("каждый понедельник", "понедельникам"),
            "TU": ("каждый вторник", "вторникам"),
            "WE": ("каждую среду", "средам"),
            "TH": ("каждый четверг", "четвергам"),
            "FR": ("каждую пятницу", "пятницам"),
            "SA": ("каждую субботу", "субботам"),
            "SU": ("каждое воскресенье", "воскресеньям"),
        }

        if "FREQ=DAILY" in normalized and "BYDAY=MO,TU,WE,TH,FR" in normalized:
            return "каждый будний день"
        if "FREQ=DAILY" in normalized:
            return "каждый день"

        byday_match = re.search(r"BYDAY=([A-Z,]+)", normalized)
        byday_values = byday_match.group(1).split(",") if byday_match else []
        named_days = [day_names.get(value, (value, value)) for value in byday_values]

        if "FREQ=WEEKLY" in normalized and named_days:
            if len(named_days) == 1:
                return named_days[0][0]
            return "каждую неделю по " + ", ".join(day[1] for day in named_days)

        if "FREQ=MONTHLY" in normalized:
            return "каждый месяц"
        if "FREQ=YEARLY" in normalized:
            return "каждый год"

        return "повтор по расписанию"

    @staticmethod
    def _quick_cancel_parse(text: str, timezone: str) -> dict[str, Any] | None:
        cleaned = (text or "").strip().lower()
        if not cleaned:
            return None
        if not any(word in cleaned for word in ("отмени", "удали", "убери", "сними")):
            return None

        now = datetime.now(ZoneInfo(timezone))
        search_until = now + timedelta(days=60)
        title_query = cleaned
        removable_phrases = (
            "отмени",
            "удали",
            "убери",
            "сними",
            "каждый",
            "каждую",
            "каждое",
            "ежедневно",
            "еженедельно",
            "повтор",
            "повторение",
            "по понедельникам",
            "по вторникам",
            "по средам",
            "по четвергам",
            "по пятницам",
            "по субботам",
            "по воскресеньям",
        )
        for phrase in removable_phrases:
            title_query = title_query.replace(phrase, " ")
        title_query = re.sub(r"\s+", " ", title_query).strip(" ,.-")
        if not title_query:
            return None

        return {
            "should_cancel": True,
            "title_query": title_query,
            "date_from_iso": now.isoformat(),
            "date_to_iso": search_until.isoformat(),
            "timezone": timezone,
            "needs_clarification": False,
            "clarification_question": "",
        }

    async def _reply_action_failed(self, message: Message, session: AsyncSession) -> None:
        reply = "Что-то сбойнуло на моей стороне. Я ничего не создал и не изменил. Давай попробуем ещё раз."
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)

    async def _reply_model_timeout(self, message: Message, session: AsyncSession) -> None:
        reply = "Сейчас сеть или модель тупят дольше нормы. Я ничего не менял. Попробуй ещё раз через минуту."
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)

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

    def _duration_minutes(self, start_at: datetime, end_at: datetime) -> int:
        return max(1, int((end_at - start_at).total_seconds() // 60))

    def _confirm_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    self._action_button("Да", ActionCallback(action="confirm_create").pack(), style="success"),
                    self._action_button("Изменить", ActionCallback(action="edit_draft").pack(), style="primary"),
                    self._action_button("Отмена", ActionCallback(action="cancel_create").pack(), style="danger"),
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
                    self._action_button(
                        text="Поставить поверх",
                        callback_data=ActionCallback(action="allow_overlap").pack(),
                        style="danger",
                    )
                ]
            )

        for local_index, option in enumerate(visible):
            rows.append(
                [
                    self._action_button(
                        text=option["label"],
                        callback_data=ActionCallback(action="pick_suggestion", option=offset + local_index).pack(),
                        style="primary",
                    )
                ]
            )

        if offset + self.SUGGESTION_PAGE_SIZE < total:
            rows.append(
                [self._action_button("Еще варианты", ActionCallback(action="more_suggestions").pack(), style="primary")]
            )

        rows.append([self._action_button("Отмена", ActionCallback(action="cancel_create").pack(), style="danger")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _selection_keyboard(self, options: list[dict[str, Any]]) -> InlineKeyboardMarkup:
        rows = [
            [self._action_button(option["label"], ActionCallback(action="pick_selection", option=index).pack(), style="primary")]
            for index, option in enumerate(options[:5])
        ]
        rows.append([self._action_button("Отмена", ActionCallback(action="cancel_create").pack(), style="danger")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _next_event_keyboard(self, *, can_remind_10: bool, can_remind_60: bool) -> InlineKeyboardMarkup | None:
        row: list[InlineKeyboardButton] = []
        if can_remind_10:
            row.append(self._action_button("Напомнить за 10 мин", ActionCallback(action="remind_next_10").pack(), style="primary"))
        if can_remind_60:
            row.append(self._action_button("Напомнить за час", ActionCallback(action="remind_next_60").pack(), style="primary"))
        if not row:
            return None
        return InlineKeyboardMarkup(inline_keyboard=[row])

    def _action_button(self, text: str, callback_data: str, *, style: str | None = None) -> InlineKeyboardButton:
        extra_data: dict[str, Any] = {}
        if style:
            extra_data["style"] = style
        return InlineKeyboardButton(text=text, callback_data=callback_data, **extra_data)

    def _event_option_label(self, title: str, start: datetime | None) -> str:
        title = title or "Без названия"
        if not start:
            return f"весь день — {title}"
        return f"{start.strftime('%d.%m %H:%M')} — {title}"

    @staticmethod
    def _event_recreate_payload(event: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key in ("summary", "description", "start", "end", "recurrence", "location"):
            if event.get(key) is not None:
                payload[key] = event[key]
        return payload

    async def _offer_recurring_cancel_scope(
        self,
        *,
        telegram_id: int,
        event: dict[str, Any],
        timezone: str,
        session: AsyncSession,
    ) -> bool:
        recurring_event_id = event.get("recurringEventId")
        if not recurring_event_id:
            return False

        start = self.calendar_service.parse_event_datetime(event, timezone, "start")
        title = event.get("summary") or "Без названия"
        label = self._event_option_label(title, start)
        options = [
            {
                "scope": "single",
                "event_id": event["id"],
                "title": title,
                "recurring_event_id": recurring_event_id,
                "instance_start_iso": start.isoformat() if start else "",
                "label": f"Только этот — {label}",
            },
            {
                "scope": "future",
                "event_id": event["id"],
                "title": title,
                "recurring_event_id": recurring_event_id,
                "instance_start_iso": start.isoformat() if start else "",
                "label": f"Этот и все следующие — {label}",
            },
            {
                "scope": "series",
                "event_id": recurring_event_id,
                "title": title,
                "recurring_event_id": recurring_event_id,
                "instance_start_iso": start.isoformat() if start else "",
                "label": f"Всю серию — {title}",
            },
        ]
        await self._set_selection_state(
            telegram_id,
            {
                "mode": "cancel_scope",
                "timezone": timezone,
                "options": options,
            },
            session,
        )
        return True

    def _user_help_text(self) -> str:
        return (
            "Вот что я умею:\n"
            "/start — показать стартовое сообщение\n"
            "/help — показать все команды и примеры\n\n"
            "/undo — откатить последнее действие\n\n"
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
            "/stats — короткая статистика по использованию\n"
            "/undo — откатить последнее действие"
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

    async def _get_user_preferences(self, user_id: int, session: AsyncSession) -> UserPreference:
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user_id))
        preferences = result.scalar_one_or_none()
        if preferences:
            return preferences

        preferences = UserPreference(user_id=user_id)
        session.add(preferences)
        await session.flush()
        return preferences

    async def _remember_reminder_preference(self, user_id: int, minutes_before: int, session: AsyncSession) -> None:
        preferences = await self._get_user_preferences(user_id, session)
        preferences.default_reminder_minutes = minutes_before
        await session.commit()

    async def _remember_overlap_preference(self, user_id: int, session: AsyncSession) -> None:
        preferences = await self._get_user_preferences(user_id, session)
        preferences.prefers_overlap = True
        await session.commit()

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
        text: str | None = None,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
    ) -> None:
        now = datetime.now(self._tz())
        target_day, target_label = self._extract_schedule_target_date(text or "", now)
        day_start = target_day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=day_start,
            time_max=day_end,
            timezone=self.settings.default_timezone,
            limit=20,
        )
        events = [
            event
            for event in events
            if (
                (start := self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start"))
                and day_start <= start < day_end
            )
        ]
        if not events:
            reply = f"На {target_label} календарь пуст. Если хочешь, можем что-нибудь запланировать."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        lines = []
        for event in events[:8]:
            start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
            summary = escape(event.get("summary") or "Без названия")
            lines.append(f"{start.strftime('%H:%M') if start else 'весь день'} — {summary}")
        free_windows = self._free_windows_text(max(now, day_start), day_end, events)
        prefix = f"На {target_label} у тебя {len(events)} {'событие' if len(events) == 1 else 'события' if 2 <= len(events) <= 4 else 'событий'}:\n"
        suffix = f"\n\nСвободные окна дальше {target_label}: {free_windows}" if free_windows and target_day.date() >= now.date() else ""
        reply = prefix + "\n".join(lines) + suffix
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)

    async def _answer_event_lookup(
        self,
        *,
        message: Message,
        text: str,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
    ) -> bool:
        query = self._extract_event_lookup_query(text)
        if not query:
            return False

        now = datetime.now(self._tz())
        target_day, _ = self._extract_schedule_target_date(text, now)
        time_min = target_day.replace(hour=0, minute=0, second=0, microsecond=0)
        time_max = time_min + timedelta(days=1)
        events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=time_min,
            time_max=time_max,
            timezone=self.settings.default_timezone,
            limit=30,
        )
        matches = self._filter_matching_events(events, query)
        if not matches:
            search_start = now - timedelta(days=1)
            search_end = now + timedelta(days=14)
            events = await self.calendar_service.list_events(
                access_token=access_token,
                refresh_token=refresh_token,
                time_min=search_start,
                time_max=search_end,
                timezone=self.settings.default_timezone,
                limit=50,
            )
            matches = self._filter_matching_events(events, query)
        if not matches:
            return False

        event = matches[0]
        start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
        end = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "end")
        summary = escape(event.get("summary") or "Событие")
        if start and end:
            reply = f"«{summary}» у тебя {start.strftime('%d.%m')} с {start.strftime('%H:%M')} до {end.strftime('%H:%M')}."
        elif start:
            reply = f"«{summary}» у тебя {start.strftime('%d.%m')} в {start.strftime('%H:%M')}."
        else:
            reply = f"Нашел событие «{summary}», но время у него не смог нормально прочитать."
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)
        return True

    async def _answer_next_event(
        self,
        *,
        message: Message,
        text: str | None = None,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
    ) -> None:
        now = datetime.now(self._tz())
        raw_text = text or ""
        memory = await self._memory_prompt(message.from_user.id, session)
        implicit_anchor = self._extract_last_assistant_anchor(memory)
        relation: str | None = None
        anchor_query = None

        explicit_after = self._extract_after_event_query(raw_text)
        explicit_before = self._extract_before_event_query(raw_text)
        lowered = raw_text.lower()
        if explicit_after:
            relation = "after"
            anchor_query = explicit_after
        elif explicit_before:
            relation = "before"
            anchor_query = explicit_before
        elif "после этого" in lowered and implicit_anchor:
            relation = "after"
            anchor_query = implicit_anchor
        elif "до этого" in lowered and implicit_anchor:
            relation = "before"
            anchor_query = implicit_anchor

        if anchor_query:
            search_start = now - timedelta(days=1)
            search_end = now + timedelta(days=14)
            events = await self.calendar_service.list_events(
                access_token=access_token,
                refresh_token=refresh_token,
                time_min=search_start,
                time_max=search_end,
                timezone=self.settings.default_timezone,
                limit=50,
            )
            matches = self._filter_matching_events(events, anchor_query)
            if not matches:
                reply = f"Не нашел в календаре событие «{escape(anchor_query)}». Напиши название чуть точнее."
                await message.answer(reply)
                await self._remember(message.from_user.id, "assistant", reply, session)
                return
            anchor_event = matches[0]
            anchor_end = self.calendar_service.parse_event_datetime(anchor_event, self.settings.default_timezone, "end")
            if not anchor_end:
                reply = f"У события «{escape(anchor_event.get('summary') or anchor_query)}» не получилось прочитать время окончания."
                await message.answer(reply)
                await self._remember(message.from_user.id, "assistant", reply, session)
                return

            if relation == "before":
                anchor_start = self.calendar_service.parse_event_datetime(anchor_event, self.settings.default_timezone, "start")
                if not anchor_start:
                    reply = f"У события «{escape(anchor_event.get('summary') or anchor_query)}» не получилось прочитать время начала."
                    await message.answer(reply)
                    await self._remember(message.from_user.id, "assistant", reply, session)
                    return
                previous_events = await self.calendar_service.list_events(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    time_min=max(now - timedelta(days=7), anchor_start - timedelta(days=7)),
                    time_max=anchor_start,
                    timezone=self.settings.default_timezone,
                    limit=20,
                )
                filtered_previous = []
                for candidate in previous_events:
                    candidate_start = self.calendar_service.parse_event_datetime(candidate, self.settings.default_timezone, "start")
                    if candidate_start and candidate_start < anchor_start:
                        filtered_previous.append(candidate)
                event = filtered_previous[-1] if filtered_previous else None
            else:
                next_events = await self.calendar_service.list_events(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    time_min=anchor_end,
                    time_max=anchor_end + timedelta(days=7),
                    timezone=self.settings.default_timezone,
                    limit=2,
                )
                event = next_events[0] if next_events else None
        else:
            event = await self.calendar_service.get_next_event(
                access_token=access_token,
                refresh_token=refresh_token,
                timezone=self.settings.default_timezone,
            )
        if event:
            event_end = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "end")
            if event_end and event_end <= now:
                event = None
        if not event:
            reply = (
                ("До этого события ничего более раннего не вижу." if relation == "before" else f"После этого события дальше пока ничего не вижу.")
                if anchor_query
                else "Пока ничего ближайшего не вижу. Похоже, день свободный."
            )
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
        if after_events:
            after_next = after_events[0]
            after_next_end = self.calendar_service.parse_event_datetime(after_next, self.settings.default_timezone, "end")
            current_event_id = event.get("id")
            if current_event_id and after_next.get("id") == current_event_id and len(after_events) > 1:
                after_next = after_events[1]
            elif current_event_id and after_next.get("id") == current_event_id:
                after_next = None
            elif after_next_end and after_next_end <= (end or now):
                after_next = after_events[1] if len(after_events) > 1 else None
            if after_next:
                after_next_start = self.calendar_service.parse_event_datetime(after_next, self.settings.default_timezone, "start")
                after_next_summary = escape(after_next.get("summary") or "что-то еще")
                if after_next_start:
                    follow_up = f" Потом еще «{after_next_summary}» в {self._format_time(after_next_start)}."

        if start and end and start <= now <= end:
            reply = f"Сейчас у тебя идет «{summary}» до {end.strftime('%H:%M')}.{follow_up}"
            markup = None
        elif start:
            prefix = "До этого у тебя" if relation == "before" else ("После этого у тебя" if anchor_query else "Дальше у тебя")
            reply = f"{prefix} «{summary}» в {self._format_dt(start)}.{follow_up}"
            minutes_until = int((start - now).total_seconds() // 60)
            markup = self._next_event_keyboard(
                can_remind_10=relation != "before" and minutes_until > 10,
                can_remind_60=relation != "before" and minutes_until > 60,
            )
        else:
            prefix = "До этого у тебя" if relation == "before" else ("После этого у тебя" if anchor_query else "Дальше у тебя")
            reply = f"{prefix} событие «{summary}»."
            markup = None
        await message.answer(reply, reply_markup=markup)
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
            created_event = await self.calendar_service.create_event_details(
                access_token=access_token,
                refresh_token=refresh_token,
                title=pending["title"],
                description=pending["description"],
                start_iso=option["start_iso"],
                end_iso=option["end_iso"],
                timezone=pending.get("timezone") or self.settings.default_timezone,
            )
            created_entry = {"title": pending["title"], "link": created_event.get("htmlLink", ""), "event_id": created_event.get("id")}
            if pending.get("batch_queue") is not None:
                state = await self._continue_batch_suggestion_flow(
                    session=session,
                    telegram_id=message.from_user.id,
                    pending=pending,
                    created_entry=created_entry,
                )
                if state["done"]:
                    reply = state["reply"] or "Готово."
                    await message.answer(reply)
                    if state.get("created"):
                        await self._record_undo_action(
                            session,
                            message.from_user.id,
                            "create_batch",
                            {"event_ids": [item["event_id"] for item in state["created"] if item.get("event_id")]},
                        )
                else:
                    reply = state["reply"]
                    await message.answer(reply, reply_markup=self._suggestion_keyboard(state["pending"]))
                await self._remember(message.from_user.id, "assistant", reply, session)
                return True

            await self._set_pending_suggestions(message.from_user.id, None, session)
            await self._record_undo_action(
                session,
                message.from_user.id,
                "create_event",
                {"event_id": created_event.get("id"), "event_title": pending["title"]},
            )
            reply = f"Супер, поставил на {option['label']}.\n<a href=\"{created_event.get('htmlLink', '')}\">Открыть событие в Google Calendar</a>"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", f"Поставил событие на {option['label']}.", session)
            return True

        if self._is_no(text):
            if pending.get("batch_queue") is not None:
                state = await self._continue_batch_suggestion_flow(
                    session=session,
                    telegram_id=message.from_user.id,
                    pending=pending,
                    skipped_current=True,
                )
                if state["done"]:
                    reply = state["reply"] or "Окей, пропустил конфликтные пункты."
                    await message.answer(reply)
                    if state.get("created"):
                        await self._record_undo_action(
                            session,
                            message.from_user.id,
                            "create_batch",
                            {"event_ids": [item["event_id"] for item in state["created"] if item.get("event_id")]},
                        )
                else:
                    reply = state["reply"]
                    await message.answer(reply, reply_markup=self._suggestion_keyboard(state["pending"]))
                await self._remember(message.from_user.id, "assistant", reply, session)
                return True

            await self._set_pending_suggestions(message.from_user.id, None, session)
            reply = "Окей, не создаю. Напиши другое время, и я попробую еще раз."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True

        return False

    async def _handle_pending_confirmation_decision(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
        session: AsyncSession,
    ) -> bool:
        pending = await self._get_pending_confirmation(message.from_user.id, session)
        if not pending:
            return False

        text = (message.text or "").strip()
        if not text:
            return False

        if self._is_no(text):
            await self._set_pending_confirmation(message.from_user.id, None, session)
            reply = "Окей, не создаю и не меняю. Если хочешь, можем переформулировать по-другому."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True

        if not self._is_yes(text):
            return False

        mode = pending.get("mode", "create")
        if mode == "create_batch":
            created: list[dict[str, Any]] = []
            skipped: list[str] = []
            unresolved: list[dict[str, Any]] = []
            for item in pending.get("items", []):
                item_timezone = item.get("timezone") or self.settings.default_timezone
                start_at = self._ensure_tz(datetime.fromisoformat(item["start_iso"]))
                end_at = self._ensure_tz(datetime.fromisoformat(item["end_iso"]))
                conflicts = await self.calendar_service.find_conflicts(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    start_at=start_at,
                    end_at=end_at,
                    timezone=item_timezone,
                )
                if conflicts:
                    suggestions = await self.calendar_service.suggest_free_slots(
                        access_token=access_token,
                        refresh_token=refresh_token,
                        desired_start=start_at,
                        desired_end=end_at,
                        timezone=item_timezone,
                        count=12,
                    )
                    if suggestions:
                        unresolved.append(
                            {
                                "mode": "create",
                                "title": item["title"],
                                "description": item["description"],
                                "requested_start_iso": item["start_iso"],
                                "requested_end_iso": item["end_iso"],
                                "timezone": item_timezone,
                                "options": [
                                    {
                                        "label": self._format_dt(slot_start),
                                        "start_iso": slot_start.isoformat(),
                                        "end_iso": slot_end.isoformat(),
                                    }
                                    for slot_start, slot_end in suggestions
                                ],
                                "offset": 0,
                            }
                        )
                    else:
                        skipped.append(f"{item['title']} — {self._format_dt(start_at)}")
                    continue

                created_event = await self.calendar_service.create_event_details(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    title=item["title"],
                    description=item["description"],
                    start_iso=item["start_iso"],
                    end_iso=item["end_iso"],
                    timezone=item_timezone,
                )
                created.append({"title": item["title"], "link": created_event.get("htmlLink", ""), "event_id": created_event.get("id")})

            await self._set_pending_confirmation(message.from_user.id, None, session)
            if unresolved:
                first = unresolved[0]
                first["batch_queue"] = unresolved[1:]
                first["batch_created"] = created
                first["batch_skipped"] = skipped
                await self._set_pending_suggestions(message.from_user.id, first, session)
                lines = self._batch_result_lines(created, skipped)
                prefix = "\n".join(lines)
                if prefix:
                    prefix += "\n\n"
                visible = self._current_suggestion_options(first)
                reply = (
                    f"{prefix}Теперь по «{escape(first['title'])}» есть конфликт на {self._format_dt(self._ensure_tz(datetime.fromisoformat(first['requested_start_iso'])))}.\n"
                    f"Могу поставить на {visible[0]['label']}.\n"
                    "Выбирай вариант кнопкой ниже."
                )
                await message.answer(reply, reply_markup=self._suggestion_keyboard(first))
                await self._remember(message.from_user.id, "assistant", reply, session)
                return True

            reply = "\n".join(self._batch_result_lines(created, skipped)) if (created or skipped) else "Похоже, тут пока нечего создавать."
            await message.answer(reply)
            await self._remember(
                message.from_user.id,
                "assistant",
                f"Обработал пакет из {len(pending.get('items', []))} событий: создал {len(created)}, пропустил {len(skipped)}.",
                session,
            )
            await self._log_usage(
                session,
                message.from_user.id,
                "event_batch_processed",
                {"created": len(created), "skipped": len(skipped)},
            )
            if created:
                await self._record_undo_action(
                    session,
                    message.from_user.id,
                    "create_batch",
                    {"event_ids": [item["event_id"] for item in created if item.get("event_id")]},
                )
            return True

        if mode == "update":
            previous = await self.calendar_service.get_event(
                access_token=access_token,
                refresh_token=refresh_token,
                event_id=pending["event_id"],
            )
            link = await self.calendar_service.update_event(
                access_token=access_token,
                refresh_token=refresh_token,
                event_id=pending["event_id"],
                title=pending.get("title"),
                description=pending.get("description"),
                start_iso=pending.get("start_iso"),
                end_iso=pending.get("end_iso"),
                timezone=pending["timezone"],
            )
            await self._record_undo_action(
                session,
                message.from_user.id,
                "update_event",
                {"event_id": pending["event_id"], "previous_event": previous},
            )
            await self._set_pending_confirmation(message.from_user.id, None, session)
            reply = f"Готово, обновил событие.\n<a href=\"{link}\">Открыть в Google Calendar</a>"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", f"Обновил событие {pending['title']}.", session)
            await self._log_usage(
                session,
                message.from_user.id,
                "event_updated",
                {"title": pending["title"], "start_iso": pending.get("start_iso")},
            )
            return True

        if mode == "update_recurring_series":
            previous = await self.calendar_service.get_event(
                access_token=access_token,
                refresh_token=refresh_token,
                event_id=pending["event_id"],
            )
            updated = await self.calendar_service.update_recurring_series(
                access_token=access_token,
                refresh_token=refresh_token,
                recurring_event_id=pending["event_id"],
                title=pending.get("title"),
                description=pending.get("description"),
                start_iso=pending.get("start_iso"),
                end_iso=pending.get("end_iso"),
                timezone=pending["timezone"],
            )
            await self._record_undo_action(
                session,
                message.from_user.id,
                "update_event",
                {"event_id": pending["event_id"], "previous_event": previous},
            )
            await self._set_pending_confirmation(message.from_user.id, None, session)
            reply = f"Готово, обновил всю серию.\n<a href=\"{updated.get('htmlLink', '')}\">Открыть в Google Calendar</a>"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", f"Обновил всю серию {pending['title']}.", session)
            await self._log_usage(session, message.from_user.id, "event_updated_recurring_series", {"title": pending["title"]})
            return True

        if mode == "update_future_recurring":
            created = await self.calendar_service.split_and_update_recurring_series(
                access_token=access_token,
                refresh_token=refresh_token,
                recurring_event_id=pending["recurring_event_id"],
                split_from=self._ensure_tz(datetime.fromisoformat(pending["instance_start_iso"])),
                title=pending["title"],
                description=pending["description"],
                start_iso=pending["start_iso"],
                end_iso=pending["end_iso"],
                timezone=pending["timezone"],
            )
            await self._set_pending_confirmation(message.from_user.id, None, session)
            reply = f"Готово, обновил этот и все следующие повторы.\n<a href=\"{created.get('htmlLink', '')}\">Открыть в Google Calendar</a>"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", f"Обновил будущие повторы {pending['title']}.", session)
            await self._log_usage(session, message.from_user.id, "event_updated_recurring_future", {"title": pending["title"]})
            return True

        if mode == "create_recurring":
            created = await self.calendar_service.create_recurring_event_details(
                access_token=access_token,
                refresh_token=refresh_token,
                title=pending["title"],
                description=pending["description"],
                start_iso=pending["start_iso"],
                end_iso=pending["end_iso"],
                timezone=pending["timezone"],
                recurrence_rule=pending["recurrence_rule"],
            )
            link = created.get("htmlLink", "")
            await self._record_undo_action(
                session,
                message.from_user.id,
                "create_event",
                {"event_id": created.get("id"), "event_title": pending["title"]},
            )
            await self._set_pending_confirmation(message.from_user.id, None, session)
            reply = f"Готово, создал повторяющееся событие.\n<a href=\"{link}\">Открыть в Google Calendar</a>"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", f"Создал повторяющееся событие {pending['title']}.", session)
            await self._log_usage(
                session,
                message.from_user.id,
                "event_created_recurring",
                {"title": pending["title"], "start_iso": pending.get("start_iso")},
            )
            return True

        created = await self.calendar_service.create_event_details(
            access_token=access_token,
            refresh_token=refresh_token,
            title=pending["title"],
            description=pending["description"],
            start_iso=pending["start_iso"],
            end_iso=pending["end_iso"],
            timezone=pending["timezone"],
        )
        link = created.get("htmlLink", "")
        await self._record_undo_action(
            session,
            message.from_user.id,
            "create_event",
            {"event_id": created.get("id"), "event_title": pending["title"]},
        )
        await self._set_pending_confirmation(message.from_user.id, None, session)
        reply = f"Готово, событие создал.\n<a href=\"{link}\">Открыть в Google Calendar</a>"
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", f"Создал событие {pending['title']}.", session)
        await self._log_usage(
            session,
            message.from_user.id,
            "event_created",
            {"title": pending["title"], "start_iso": pending.get("start_iso")},
        )
        return True

    async def _handle_pending_confirmation_update(
        self,
        *,
        message: Message,
        access_token: str | None,
        refresh_token: str,
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

        after_event_query = self._extract_after_event_query(text)
        before_event_query = self._extract_before_event_query(text)
        if after_event_query or before_event_query:
            timezone = pending.get("timezone") or self.settings.default_timezone
            pending_start = self._ensure_tz(datetime.fromisoformat(pending["start_iso"]))
            pending_end = self._ensure_tz(datetime.fromisoformat(pending["end_iso"]))
            resolved = await self._resolve_relative_reference_from_calendar(
                text=text,
                base_start=pending_start,
                base_end=pending_end,
                access_token=access_token,
                refresh_token=refresh_token,
                timezone=timezone,
            )
            if resolved:
                start_at, end_at = resolved
                updated_pending = {
                    "mode": pending.get("mode", "create"),
                    "event_id": pending.get("event_id"),
                    "title": pending["title"],
                    "description": pending["description"],
                    "start_iso": start_at.isoformat(),
                    "end_iso": end_at.isoformat(),
                    "timezone": timezone,
                }
                if pending.get("recurrence_rule"):
                    updated_pending["recurrence_rule"] = pending["recurrence_rule"]
                await self._set_pending_confirmation(message.from_user.id, updated_pending, session)
                reply = (
                    "Обновил черновик:\n"
                    f"• {escape(pending['title'])}\n"
                    f"• {self._format_dt(start_at)} — {self._format_dt(end_at)}\n\n"
                    + (
                        f"Повтор: {escape(self._humanize_rrule(pending['recurrence_rule']))}\n\n"
                        if pending.get("mode") == "create_recurring" and pending.get("recurrence_rule")
                        else ""
                    )
                    + "Если все так, жми кнопку ниже."
                )
                await message.answer(reply, reply_markup=self._confirm_keyboard())
                await self._remember(
                    message.from_user.id,
                    "assistant",
                    f"Привязал черновик к событию из календаря: {after_event_query or before_event_query}.",
                    session,
                )
                return True

            relation_label = "после" if after_event_query else "до"
            reply = f"Не нашел в календаре событие, {relation_label} которого ты хочешь это поставить. Напиши точнее его название."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True

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
        recurrence_rule = pending.get("recurrence_rule")

        if pending.get("mode") == "create_recurring" and self._looks_like_recurring_request(text):
            merged_text = (
                f"{title} {text} в {self._format_time(start_at)} "
                f"на {self._duration_minutes(start_at, end_at)} минут"
            )
            recurring = await self.parser.parse_recurring_request(merged_text)
            if recurring.get("should_create") and recurring.get("recurrence_rule"):
                recurrence_rule = recurring["recurrence_rule"]

        updated_pending = {
            "mode": pending.get("mode", "create"),
            "event_id": pending.get("event_id"),
            "title": title,
            "description": description,
            "start_iso": start_at.isoformat(),
            "end_iso": end_at.isoformat(),
            "timezone": timezone,
        }
        if recurrence_rule:
            updated_pending["recurrence_rule"] = recurrence_rule

        await self._set_pending_confirmation(message.from_user.id, updated_pending, session)
        reply = (
            "Обновил черновик:\n"
            f"• {escape(title)}\n"
            f"• {self._format_dt(start_at)} — {self._format_dt(end_at)}\n\n"
            + (
                f"Повтор: {escape(self._humanize_rrule(recurrence_rule))}\n\n"
                if pending.get("mode") == "create_recurring" and recurrence_rule
                else ""
            )
            + "Если все так, жми кнопку ниже."
        )
        await message.answer(reply, reply_markup=self._confirm_keyboard())
        await self._remember(message.from_user.id, "assistant", f"Обновил черновик {title} на {self._format_dt(start_at)}.", session)
        return True

    async def _handle_pending_state_clarification(self, message: Message, session: AsyncSession) -> bool:
        user_id = message.from_user.id
        text = (message.text or "").strip()

        selection = await self._get_selection_state(user_id, session)
        if selection:
            mode = selection.get("mode")
            options = selection.get("options", [])
            if mode == "cancel":
                lowered = text.lower()
                if any(word in lowered for word in ("повтор", "повторение", "серия", "всю серию")):
                    reply = "Можно. Для повторяющегося события я умею удалить один день, этот и все следующие или всю серию. Выбери вариант кнопкой ниже."
                    await message.answer(reply)
                    await self._remember(user_id, "assistant", reply, session)
                    return True

                if text:
                    recurring_matches = [
                        option
                        for option in options
                        if option.get("recurring_event_id")
                        and self._event_match_score(text, option.get("title") or "") >= 0.45
                    ]
                    if recurring_matches:
                        chosen = recurring_matches[0]
                        await self._offer_recurring_cancel_scope(
                            telegram_id=user_id,
                            event={
                                "id": chosen["event_id"],
                                "summary": chosen["title"],
                                "recurringEventId": chosen.get("recurring_event_id"),
                                "start": {"dateTime": chosen.get("instance_start_iso")},
                            },
                            timezone=selection.get("timezone") or self.settings.default_timezone,
                            session=session,
                        )
                        new_selection = await self._get_selection_state(user_id, session)
                        reply = (
                            f"«{escape(chosen['title'])}» — это повторяющееся событие.\n"
                            "Выбери, что удалить: только один день, этот и все следующие или всю серию."
                        )
                        await message.answer(reply, reply_markup=self._selection_keyboard(new_selection["options"]))
                        await self._remember(user_id, "assistant", reply, session)
                        return True

            reply = "Я сейчас жду, что ты выберешь один из вариантов кнопкой ниже. Если не подходит, жми «Отмена»."
            await message.answer(reply)
            await self._remember(user_id, "assistant", reply, session)
            return True

        pending_suggestions = await self._get_pending_suggestions(user_id, session)
        if pending_suggestions:
            if pending_suggestions.get("requested_start_iso"):
                reply = "Я сейчас жду выбор по конфликту: можно нажать на свободный слот, «Поставить поверх» или «Отмена»."
            else:
                reply = "Я сейчас жду, что ты выберешь один из предложенных слотов кнопкой ниже или нажмёшь «Отмена»."
            await message.answer(reply)
            await self._remember(user_id, "assistant", reply, session)
            return True

        pending_confirmation = await self._get_pending_confirmation(user_id, session)
        if pending_confirmation:
            if pending_confirmation.get("mode") == "create_batch":
                reply = "Я сейчас жду подтверждение по пачке событий. Можешь ответить «да», «нет» или переформулировать всё сообщение целиком."
            else:
                reply = "Я сейчас жду подтверждение по черновику. Можешь ответить «да», «нет» или написать правку вроде «не в 15, а в 16»."
            await message.answer(reply)
            await self._remember(user_id, "assistant", reply, session)
            return True

        return False

    async def _contextual_reply(self, message: Message, text: str, session: AsyncSession) -> bool:
        if not self._is_reaction(text):
            return False

        cleaned = self._normalize_event_text(text)
        if cleaned in {"и все", "и все?"}:
            reply = "Да, по календарю на этом всё. Если хочешь, могу ещё подсказать свободные окна."
        elif cleaned in {"офигет", "капец", "жест", "серьезн", "серьезно"}:
            reply = "Понимаю. Если хочешь, сразу посмотрю, что у тебя дальше, или найду свободное время."
        else:
            reply = "Окей. Если хочешь, могу сразу что-то добавить, перенести или показать свободное время."
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
        fast_intent = self._fast_intent(text)
        if fast_intent:
            intent = fast_intent
            logger.info("fast_intent_detected user=%s intent=%s text=%r", message.from_user.id, intent, text[:120])
            await self._log_usage(session, message.from_user.id, f"fast_intent:{intent}")
        else:
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
                text=text,
                access_token=access_token,
                refresh_token=refresh_token,
                session=session,
            )
            return

        if self._looks_like_event_lookup(text):
            if await self._answer_event_lookup(
                message=message,
                text=text,
                access_token=access_token,
                refresh_token=refresh_token,
                session=session,
            ):
                return

        if intent == "next_event":
            await self._answer_next_event(
                message=message,
                text=text,
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
            routing_input = text
            memory = await self._memory_prompt(message.from_user.id, session)
            if memory:
                routing_input = f"Recent conversation:\n{memory}\n\nCurrent message:\n{text}"
            routing = await self.parser.classify_intent(routing_input)
            reply = routing.get("clarification_question") or "Не до конца понял. Ты хочешь создать событие или посмотреть расписание?"
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        if intent == "plan_events":
            await self._process_planning_request(
                message=message,
                text=text,
                refresh_token=refresh_token,
                access_token=access_token,
                session=session,
            )
            return

        if intent == "set_reminder":
            result = await session.execute(select(User).where(User.telegram_id == message.from_user.id))
            user = result.scalar_one_or_none()
            if not user:
                reply = "Не нашел твой профиль, попробуй еще раз."
                await message.answer(reply)
                await self._remember(message.from_user.id, "assistant", reply, session)
                return
            await self._process_reminder_request(
                message=message,
                text=text,
                refresh_token=refresh_token,
                access_token=access_token,
                user=user,
                session=session,
            )
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
        reply = (
            "Не до конца понял запрос. Я лучше работаю, когда ты пишешь прямо по делу.\n"
            "Например:\n"
            "• созвон завтра в 15:00 на час\n"
            "• что у меня сегодня\n"
            "• что дальше\n"
            "• перенеси созвон с Колей на 16:00\n"
            "• отмени тренировку в пятницу"
        )
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)

    async def _process_planning_request(
        self,
        *,
        message: Message,
        text: str,
        refresh_token: str,
        access_token: str | None,
        session: AsyncSession,
    ) -> None:
        parsing_input = text
        memory = await self._memory_prompt(message.from_user.id, session)
        if memory:
            parsing_input = f"Recent conversation:\n{memory}\n\nCurrent message:\n{text}"

        parsed = await self.parser.parse_planning_request(parsing_input)
        if parsed.get("needs_clarification"):
            reply = parsed.get("clarification_question") or "Уточни, пожалуйста, в какие дни и с какого времени тебе искать слот."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return
        if not parsed.get("should_plan"):
            reply = "Не до конца понял, какое окно тебе нужно. Напиши, например: «найди сегодня вечером 2 часа для прогулки»."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        dates = parsed.get("dates") or []
        if not dates:
            reply = "Мне нужны хотя бы конкретные дни, чтобы подобрать свободные окна."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        timezone = parsed.get("timezone") or self.settings.default_timezone
        title, description = self._apply_event_template(
            parsed.get("title") or "Новое событие",
            parsed.get("description") or text,
        )
        duration_minutes = max(15, int(parsed.get("duration_minutes") or 60))
        earliest_time = parsed.get("earliest_time")
        latest_time = parsed.get("latest_time")
        day_parts = parsed.get("day_parts") or []
        start_hour, end_hour = self._window_hours_for_day_parts(day_parts, earliest_time, latest_time)
        count = int(parsed.get("count") or len(dates) or 1)

        items: list[dict[str, Any]] = []
        unavailable: list[str] = []
        alternatives_by_day: list[tuple[str, list[str]]] = []
        for date_iso in dates[:count]:
            day = datetime.fromisoformat(date_iso).replace(tzinfo=self._tz())
            if earliest_time:
                hour, minute = map(int, earliest_time.split(":"))
            else:
                hour, minute = start_hour, 0
            if latest_time:
                end_hour_exact, end_minute = map(int, latest_time.split(":"))
            else:
                end_hour_exact, end_minute = end_hour, 0
            window_start = day.replace(hour=hour, minute=minute, second=0, microsecond=0)
            window_end = day.replace(hour=end_hour_exact, minute=end_minute, second=0, microsecond=0)
            if window_end <= window_start:
                window_end = window_start + timedelta(hours=4)

            slots = await self.calendar_service.find_free_slots_in_window(
                access_token=access_token,
                refresh_token=refresh_token,
                window_start=window_start,
                window_end=window_end,
                duration_minutes=duration_minutes,
                timezone=timezone,
                count=3,
            )
            if not slots:
                unavailable.append(day.strftime("%d.%m"))
                continue
            slot_start, slot_end = slots[0]
            items.append(
                {
                    "title": title,
                    "description": description,
                    "start_iso": slot_start.isoformat(),
                    "end_iso": slot_end.isoformat(),
                    "timezone": timezone,
                }
            )
            if len(slots) > 1:
                alternatives_by_day.append(
                    (
                        day.strftime("%d.%m"),
                        [self._format_dt(candidate_start) for candidate_start, _ in slots[1:]],
                    )
                )

        if not items:
            reply = "Я не нашел нормальных свободных окон под это в указанные дни. Давай сузим время или выберем другие дни."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        await self._set_pending_confirmation(
            message.from_user.id,
            {"mode": "create_batch", "items": items},
            session,
        )
        reply = "Нашел такие слоты:\n" + self._describe_pending({"items": items})
        if unavailable:
            reply += "\n\nНе нашел окно в: " + ", ".join(unavailable)
        planner_alternatives = self._planner_alternatives_text(alternatives_by_day)
        if planner_alternatives:
            reply += "\n\n" + planner_alternatives
        reply += "\n\nЕсли все ок, ответь «да» или жми кнопку."
        await message.answer(reply, reply_markup=self._confirm_keyboard())
        await self._remember(message.from_user.id, "assistant", reply, session)

    async def _process_recurring_request(
        self,
        *,
        message: Message,
        text: str,
        session: AsyncSession,
    ) -> bool:
        parsed = await self.parser.parse_recurring_request(text)
        if parsed.get("needs_clarification"):
            reply = parsed.get("clarification_question") or "Уточни, пожалуйста, в какие дни и во сколько повторять событие."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True
        if not parsed.get("should_create"):
            return False

        title, description = self._apply_event_template(
            parsed.get("title") or "Новое событие",
            parsed.get("description") or text,
        )
        pending = {
            "mode": "create_recurring",
            "title": title,
            "description": description,
            "start_iso": parsed["start_iso"],
            "end_iso": parsed["end_iso"],
            "timezone": parsed.get("timezone") or self.settings.default_timezone,
            "recurrence_rule": parsed["recurrence_rule"],
        }
        await self._set_pending_confirmation(message.from_user.id, pending, session)
        reply = (
            "Понял так, создаем повторяющееся событие:\n"
            f"{self._describe_pending(pending)}\n"
            f"Повтор: {escape(self._humanize_rrule(parsed['recurrence_rule']))}\n\n"
            "Если всё так, ответь «да» или жми кнопку."
        )
        await message.answer(reply, reply_markup=self._confirm_keyboard())
        await self._remember(message.from_user.id, "assistant", reply, session)
        return True

    async def _process_reminder_request(
        self,
        *,
        message: Message,
        text: str,
        refresh_token: str,
        access_token: str | None,
        user: User,
        session: AsyncSession,
    ) -> None:
        parsed = await self.parser.parse_reminder_request(text)
        if parsed.get("needs_clarification"):
            reply = parsed.get("clarification_question") or "Уточни, пожалуйста, к какому именно событию поставить напоминание."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return
        if not parsed.get("should_set"):
            reply = "Не до конца понял, к какому событию поставить напоминание. Напиши, например: «напомни за 10 минут до универа»."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        timezone = parsed.get("timezone") or self.settings.default_timezone
        time_min = self._ensure_tz(datetime.fromisoformat(parsed["search_from_iso"]))
        time_max = self._ensure_tz(datetime.fromisoformat(parsed["search_to_iso"]))
        minutes_before = max(1, int(parsed.get("minutes_before") or 10))
        title_query = (parsed.get("title_query") or "").strip()

        events = await self.calendar_service.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=time_min,
            time_max=time_max,
            timezone=timezone,
            limit=30,
        )
        matches = self._filter_matching_events(events, title_query)
        if not matches:
            reply = "Не нашел подходящее событие для напоминания. Назови его чуть точнее или добавь день."
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
                        "event_start_iso": start.isoformat() if start else "",
                        "label": self._event_option_label(event.get("summary") or "Без названия", start),
                    }
                )
            await self._set_selection_state(
                message.from_user.id,
                {
                    "mode": "reminder",
                    "minutes_before": minutes_before,
                    "options": options,
                },
                session,
            )
            reply = "Нашел несколько похожих событий. Выбери, на какое именно поставить напоминание."
            await message.answer(reply, reply_markup=self._selection_keyboard(options))
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        target = matches[0]
        start = self.calendar_service.parse_event_datetime(target, timezone, "start")
        if not start:
            reply = "У этого события странное время, пока не могу надежно поставить напоминание."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        await self._schedule_event_reminder(
            session=session,
            user_id=user.id,
            telegram_id=message.from_user.id,
            event_id=target["id"],
            event_title=target.get("summary") or "Без названия",
            event_start_iso=start.isoformat(),
            minutes_before=minutes_before,
        )
        await self._remember_reminder_preference(user.id, minutes_before, session)
        reply = f"Готово, напомню про «{escape(target.get('summary') or 'событие')}» за {minutes_before} мин."
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

    async def _perform_undo(self, message: Message, session: AsyncSession, user: User) -> bool:
        google_account = await self._get_google_account(user.id, session)
        action = await self._get_last_undo_action(session, message.from_user.id)
        if not action:
            reply = "Пока нечего откатывать."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True

        payload = self._loads_json(action.payload_json, {}) or {}
        try:
            if action.kind == "create_event":
                event_id = payload.get("event_id")
                if not event_id:
                    raise ValueError("missing event_id")
                await self.calendar_service.delete_event(
                    access_token=google_account.access_token,
                    refresh_token=google_account.refresh_token,
                    event_id=event_id,
                )
                reply = "Откатил последнее создание события."
            elif action.kind == "create_batch":
                for event_id in payload.get("event_ids", []):
                    await self.calendar_service.delete_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=event_id,
                    )
                reply = "Откатил последнее пакетное создание."
            elif action.kind == "update_event":
                previous_event = payload.get("previous_event") or {}
                event_id = payload.get("event_id")
                if not event_id or not previous_event:
                    raise ValueError("missing previous_event")
                await self.calendar_service.update_event(
                    access_token=google_account.access_token,
                    refresh_token=google_account.refresh_token,
                    event_id=event_id,
                    title=previous_event.get("summary"),
                    description=previous_event.get("description"),
                    start_iso=(previous_event.get("start") or {}).get("dateTime"),
                    end_iso=(previous_event.get("end") or {}).get("dateTime"),
                    timezone=(previous_event.get("start") or {}).get("timeZone") or self.settings.default_timezone,
                )
                reply = "Откатил последнее изменение события."
            elif action.kind == "delete_event":
                event_payload = payload.get("event_payload") or {}
                created = await self.calendar_service.create_event_from_payload(
                    access_token=google_account.access_token,
                    refresh_token=google_account.refresh_token,
                    payload=event_payload,
                )
                reply = f"Вернул удалённое событие.\n<a href=\"{created.get('htmlLink', '')}\">Открыть в Google Calendar</a>"
            else:
                reply = "Это действие я пока не умею безопасно откатывать."
                await message.answer(reply)
                await self._remember(message.from_user.id, "assistant", reply, session)
                return True
        except Exception:
            logger.exception("undo_failed user=%s action=%s", message.from_user.id, action.kind)
            reply = "Не смог откатить последнее действие. Лучше проверить календарь руками."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return True

        action.undone_at = datetime.now(self._tz())
        await session.commit()
        await message.answer(reply)
        await self._remember(message.from_user.id, "assistant", reply, session)
        return True

    async def cmd_undo(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._ensure_access(message, session)
            if not user:
                return
            await self._perform_undo(message, session, user)

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

            if self._is_undo_request(message.text or ""):
                await self._perform_undo(message, session, user)
                return

            if await self._handle_pending_suggestion(
                message=message,
                access_token=google_account.access_token,
                refresh_token=google_account.refresh_token,
                session=session,
            ):
                return

            if await self._handle_pending_confirmation_decision(
                message=message,
                access_token=google_account.access_token,
                refresh_token=google_account.refresh_token,
                session=session,
            ):
                return

            if await self._handle_pending_confirmation_update(
                message=message,
                access_token=google_account.access_token,
                refresh_token=google_account.refresh_token,
                session=session,
            ):
                return

            if await self._handle_pending_state_clarification(message, session):
                return

            text = message.text or ""
            await self._remember(message.from_user.id, "user", text, session)
            try:
                await self._route_intent(
                    message=message,
                    text=text,
                    access_token=google_account.access_token,
                    refresh_token=google_account.refresh_token,
                    session=session,
                )
            except httpx.TimeoutException:
                logger.exception("text_route_timeout user=%s", message.from_user.id)
                await self._reply_model_timeout(message, session)
            except Exception:
                logger.exception("text_route_failed user=%s text=%r", message.from_user.id, text[:200])
                await self._reply_action_failed(message, session)

    async def handle_voice(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._ensure_access(message, session)
            if not user:
                return
            google_account = await self._get_google_account(user.id, session)

            await message.answer("Сек, расшифровываю голосовое...")
            try:
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
            except httpx.TimeoutException:
                logger.exception("voice_route_timeout user=%s", message.from_user.id)
                await self._reply_model_timeout(message, session)
            except Exception:
                logger.exception("voice_route_failed user=%s", message.from_user.id)
                await self._reply_action_failed(message, session)

    async def handle_action_callback(self, callback: CallbackQuery, callback_data: ActionCallback) -> None:
        user_id = callback.from_user.id
        async with self.session_factory() as session:
            user = await self._load_user_by_telegram_id(user_id, session)
            if not user:
                await callback.answer("Пользователь не найден", show_alert=True)
                return
            google_account = await self._get_google_account(user.id, session)

            if callback_data.action == "cancel_create":
                selection = await self._get_selection_state(user_id, session)
                pending_confirmation = await self._get_pending_confirmation(user_id, session)
                pending_suggestions = await self._get_pending_suggestions(user_id, session)
                await self._clear_pending_state(user_id, session)
                await self._set_selection_state(user_id, None, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                if selection:
                    reply = "Окей, отменил этот выбор."
                elif pending_suggestions:
                    reply = "Окей, этот вариант отменил."
                elif pending_confirmation and pending_confirmation.get("mode", "").startswith("update"):
                    reply = "Окей, ничего не меняю."
                elif pending_confirmation and pending_confirmation.get("mode") == "cancel":
                    reply = "Окей, ничего не удаляю."
                else:
                    reply = "Окей, ничего не создаю."
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", reply, session)
                await callback.answer()
                return

            if callback_data.action == "edit_draft":
                pending = await self._get_pending_confirmation(user_id, session)
                if not pending:
                    await callback.answer("Черновик уже устарел.", show_alert=True)
                    return
                mode = pending.get("mode", "create")
                if mode == "create_batch":
                    reply = "Ок, поправь весь запрос целиком текстом. Например: «не четверг и пятница, а только пятница вечером»."
                elif mode == "update":
                    reply = "Ок, напиши, что поменять. Например: «не в 15, а в 16» или «не на час, а на 30 минут»."
                elif mode == "create_recurring":
                    reply = "Ок, напиши правку текстом. Например: «не по вторникам, а по средам» или «не в 19:00, а в 20:00»."
                else:
                    reply = "Ок, напиши правку текстом. Например: «не в 15, а в 16» или «не на час, а на 30 минут»."
                await callback.answer()
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", reply, session)
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
                    if option.get("recurring_event_id"):
                        await self._offer_recurring_cancel_scope(
                            telegram_id=user_id,
                            event={
                                "id": option["event_id"],
                                "summary": option["title"],
                                "recurringEventId": option.get("recurring_event_id"),
                                "start": {"dateTime": option.get("instance_start_iso")},
                            },
                            timezone=selection.get("timezone") or self.settings.default_timezone,
                            session=session,
                        )
                        reply = (
                            f"«{escape(option['title'])}» похоже на повторяющееся событие.\n"
                            "Выбери, что именно удалить: только этот день, этот и все следующие, или всю серию."
                        )
                        next_selection = await self._get_selection_state(user_id, session)
                        await callback.message.answer(reply, reply_markup=self._selection_keyboard(next_selection["options"]))
                        await self._remember(user_id, "assistant", reply, session)
                        await callback.answer()
                        return

                    deleted_event = await self.calendar_service.get_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=option["event_id"],
                    )
                    await self.calendar_service.delete_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=option["event_id"],
                    )
                    await self._record_undo_action(
                        session,
                        user_id,
                        "delete_event",
                        {"event_payload": self._event_recreate_payload(deleted_event)},
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

                if mode == "cancel_scope":
                    scope = option.get("scope")
                    title = option["title"]
                    if scope == "single":
                        deleted_event = await self.calendar_service.get_event(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            event_id=option["event_id"],
                        )
                        await self.calendar_service.delete_event(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            event_id=option["event_id"],
                        )
                        await self._record_undo_action(
                            session,
                            user_id,
                            "delete_event",
                            {"event_payload": self._event_recreate_payload(deleted_event)},
                        )
                        reply = f"Готово, убрал только этот повтор «{escape(title)}»."
                        usage_kind = "event_deleted_single_recurring"
                    elif scope == "future":
                        instance_start = self._ensure_tz(datetime.fromisoformat(option["instance_start_iso"]))
                        await self.calendar_service.truncate_recurring_series(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            recurring_event_id=option["recurring_event_id"],
                            keep_until_before=instance_start - timedelta(seconds=1),
                        )
                        reply = f"Готово, убрал «{escape(title)}» начиная с этого дня и все следующие повторы."
                        usage_kind = "event_deleted_future_recurring"
                    else:
                        deleted_series = await self.calendar_service.get_event(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            event_id=option["recurring_event_id"],
                        )
                        await self.calendar_service.delete_event(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            event_id=option["recurring_event_id"],
                        )
                        await self._record_undo_action(
                            session,
                            user_id,
                            "delete_event",
                            {"event_payload": self._event_recreate_payload(deleted_series)},
                        )
                        reply = f"Готово, удалил всю серию «{escape(title)}»."
                        usage_kind = "event_deleted_series_recurring"

                    await callback.message.answer(reply)
                    await self._remember(user_id, "assistant", reply, session)
                    await self._log_usage(
                        session,
                        user_id,
                        usage_kind,
                        {"title": title, "scope": scope},
                    )
                    await callback.answer()
                    return

                if mode == "update_scope":
                    scope = option.get("scope")
                    if scope == "single":
                        pending = {
                            "mode": "update",
                            "event_id": option["event_id"],
                            "title": selection["new_title"] or option["title"],
                            "description": selection["new_description"] or "",
                            "start_iso": selection["new_start_iso"],
                            "end_iso": selection["new_end_iso"],
                            "timezone": selection["timezone"],
                        }
                        reply = (
                            f"Понял, обновляем только этот повтор «{escape(option['title'])}»:\n"
                            f"{self._describe_pending(pending)}\n\n"
                            "Если все так, жми кнопку ниже."
                        )
                    elif scope == "future":
                        pending = {
                            "mode": "update_future_recurring",
                            "event_id": option["event_id"],
                            "recurring_event_id": option["recurring_event_id"],
                            "instance_start_iso": option["instance_start_iso"],
                            "title": selection["new_title"] or option["title"],
                            "description": selection["new_description"] or "",
                            "start_iso": selection["new_start_iso"],
                            "end_iso": selection["new_end_iso"],
                            "timezone": selection["timezone"],
                        }
                        reply = (
                            f"Понял, обновляем этот и все следующие повторы «{escape(option['title'])}»:\n"
                            f"{self._describe_pending(pending)}\n\n"
                            "Если все так, жми кнопку ниже."
                        )
                    else:
                        pending = {
                            "mode": "update_recurring_series",
                            "event_id": option["recurring_event_id"],
                            "title": selection["new_title"] or option["title"],
                            "description": selection["new_description"] or "",
                            "start_iso": selection["new_start_iso"],
                            "end_iso": selection["new_end_iso"],
                            "timezone": selection["timezone"],
                        }
                        reply = (
                            f"Понял, обновляем всю серию «{escape(option['title'])}»:\n"
                            f"{self._describe_pending(pending)}\n\n"
                            "Если все так, жми кнопку ниже."
                        )

                    await self._set_pending_confirmation(user_id, pending, session)
                    await callback.message.answer(reply, reply_markup=self._confirm_keyboard())
                    await self._remember(user_id, "assistant", reply, session)
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

                if mode == "reminder":
                    await self._schedule_event_reminder(
                        session=session,
                        user_id=user.id,
                        telegram_id=user_id,
                        event_id=option["event_id"],
                        event_title=option["title"],
                        event_start_iso=option["event_start_iso"],
                        minutes_before=int(selection["minutes_before"]),
                    )
                    await self._remember_reminder_preference(user.id, int(selection["minutes_before"]), session)
                    reply = f"Готово, напомню про «{escape(option['title'])}» за {selection['minutes_before']} мин."
                    await callback.message.answer(reply)
                    await self._remember(user_id, "assistant", reply, session)
                    await callback.answer()
                    return

            if callback_data.action in {"remind_next_10", "remind_next_60"}:
                minutes_before = 10 if callback_data.action.endswith("_10") else 60
                event = await self.calendar_service.get_next_event(
                    access_token=google_account.access_token,
                    refresh_token=google_account.refresh_token,
                    timezone=self.settings.default_timezone,
                )
                if not event:
                    await callback.answer("Ближайшее событие не нашёл.", show_alert=True)
                    return
                start = self.calendar_service.parse_event_datetime(event, self.settings.default_timezone, "start")
                if not start:
                    await callback.answer("У этого события не получилось прочитать время.", show_alert=True)
                    return
                if start <= datetime.now(self._tz()):
                    await callback.answer("Это событие уже началось, напоминание не поставлю.", show_alert=True)
                    return

                await self._schedule_event_reminder(
                    session=session,
                    user_id=user.id,
                    telegram_id=user_id,
                    event_id=event["id"],
                    event_title=event.get("summary") or "Без названия",
                    event_start_iso=start.isoformat(),
                    minutes_before=minutes_before,
                )
                await self._remember_reminder_preference(user.id, minutes_before, session)
                reply = f"Готово, напомню про «{escape(event.get('summary') or 'событие')}» за {minutes_before} мин."
                await callback.answer("Напоминание поставил")
                await callback.message.answer(reply)
                await self._remember(user_id, "assistant", reply, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                return

            if callback_data.action == "confirm_create":
                pending = await self._get_pending_confirmation(user_id, session)
                if not pending:
                    await callback.answer("У меня уже нет этого черновика.", show_alert=True)
                    return
                mode = pending.get("mode", "create")
                if mode == "create_batch":
                    created: list[dict[str, Any]] = []
                    skipped: list[str] = []
                    unresolved: list[dict[str, Any]] = []
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
                            suggestions = await self.calendar_service.suggest_free_slots(
                                access_token=google_account.access_token,
                                refresh_token=google_account.refresh_token,
                                desired_start=start_at,
                                desired_end=end_at,
                                timezone=item_timezone,
                                count=12,
                            )
                            if suggestions:
                                unresolved.append(
                                    {
                                        "mode": "create",
                                        "title": item["title"],
                                        "description": item["description"],
                                        "requested_start_iso": item["start_iso"],
                                        "requested_end_iso": item["end_iso"],
                                        "timezone": item_timezone,
                                        "options": [
                                            {
                                                "label": self._format_dt(slot_start),
                                                "start_iso": slot_start.isoformat(),
                                                "end_iso": slot_end.isoformat(),
                                            }
                                            for slot_start, slot_end in suggestions
                                        ],
                                        "offset": 0,
                                    }
                                )
                            else:
                                skipped.append(f"{item['title']} — {self._format_dt(start_at)}")
                            continue

                        created_event = await self.calendar_service.create_event_details(
                            access_token=google_account.access_token,
                            refresh_token=google_account.refresh_token,
                            title=item["title"],
                            description=item["description"],
                            start_iso=item["start_iso"],
                            end_iso=item["end_iso"],
                            timezone=item_timezone,
                        )
                        created.append({"title": item["title"], "link": created_event.get("htmlLink", ""), "event_id": created_event.get("id")})

                    await self._set_pending_confirmation(user_id, None, session)
                    await callback.message.edit_reply_markup(reply_markup=None)
                    if unresolved:
                        first = unresolved[0]
                        first["batch_queue"] = unresolved[1:]
                        first["batch_created"] = created
                        first["batch_skipped"] = skipped
                        await self._set_pending_suggestions(user_id, first, session)
                        lines = self._batch_result_lines(created, skipped)
                        prefix = "\n".join(lines)
                        if prefix:
                            prefix += "\n\n"
                        visible = self._current_suggestion_options(first)
                        reply = (
                            f"{prefix}Теперь по «{escape(first['title'])}» есть конфликт на {self._format_dt(self._ensure_tz(datetime.fromisoformat(first['requested_start_iso'])))}.\n"
                            f"Могу поставить на {visible[0]['label']}.\n"
                            "Выбирай вариант кнопкой ниже."
                        )
                        await callback.message.answer(reply, reply_markup=self._suggestion_keyboard(first))
                        await self._remember(user_id, "assistant", reply, session)
                        await callback.answer()
                        return

                    reply = "\n".join(self._batch_result_lines(created, skipped)) if (created or skipped) else "Похоже, тут пока нечего создавать."
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
                    if created:
                        await self._record_undo_action(
                            session,
                            user_id,
                            "create_batch",
                            {"event_ids": [item["event_id"] for item in created if item.get("event_id")]},
                        )
                    await callback.answer()
                    return
                if mode == "create_recurring":
                    created = await self.calendar_service.create_recurring_event_details(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=pending["start_iso"],
                        end_iso=pending["end_iso"],
                        timezone=pending["timezone"],
                        recurrence_rule=pending["recurrence_rule"],
                    )
                    link = created.get("htmlLink", "")
                    await self._record_undo_action(
                        session,
                        user_id,
                        "create_event",
                        {"event_id": created.get("id"), "event_title": pending["title"]},
                    )
                    await self._set_pending_confirmation(user_id, None, session)
                    await callback.message.edit_reply_markup(reply_markup=None)
                    reply = f"Готово, создал повторяющееся событие.\n<a href=\"{link}\">Открыть в Google Calendar</a>"
                    await callback.message.answer(reply)
                    await self._remember(user_id, "assistant", f"Создал повторяющееся событие {pending['title']}.", session)
                    await self._log_usage(
                        session,
                        user_id,
                        "event_created_recurring",
                        {"title": pending["title"], "start_iso": pending.get("start_iso")},
                    )
                    await callback.answer()
                    return
                if mode == "update":
                    previous = await self.calendar_service.get_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=pending["event_id"],
                    )
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
                    await self._record_undo_action(
                        session,
                        user_id,
                        "update_event",
                        {"event_id": pending["event_id"], "previous_event": previous},
                    )
                elif mode == "update_recurring_series":
                    previous = await self.calendar_service.get_event(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        event_id=pending["event_id"],
                    )
                    updated = await self.calendar_service.update_recurring_series(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        recurring_event_id=pending["event_id"],
                        title=pending.get("title"),
                        description=pending.get("description"),
                        start_iso=pending.get("start_iso"),
                        end_iso=pending.get("end_iso"),
                        timezone=pending["timezone"],
                    )
                    link = updated.get("htmlLink", "")
                    await self._record_undo_action(
                        session,
                        user_id,
                        "update_event",
                        {"event_id": pending["event_id"], "previous_event": previous},
                    )
                elif mode == "update_future_recurring":
                    created = await self.calendar_service.split_and_update_recurring_series(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        recurring_event_id=pending["recurring_event_id"],
                        split_from=self._ensure_tz(datetime.fromisoformat(pending["instance_start_iso"])),
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=pending["start_iso"],
                        end_iso=pending["end_iso"],
                        timezone=pending["timezone"],
                    )
                    link = created.get("htmlLink", "")
                else:
                    created = await self.calendar_service.create_event_details(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=pending["start_iso"],
                        end_iso=pending["end_iso"],
                        timezone=pending["timezone"],
                    )
                    link = created.get("htmlLink", "")
                    await self._record_undo_action(
                        session,
                        user_id,
                        "create_event",
                        {"event_id": created.get("id"), "event_title": pending["title"]},
                    )
                await self._set_pending_confirmation(user_id, None, session)
                await callback.message.edit_reply_markup(reply_markup=None)
                reply = (
                    f"Готово, событие {'обновил' if mode in {'update', 'update_recurring_series', 'update_future_recurring'} else 'создал'}.\n"
                    f"<a href=\"{link}\">Открыть в Google Calendar</a>"
                )
                await callback.message.answer(reply)
                await self._remember(
                    user_id,
                    "assistant",
                    f"{'Обновил' if mode in {'update', 'update_recurring_series', 'update_future_recurring'} else 'Создал'} событие {pending['title']}.",
                    session,
                )
                logger.info(
                    "%s user=%s title=%r start=%s",
                    "event_updated" if mode in {'update', 'update_recurring_series', 'update_future_recurring'} else "event_created",
                    user_id,
                    pending["title"],
                    pending.get("start_iso"),
                )
                await self._log_usage(
                    session,
                    user_id,
                    "event_updated" if mode in {'update', 'update_recurring_series', 'update_future_recurring'} else "event_created",
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
                    created = await self.calendar_service.create_event_details(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=requested_start_iso,
                        end_iso=requested_end_iso,
                        timezone=timezone,
                    )
                    link = created.get("htmlLink", "")
                    reply = f"Ок, поставил поверх на {self._format_dt(self._ensure_tz(datetime.fromisoformat(requested_start_iso)))}.\n<a href=\"{link}\">Открыть событие в Google Calendar</a>"
                    usage_kind = "event_created_overlap"

                await self._remember_overlap_preference(user.id, session)

                if mode != "update" and pending.get("batch_queue") is not None:
                    await callback.message.edit_reply_markup(reply_markup=None)
                    state = await self._continue_batch_suggestion_flow(
                        session=session,
                        telegram_id=user_id,
                        pending=pending,
                        created_entry={"title": pending["title"], "link": link, "event_id": created.get("id")},
                    )
                    if state["done"]:
                        reply = state["reply"] or "Готово."
                        await callback.message.answer(reply)
                        if state.get("created"):
                            await self._record_undo_action(
                                session,
                                user_id,
                                "create_batch",
                                {"event_ids": [item["event_id"] for item in state["created"] if item.get("event_id")]},
                            )
                    else:
                        reply = state["reply"]
                        await callback.message.answer(reply, reply_markup=self._suggestion_keyboard(state["pending"]))
                    await self._remember(user_id, "assistant", reply, session)
                    await callback.answer()
                    return

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
                if mode != "update":
                    await self._record_undo_action(
                        session,
                        user_id,
                        "create_event",
                        {"event_id": created.get("id"), "event_title": pending["title"]},
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
                        timezone=pending.get("timezone") or self.settings.default_timezone,
                    )
                else:
                    created = await self.calendar_service.create_event_details(
                        access_token=google_account.access_token,
                        refresh_token=google_account.refresh_token,
                        title=pending["title"],
                        description=pending["description"],
                        start_iso=option["start_iso"],
                        end_iso=option["end_iso"],
                        timezone=self.settings.default_timezone,
                    )
                    link = created.get("htmlLink", "")
                if mode != "update" and pending.get("batch_queue") is not None:
                    await callback.message.edit_reply_markup(reply_markup=None)
                    state = await self._continue_batch_suggestion_flow(
                        session=session,
                        telegram_id=user_id,
                        pending=pending,
                        created_entry={"title": pending["title"], "link": link, "event_id": created.get("id")},
                    )
                    if state["done"]:
                        reply = state["reply"] or "Готово."
                        await callback.message.answer(reply)
                        if state.get("created"):
                            await self._record_undo_action(
                                session,
                                user_id,
                                "create_batch",
                                {"event_ids": [item["event_id"] for item in state["created"] if item.get("event_id")]},
                            )
                    else:
                        reply = state["reply"]
                        await callback.message.answer(reply, reply_markup=self._suggestion_keyboard(state["pending"]))
                    await self._remember(user_id, "assistant", reply, session)
                    await callback.answer()
                    return

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
                if mode != "update":
                    await self._record_undo_action(
                        session,
                        user_id,
                        "create_event",
                        {"event_id": created.get("id"), "event_title": pending["title"]},
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
        if self._looks_like_recurring_request(text) and await self._process_recurring_request(message=message, text=parsing_input, session=session):
            return
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
                title, description = self._apply_event_template(
                    raw_item.get("title") or "Новое событие",
                    raw_item.get("description") or text,
                )
                start_at = self._ensure_tz(datetime.fromisoformat(start_iso))
                end_at = self._ensure_tz(datetime.fromisoformat(end_iso))
                items.append(
                    {
                        "title": title,
                        "description": description,
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
            if self._looks_like_create_event(text):
                reply = "Не смог уверенно собрать событие. Напиши одной фразой что и когда, например: «созвон сегодня в 19:00 на час»."
                await message.answer(reply)
                await self._remember(message.from_user.id, "assistant", reply, session)
                return
            await self._friendly_fallback(message, text, session)
            return

        timezone = parsed.get("timezone") or self.settings.default_timezone
        start_at = self._ensure_tz(datetime.fromisoformat(parsed["start_iso"]))
        end_at = self._ensure_tz(datetime.fromisoformat(parsed["end_iso"]))
        resolved_relative = await self._resolve_relative_reference_from_calendar(
            text=text,
            base_start=start_at,
            base_end=end_at,
            access_token=access_token,
            refresh_token=refresh_token,
            timezone=timezone,
        )
        if resolved_relative:
            start_at, end_at = resolved_relative
        elif self._extract_after_event_query(text) or self._extract_before_event_query(text):
            relation_label = "после" if self._extract_after_event_query(text) else "до"
            reply = f"Я не нашел в календаре событие, {relation_label} которого ты хочешь это поставить. Напиши его название точнее."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

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
            user = await self._load_user_by_telegram_id(message.from_user.id, session)
            preferences = await self._get_user_preferences(user.id, session) if user else None
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
                overlap_hint = (
                    " Если хочешь как обычно поставить поверх — это можно сделать первой кнопкой.\n"
                    if preferences and preferences.prefers_overlap
                    else "Если хочешь, могу и поставить прямо поверх.\n"
                )
                reply = (
                    f"Смотри, в {time_text} у тебя уже стоит «{conflict_title}».\n"
                    f"Зато могу поставить на {visible[0]['label']}.{tail}\n"
                    f"{overlap_hint}"
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

        title, description = self._apply_event_template(
            parsed.get("title") or "Новое событие",
            parsed.get("description") or text,
        )
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
            reply = "Не до конца понял, что именно менять. Напиши что-то вроде: «перенеси созвон с Колей на 16:00»."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
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
        matches = self._filter_matching_events(events, title_query)

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
                        "label": self._event_option_label(event.get("summary") or "Без названия", start),
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

        if target.get("recurringEventId"):
            options = [
                {
                    "scope": "single",
                    "event_id": target["id"],
                    "title": current_title,
                    "recurring_event_id": target.get("recurringEventId"),
                    "instance_start_iso": current_start.isoformat(),
                    "label": f"Только этот — {self._event_option_label(current_title, current_start)}",
                },
                {
                    "scope": "future",
                    "event_id": target["id"],
                    "title": current_title,
                    "recurring_event_id": target.get("recurringEventId"),
                    "instance_start_iso": current_start.isoformat(),
                    "label": f"Этот и все следующие — {self._event_option_label(current_title, current_start)}",
                },
                {
                    "scope": "series",
                    "event_id": target.get("recurringEventId"),
                    "title": current_title,
                    "recurring_event_id": target.get("recurringEventId"),
                    "instance_start_iso": current_start.isoformat(),
                    "label": f"Всю серию — {current_title}",
                },
            ]
            await self._set_selection_state(
                message.from_user.id,
                {
                    "mode": "update_scope",
                    "timezone": timezone,
                    "new_title": new_title,
                    "new_description": new_description,
                    "new_start_iso": new_start.isoformat(),
                    "new_end_iso": new_end.isoformat(),
                    "options": options,
                },
                session,
            )
            reply = (
                f"«{escape(current_title)}» — это повторяющееся событие.\n"
                "Выбери, что именно менять: только этот повтор, этот и все следующие, или всю серию."
            )
            await message.answer(reply, reply_markup=self._selection_keyboard(options))
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

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
            user = await self._load_user_by_telegram_id(message.from_user.id, session)
            preferences = await self._get_user_preferences(user.id, session) if user else None
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
                overlap_hint = (
                    " Если хочешь оставить пересечение как обычно, можно поставить поверх той же кнопкой.\n"
                    if preferences and preferences.prefers_overlap
                    else ""
                )
                reply = (
                    f"Хотел перенести «{escape(current_title)}», но в {time_text} уже стоит «{conflict_title}».\n"
                    f"Могу вместо этого перенести на {visible[0]['label']}.{tail}\n"
                    f"{overlap_hint}"
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
        timezone = self.settings.default_timezone
        parsed = None
        recurring_cancel_hint = self._looks_like_recurring_request(text)
        if recurring_cancel_hint:
            parsed = self._quick_cancel_parse(text, timezone)
        if parsed is None:
            parsed = await self.parser.parse_cancel_request(text)
        if parsed.get("needs_clarification"):
            reply = parsed.get("clarification_question") or "Уточни, какое именно событие убрать."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return
        if not parsed.get("should_cancel"):
            reply = "Не до конца понял, что удалить. Напиши, например: «отмени тренировку в пятницу»."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
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

        matches = self._filter_matching_events(events, title_query)

        if not matches:
            reply = "Не нашел подходящее событие в календаре. Попробуй точнее назвать его или дату."
            await message.answer(reply)
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        if len(matches) > 1:
            if recurring_cancel_hint:
                recurring_groups: dict[str, dict[str, Any]] = {}
                for event in matches:
                    recurring_id = event.get("recurringEventId")
                    if not recurring_id:
                        continue
                    recurring_groups.setdefault(recurring_id, event)

                if len(recurring_groups) == 1:
                    target = next(iter(recurring_groups.values()))
                    await self._offer_recurring_cancel_scope(
                        telegram_id=message.from_user.id,
                        event=target,
                        timezone=timezone,
                        session=session,
                    )
                    selection = await self._get_selection_state(message.from_user.id, session)
                    reply = (
                        f"«{escape(target.get('summary') or 'Событие')}» — это повторяющееся событие.\n"
                        "Выбери, что удалить: только один день, этот и все следующие, или всю серию."
                    )
                    await message.answer(reply, reply_markup=self._selection_keyboard(selection["options"]))
                    await self._remember(message.from_user.id, "assistant", reply, session)
                    return

                if recurring_groups:
                    options = []
                    for event in list(recurring_groups.values())[:5]:
                        start = self.calendar_service.parse_event_datetime(event, timezone, "start")
                        options.append(
                            {
                                "event_id": event["id"],
                                "title": event.get("summary") or "Без названия",
                                "recurring_event_id": event.get("recurringEventId"),
                                "instance_start_iso": start.isoformat() if start else "",
                                "label": f"Серия: {self._event_option_label(event.get('summary') or 'Без названия', start)}",
                            }
                        )
                    await self._set_selection_state(
                        message.from_user.id,
                        {
                            "mode": "cancel",
                            "timezone": timezone,
                            "options": options,
                        },
                        session,
                    )
                    reply = "Нашел несколько повторяющихся серий. Выбери, какую серию хочешь удалить."
                    await message.answer(reply, reply_markup=self._selection_keyboard(options))
                    await self._remember(message.from_user.id, "assistant", reply, session)
                    return

            options = []
            for event in matches[:5]:
                start = self.calendar_service.parse_event_datetime(event, timezone, "start")
                options.append(
                    {
                        "event_id": event["id"],
                        "title": event.get("summary") or "Без названия",
                        "recurring_event_id": event.get("recurringEventId"),
                        "instance_start_iso": start.isoformat() if start else "",
                        "label": self._event_option_label(event.get("summary") or "Без названия", start),
                    }
                )
            await self._set_selection_state(
                message.from_user.id,
                {
                    "mode": "cancel",
                    "timezone": timezone,
                    "options": options,
                },
                session,
            )
            reply = "Нашел несколько похожих событий. Выбери, что именно удалить."
            await message.answer(reply, reply_markup=self._selection_keyboard(options))
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        target = matches[0]
        if target.get("recurringEventId"):
            await self._offer_recurring_cancel_scope(
                telegram_id=message.from_user.id,
                event=target,
                timezone=timezone,
                session=session,
            )
            selection = await self._get_selection_state(message.from_user.id, session)
            reply = (
                f"«{escape(target.get('summary') or 'Событие')}» — это повторяющееся событие.\n"
                "Выбери, что удалить: только этот день, этот и все следующие, или всю серию."
            )
            await message.answer(reply, reply_markup=self._selection_keyboard(selection["options"]))
            await self._remember(message.from_user.id, "assistant", reply, session)
            return

        await self._record_undo_action(
            session,
            message.from_user.id,
            "delete_event",
            {"event_payload": self._event_recreate_payload(target)},
        )
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
