from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.config import get_settings
from app.services.deepinfra import DeepInfraClient


class TaskParser:
    def __init__(self, client: DeepInfraClient) -> None:
        self.client = client
        self.settings = get_settings()
        self.timezone = ZoneInfo(self.settings.default_timezone)

    async def classify_intent(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        messages = [
            {
                "role": "system",
                "content": (
                    "You route Telegram calendar assistant messages. "
                    "Return only JSON with keys: intent, confidence, reply_style, clarification_question. "
                    "Allowed intents: create_event, update_event, cancel_event, plan_events, set_reminder, today_schedule, next_event, general_help, clarify, other. "
                    "reply_style must be one of: casual, neutral. "
                    "Use create_event when the user wants to add or plan a brand new calendar event. "
                    "Use update_event when the user wants to move, reschedule, rename, shorten, lengthen, or otherwise change an existing event. "
                    "Use cancel_event when the user wants to cancel, delete, remove, or drop an event from the calendar. "
                    "Use plan_events when the user asks you to find free time, fit events into free slots, or arrange multiple items across days. "
                    "Use set_reminder when the user asks to remind them about a specific event before it starts. "
                    "Use today_schedule when the user asks what is planned today. "
                    "Use next_event when the user asks what is next, what is happening now, or what comes after. "
                    "Use general_help when the user asks what the bot can do. "
                    "Use clarify when the intent is probably calendar-related but ambiguous. "
                    "Use other for unrelated conversation. "
                    "clarification_question must be empty unless intent=clarify. "
                    "If intent=clarify, clarification_question must be friendly, short, and in Russian, addressing the user as 'ты'. "
                    f"Current datetime is {now.isoformat()} in timezone {self.settings.default_timezone}."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def parse_planning_request(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract free-slot planning requests from Russian or English messages. "
                    "Return only JSON with keys: should_plan(boolean), title(string), description(string), "
                    "duration_minutes(integer), dates(array of ISO dates YYYY-MM-DD), "
                    "day_parts(array of strings), earliest_time(string), latest_time(string), "
                    "count(integer), timezone(string), needs_clarification(boolean), clarification_question(string). "
                    f"Assume timezone {self.settings.default_timezone}. Current datetime is {now.isoformat()}. "
                    "Use day_parts values only from: morning, afternoon, evening, night. "
                    "If the user asks for Thursday and Friday evenings, include both dates and day_parts=['evening']. "
                    "If earliest exact time is given, put it into earliest_time as HH:MM. "
                    "If no latest_time is given, infer it from day_parts when possible. "
                    "If the user says evening and gives no exact time, use needs_clarification=true unless you can still safely plan by asking a short follow-up. "
                    "The input may include recent conversation context. Reuse dates, day parts, and time constraints that the user already stated earlier in the same conversation. "
                    "Do not ask again for a date, day part, or time range if it is already clear from the recent conversation. "
                    "Ask at most one short clarification question and only for the single missing piece. "
                    "count is how many separate events the user wants to place. "
                    "clarification_question must be short, friendly, and in Russian, addressing the user as 'ты'."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def parse_reminder_request(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract event reminder requests from Russian or English messages. "
                    "Return only JSON with keys: should_set(boolean), title_query(string), "
                    "search_from_iso(string), search_to_iso(string), minutes_before(integer), "
                    "timezone(string), needs_clarification(boolean), clarification_question(string). "
                    f"Assume timezone {self.settings.default_timezone}. Current datetime is {now.isoformat()}. "
                    "Use minutes_before from phrases like 'за 10 минут', 'за час', 'за 30 минут'. "
                    "Use search_from_iso/search_to_iso to narrow the target event window if user mentions today/tomorrow/day name. "
                    "If the target event is unclear, set needs_clarification=true. "
                    "clarification_question must be short, friendly, and in Russian, addressing the user as 'ты'."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def parse_recurring_request(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract recurring calendar event requests from Russian or English messages. "
                    "Return only JSON with keys: should_create(boolean), title(string), description(string), "
                    "start_iso(string), end_iso(string), timezone(string), recurrence_rule(string), "
                    "needs_clarification(boolean), clarification_question(string). "
                    f"Assume timezone {self.settings.default_timezone}. Current datetime is {now.isoformat()}. "
                    "Use Google Calendar RRULE format, for example RRULE:FREQ=WEEKLY;BYDAY=MO,WE or RRULE:FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR. "
                    "Only set should_create=true when the user clearly asks for a repeating event like every Tuesday, every weekday, each Friday, weekly, or similar. "
                    "If the user does not explicitly ask for repetition, set should_create=false and needs_clarification=false. "
                    "Do not ask whether the event is one-time or recurring unless the user explicitly indicates repetition but leaves the pattern incomplete. "
                    "Always return explicit timezone offsets in start_iso and end_iso. "
                    "If the user asks for a non-recurring event, set should_create=false. "
                    "clarification_question must be short, friendly, and in Russian, addressing the user as 'ты'."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def parse_update_request(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        tomorrow = (now + timedelta(days=1)).date().isoformat()
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract calendar event update requests from Russian or English messages. "
                    "Return only JSON with keys: should_update(boolean), title_query(string), "
                    "search_from_iso(string), search_to_iso(string), "
                    "new_title(string), new_description(string), new_start_iso(string), new_end_iso(string), "
                    "timezone(string), needs_clarification(boolean), clarification_question(string). "
                    f"Assume timezone {self.settings.default_timezone}. "
                    f"Current datetime is {now.isoformat()}. Tomorrow date is {tomorrow}. "
                    "Use title_query to identify the existing event that should be changed. "
                    "Use search_from_iso and search_to_iso as the time window where the existing event should be searched. "
                    "If the user mentions today or tomorrow, use that to narrow the search window. "
                    "If the user gives no explicit search date, default to now through 7 days ahead. "
                    "Use new_start_iso and new_end_iso for the desired updated time. "
                    "If the user only changes title, keep new_start_iso and new_end_iso empty strings. "
                    "If the user only changes time, keep new_title empty unless they explicitly renamed it. "
                    "Always keep explicit timezone offsets in new_start_iso and new_end_iso when they are not empty. "
                    "If it is unclear which existing event should be updated or what should be changed, set needs_clarification=true. "
                    "clarification_question must be friendly, short, and in Russian, speaking to the user with 'ты'."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def parse_cancel_request(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        tomorrow = (now + timedelta(days=1)).date().isoformat()
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract calendar cancellation requests from Russian or English messages. "
                    "Return only JSON with keys: should_cancel(boolean), title_query(string), "
                    "date_from_iso(string), date_to_iso(string), timezone(string), "
                    "needs_clarification(boolean), clarification_question(string). "
                    f"Assume timezone {self.settings.default_timezone}. "
                    f"Current datetime is {now.isoformat()}. Tomorrow date is {tomorrow}. "
                    "If the user says today, use today's date boundaries in that timezone. "
                    "If the user says tomorrow, use tomorrow's date boundaries in that timezone. "
                    "If no date is provided, default to a search window from now to 7 days ahead. "
                    "If no identifiable event title or time clue is present, set needs_clarification=true. "
                    "clarification_question must be friendly, short, and in Russian, speaking to the user with 'ты'."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def parse_events(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        tomorrow = (now + timedelta(days=1)).date().isoformat()
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract one or more calendar events from Russian or English messages. "
                    "Return only a JSON object with keys: "
                    "should_create(boolean), events(array), "
                    "needs_clarification(boolean), clarification_question(string). "
                    "Each item in events must be an object with keys: "
                    "title(string), description(string), start_iso(string), end_iso(string), timezone(string). "
                    "If the message contains several different planned tasks, appointments, or meetings, return all of them in events. "
                    "If the message contains only one event, return a one-item events array. "
                    "If the message is not a request to create calendar events, set should_create=false and events=[]. "
                    f"Assume timezone {self.settings.default_timezone}. "
                    f"Current datetime is {now.isoformat()}. Tomorrow date is {tomorrow}. "
                    "For relative expressions like 'in 30 minutes', 'after that', 'in the evening', "
                    "calculate from the current datetime above in that timezone. "
                    "Always return start_iso and end_iso with an explicit timezone offset. "
                    "If no exact end time is given, infer a reasonable duration. "
                    "If at least one requested event has no usable time, set needs_clarification=true instead of guessing badly. "
                    "clarification_question must always be friendly, short, and in Russian, speaking to the user with 'ты'."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def parse_event(self, text: str) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        tomorrow = (now + timedelta(days=1)).date().isoformat()
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract calendar events from Russian or English messages. "
                    "Return only a JSON object with keys: "
                    "should_create(boolean), title(string), description(string), "
                    "start_iso(string), end_iso(string), timezone(string), "
                    "needs_clarification(boolean), clarification_question(string). "
                    "If the message is not a request to create or move a calendar event, set should_create=false "
                    "and needs_clarification=false. "
                    f"Assume timezone {self.settings.default_timezone}. "
                    f"Current datetime is {now.isoformat()}. Tomorrow date is {tomorrow}. "
                    "For relative expressions like 'in 30 minutes', 'today evening', 'tomorrow morning', "
                    "calculate from the current datetime above in that timezone. "
                    "Always return start_iso and end_iso with an explicit timezone offset. "
                    "If no exact end time is given, infer a reasonable duration. "
                    "If the user only provides a task with no time, set needs_clarification=true. "
                    "clarification_question must always be friendly, short, and in Russian, speaking to the user with 'ты'."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

    async def revise_event_draft(
        self,
        *,
        draft_title: str,
        draft_description: str,
        draft_start_iso: str,
        draft_end_iso: str,
        user_message: str,
    ) -> dict[str, Any]:
        now = datetime.now(self.timezone)
        messages = [
            {
                "role": "system",
                "content": (
                    "You revise an existing calendar event draft based on a follow-up user message. "
                    "Return only JSON with keys: title, description, start_iso, end_iso, timezone, "
                    "needs_clarification, clarification_question. "
                    f"Assume timezone {self.settings.default_timezone}. "
                    f"Current datetime is {now.isoformat()}. "
                    "Always keep explicit timezone offsets in start_iso and end_iso. "
                    "If the user says things like 'not at 15, at 16', 'make it 30 minutes', "
                    "'rename it', or 'move to tomorrow', update only the relevant parts of the draft. "
                    "If the user message is too vague to update the draft safely, set needs_clarification=true. "
                    "clarification_question must be short, friendly, in Russian, and address the user as 'ты'."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Current draft:\n"
                    f"title={draft_title}\n"
                    f"description={draft_description}\n"
                    f"start_iso={draft_start_iso}\n"
                    f"end_iso={draft_end_iso}\n\n"
                    f"User update:\n{user_message}"
                ),
            },
        ]
        return await self.client.chat_json(messages)
