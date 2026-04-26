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
