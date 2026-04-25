from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.config import get_settings
from app.services.deepinfra import DeepInfraClient


class TaskParser:
    def __init__(self, client: DeepInfraClient) -> None:
        self.client = client
        self.settings = get_settings()

    async def parse_event(self, text: str) -> dict[str, Any]:
        now = datetime.now().isoformat()
        tomorrow = (datetime.now() + timedelta(days=1)).date().isoformat()
        messages = [
            {
                "role": "system",
                "content": (
                    "You extract calendar events from Russian or English messages. "
                    "Return only a JSON object with keys: "
                    "should_create(boolean), title(string), description(string), "
                    "start_iso(string), end_iso(string), timezone(string), "
                    "needs_clarification(boolean), clarification_question(string). "
                    f"Assume timezone {self.settings.default_timezone}. "
                    f"Current datetime is {now}. Tomorrow date is {tomorrow}. "
                    "If no exact end time is given, infer a reasonable duration. "
                    "If the user only provides a task with no time, set needs_clarification=true."
                ),
            },
            {"role": "user", "content": text},
        ]
        return await self.client.chat_json(messages)

