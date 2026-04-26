from __future__ import annotations

import json
from typing import Any

import httpx

from app.config import get_settings


class DeepInfraClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.headers = {"Authorization": f"Bearer {self.settings.deepinfra_api_key}"}

    async def transcribe(self, filename: str, content: bytes) -> str:
        url = f"https://api.deepinfra.com/v1/inference/{self.settings.deepinfra_stt_model}"
        files = {"audio": (filename, content, "audio/ogg")}
        data = {
            "language": "ru",
            "task": "transcribe",
        }
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(url, headers=self.headers, files=files, data=data)
            response.raise_for_status()
            data = response.json()

        for key in ("text", "transcript", "output"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        if isinstance(data.get("results"), list) and data["results"]:
            first = data["results"][0]
            if isinstance(first, dict):
                for key in ("text", "generated_text"):
                    value = first.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()

        raise ValueError(f"Unexpected STT response from DeepInfra: {data}")

    async def chat_json(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        url = "https://api.deepinfra.com/v1/openai/chat/completions"
        payload = {
            "model": self.settings.deepinfra_parser_model,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "temperature": 0.1,
        }

        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json()

        content = data["choices"][0]["message"]["content"]
        if isinstance(content, str):
            return json.loads(content)
        return content

    async def chat_text(self, messages: list[dict[str, str]], temperature: float = 0.3) -> str:
        url = "https://api.deepinfra.com/v1/openai/chat/completions"
        payload = {
            "model": self.settings.deepinfra_parser_model,
            "messages": messages,
            "temperature": temperature,
        }

        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json()

        content = data["choices"][0]["message"]["content"]
        return content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
