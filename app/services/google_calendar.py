from __future__ import annotations

import asyncio
from urllib.parse import urlencode

import httpx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from app.config import get_settings


GOOGLE_SCOPES = ["https://www.googleapis.com/auth/calendar.events"]


class GoogleCalendarService:
    def __init__(self) -> None:
        self.settings = get_settings()

    def build_authorize_url(self, state: str) -> str:
        query = urlencode(
            {
                "client_id": self.settings.google_client_id,
                "redirect_uri": self.settings.google_redirect_uri,
                "response_type": "code",
                "access_type": "offline",
                "prompt": "consent",
                "scope": " ".join(GOOGLE_SCOPES),
                "state": state,
            }
        )
        return f"https://accounts.google.com/o/oauth2/v2/auth?{query}"

    async def exchange_code(self, code: str) -> dict:
        payload = {
            "code": code,
            "client_id": self.settings.google_client_id,
            "client_secret": self.settings.google_client_secret,
            "redirect_uri": self.settings.google_redirect_uri,
            "grant_type": "authorization_code",
        }
        async with httpx.AsyncClient(timeout=60) as client:
            token_response = await client.post("https://oauth2.googleapis.com/token", data=payload)
            token_response.raise_for_status()
            tokens = token_response.json()

            userinfo_response = await client.get(
                "https://www.googleapis.com/oauth2/v3/userinfo",
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )
            if userinfo_response.is_success:
                tokens["userinfo"] = userinfo_response.json()
            else:
                # Calendar-only scopes may not allow userinfo. That's fine for this MVP.
                tokens["userinfo"] = {}

        return tokens

    async def create_event(
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
        credentials = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=self.settings.google_client_id,
            client_secret=self.settings.google_client_secret,
            scopes=GOOGLE_SCOPES,
        )
        if not credentials.valid:
            credentials.refresh(Request())

        event_body = {
            "summary": title,
            "description": description,
            "start": {"dateTime": start_iso, "timeZone": timezone},
            "end": {"dateTime": end_iso, "timeZone": timezone},
        }

        def _insert() -> str:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            event = service.events().insert(calendarId="primary", body=event_body).execute()
            return event.get("htmlLink", "")

        return await asyncio.to_thread(_insert)
