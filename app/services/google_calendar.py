from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

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
                tokens["userinfo"] = {}

        return tokens

    def _build_credentials(self, access_token: str | None, refresh_token: str) -> Credentials:
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
        return credentials

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
        credentials = self._build_credentials(access_token, refresh_token)

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

    async def delete_event(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        event_id: str,
    ) -> None:
        credentials = self._build_credentials(access_token, refresh_token)

        def _delete() -> None:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            service.events().delete(calendarId="primary", eventId=event_id).execute()

        await asyncio.to_thread(_delete)

    async def list_events(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        time_min: datetime,
        time_max: datetime,
        timezone: str,
        limit: int = 10,
    ) -> list[dict]:
        credentials = self._build_credentials(access_token, refresh_token)
        tz = ZoneInfo(timezone)

        def _list() -> list[dict]:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            response = (
                service.events()
                .list(
                    calendarId="primary",
                    timeMin=time_min.astimezone(tz).isoformat(),
                    timeMax=time_max.astimezone(tz).isoformat(),
                    singleEvents=True,
                    orderBy="startTime",
                    maxResults=limit,
                )
                .execute()
            )
            return response.get("items", [])

        return await asyncio.to_thread(_list)

    async def get_next_event(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        timezone: str,
    ) -> dict | None:
        now = datetime.now(ZoneInfo(timezone))
        events = await self.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=now,
            time_max=now + timedelta(days=7),
            timezone=timezone,
            limit=1,
        )
        return events[0] if events else None

    async def find_conflicts(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        start_at: datetime,
        end_at: datetime,
        timezone: str,
    ) -> list[dict]:
        return await self.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=start_at,
            time_max=end_at,
            timezone=timezone,
            limit=20,
        )

    async def suggest_free_slots(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        desired_start: datetime,
        desired_end: datetime,
        timezone: str,
        count: int = 3,
    ) -> list[tuple[datetime, datetime]]:
        tz = ZoneInfo(timezone)
        duration = desired_end - desired_start
        search_start = (desired_start - timedelta(hours=3)).astimezone(tz)
        search_end = (desired_end + timedelta(hours=8)).astimezone(tz)
        events = await self.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=search_start,
            time_max=search_end,
            timezone=timezone,
            limit=50,
        )

        busy_ranges: list[tuple[datetime, datetime]] = []
        for event in events:
            start = self.parse_event_datetime(event, timezone, "start")
            end = self.parse_event_datetime(event, timezone, "end")
            if start and end:
                busy_ranges.append((start, end))

        busy_ranges.sort(key=lambda item: item[0])
        suggestions: list[tuple[datetime, datetime]] = []
        candidate = search_start

        while candidate + duration <= search_end and len(suggestions) < count:
            overlap = next((item for item in busy_ranges if item[0] < candidate + duration and item[1] > candidate), None)
            if overlap:
                candidate = overlap[1]
                continue
            suggestions.append((candidate, candidate + duration))
            candidate += timedelta(hours=1)

        return suggestions

    @staticmethod
    def parse_event_datetime(event: dict, timezone: str, key: str) -> datetime | None:
        value = event.get(key, {})
        tz = ZoneInfo(timezone)
        if "dateTime" in value:
            return datetime.fromisoformat(value["dateTime"]).astimezone(tz)
        if "date" in value:
            return datetime.fromisoformat(value["date"]).replace(tzinfo=tz)
        return None
