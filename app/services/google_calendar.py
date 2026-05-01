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
        event = await self.create_event_details(
            access_token=access_token,
            refresh_token=refresh_token,
            title=title,
            description=description,
            start_iso=start_iso,
            end_iso=end_iso,
            timezone=timezone,
        )
        return event.get("htmlLink", "")

    async def create_event_details(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        title: str,
        description: str,
        start_iso: str,
        end_iso: str,
        timezone: str,
    ) -> dict:
        credentials = self._build_credentials(access_token, refresh_token)

        event_body = {
            "summary": title,
            "description": description,
            "start": {"dateTime": start_iso, "timeZone": timezone},
            "end": {"dateTime": end_iso, "timeZone": timezone},
        }

        def _insert() -> dict:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            return service.events().insert(calendarId="primary", body=event_body).execute()

        return await asyncio.to_thread(_insert)

    async def create_recurring_event(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        title: str,
        description: str,
        start_iso: str,
        end_iso: str,
        timezone: str,
        recurrence_rule: str,
    ) -> str:
        event = await self.create_recurring_event_details(
            access_token=access_token,
            refresh_token=refresh_token,
            title=title,
            description=description,
            start_iso=start_iso,
            end_iso=end_iso,
            timezone=timezone,
            recurrence_rule=recurrence_rule,
        )
        return event.get("htmlLink", "")

    async def create_recurring_event_details(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        title: str,
        description: str,
        start_iso: str,
        end_iso: str,
        timezone: str,
        recurrence_rule: str,
    ) -> dict:
        credentials = self._build_credentials(access_token, refresh_token)

        event_body = {
            "summary": title,
            "description": description,
            "start": {"dateTime": start_iso, "timeZone": timezone},
            "end": {"dateTime": end_iso, "timeZone": timezone},
            "recurrence": [recurrence_rule],
        }

        def _insert() -> dict:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            return service.events().insert(calendarId="primary", body=event_body).execute()

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

    async def create_event_from_payload(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        payload: dict,
    ) -> dict:
        credentials = self._build_credentials(access_token, refresh_token)

        def _insert() -> dict:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            return service.events().insert(calendarId="primary", body=payload).execute()

        return await asyncio.to_thread(_insert)

    async def truncate_recurring_series(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        recurring_event_id: str,
        keep_until_before: datetime,
    ) -> None:
        credentials = self._build_credentials(access_token, refresh_token)

        def _truncate() -> None:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            event = service.events().get(calendarId="primary", eventId=recurring_event_id).execute()
            recurrence = event.get("recurrence", [])
            until_utc = keep_until_before.astimezone(ZoneInfo("UTC")).strftime("%Y%m%dT%H%M%SZ")
            updated_recurrence: list[str] = []
            for rule in recurrence:
                if not rule.startswith("RRULE:"):
                    updated_recurrence.append(rule)
                    continue
                body = rule[len("RRULE:") :]
                parts = [part for part in body.split(";") if part and not part.startswith("UNTIL=") and not part.startswith("COUNT=")]
                parts.append(f"UNTIL={until_utc}")
                updated_recurrence.append("RRULE:" + ";".join(parts))
            if not updated_recurrence:
                updated_recurrence = [f"RRULE:FREQ=DAILY;COUNT=0;UNTIL={until_utc}"]
            event["recurrence"] = updated_recurrence
            service.events().update(calendarId="primary", eventId=recurring_event_id, body=event).execute()

        await asyncio.to_thread(_truncate)

    async def split_and_update_recurring_series(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        recurring_event_id: str,
        split_from: datetime,
        title: str,
        description: str,
        start_iso: str,
        end_iso: str,
        timezone: str,
    ) -> dict:
        credentials = self._build_credentials(access_token, refresh_token)

        def _normalize_recurrence_for_new_series(recurrence: list[str]) -> list[str]:
            normalized: list[str] = []
            for rule in recurrence:
                if not rule.startswith("RRULE:"):
                    normalized.append(rule)
                    continue
                body = rule[len("RRULE:") :]
                parts = [part for part in body.split(";") if part and not part.startswith("UNTIL=") and not part.startswith("COUNT=")]
                normalized.append("RRULE:" + ";".join(parts))
            return normalized

        def _split() -> dict:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            master = service.events().get(calendarId="primary", eventId=recurring_event_id).execute()
            recurrence = _normalize_recurrence_for_new_series(master.get("recurrence", []))
            until_utc = (split_from - timedelta(seconds=1)).astimezone(ZoneInfo("UTC")).strftime("%Y%m%dT%H%M%SZ")

            updated_recurrence: list[str] = []
            for rule in master.get("recurrence", []):
                if not rule.startswith("RRULE:"):
                    updated_recurrence.append(rule)
                    continue
                body = rule[len("RRULE:") :]
                parts = [part for part in body.split(";") if part and not part.startswith("UNTIL=") and not part.startswith("COUNT=")]
                parts.append(f"UNTIL={until_utc}")
                updated_recurrence.append("RRULE:" + ";".join(parts))
            master["recurrence"] = updated_recurrence
            service.events().update(calendarId="primary", eventId=recurring_event_id, body=master).execute()

            new_body = {
                "summary": title,
                "description": description,
                "start": {"dateTime": start_iso, "timeZone": timezone},
                "end": {"dateTime": end_iso, "timeZone": timezone},
                "recurrence": recurrence,
            }
            return service.events().insert(calendarId="primary", body=new_body).execute()

        return await asyncio.to_thread(_split)

    async def update_event(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        event_id: str,
        title: str | None = None,
        description: str | None = None,
        start_iso: str | None = None,
        end_iso: str | None = None,
        timezone: str | None = None,
    ) -> str:
        event = await self.update_event_details(
            access_token=access_token,
            refresh_token=refresh_token,
            event_id=event_id,
            title=title,
            description=description,
            start_iso=start_iso,
            end_iso=end_iso,
            timezone=timezone,
        )
        return event.get("htmlLink", "")

    async def update_event_details(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        event_id: str,
        title: str | None = None,
        description: str | None = None,
        start_iso: str | None = None,
        end_iso: str | None = None,
        timezone: str | None = None,
    ) -> dict:
        credentials = self._build_credentials(access_token, refresh_token)

        def _update() -> dict:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            event = service.events().get(calendarId="primary", eventId=event_id).execute()
            if title is not None:
                event["summary"] = title
            if description is not None:
                event["description"] = description
            if start_iso and timezone is not None:
                event["start"] = {"dateTime": start_iso, "timeZone": timezone}
            if end_iso and timezone is not None:
                event["end"] = {"dateTime": end_iso, "timeZone": timezone}
            return service.events().update(calendarId="primary", eventId=event_id, body=event).execute()

        return await asyncio.to_thread(_update)

    async def update_recurring_series(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        recurring_event_id: str,
        title: str | None = None,
        description: str | None = None,
        start_iso: str | None = None,
        end_iso: str | None = None,
        timezone: str | None = None,
    ) -> dict:
        return await self.update_event_details(
            access_token=access_token,
            refresh_token=refresh_token,
            event_id=recurring_event_id,
            title=title,
            description=description,
            start_iso=start_iso,
            end_iso=end_iso,
            timezone=timezone,
        )

    async def get_event(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        event_id: str,
    ) -> dict:
        credentials = self._build_credentials(access_token, refresh_token)

        def _get() -> dict:
            service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
            return service.events().get(calendarId="primary", eventId=event_id).execute()

        return await asyncio.to_thread(_get)

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
        desired_start = desired_start.astimezone(tz)
        desired_end = desired_end.astimezone(tz)
        duration = desired_end - desired_start
        search_start = max(datetime.now(tz), desired_start - timedelta(hours=2))
        search_end = desired_end + timedelta(hours=8)
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
        seen: set[str] = set()

        def is_free(candidate_start: datetime) -> bool:
            candidate_end = candidate_start + duration
            return all(not (busy_start < candidate_end and busy_end > candidate_start) for busy_start, busy_end in busy_ranges)

        candidates: list[datetime] = [desired_start]

        # Prefer the nearest free slots around the originally requested time.
        for step in range(1, 17):
            delta = timedelta(minutes=15 * step)
            before = desired_start - delta
            after = desired_start + delta
            if before >= search_start:
                candidates.append(before)
            if after + duration <= search_end:
                candidates.append(after)

        for candidate in candidates:
            normalized = candidate.astimezone(tz).replace(second=0, microsecond=0)
            key = normalized.isoformat()
            if key in seen:
                continue
            seen.add(key)
            if is_free(normalized):
                suggestions.append((normalized, normalized + duration))
            if len(suggestions) >= count:
                break

        return suggestions

    async def find_first_free_slot_in_window(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        window_start: datetime,
        window_end: datetime,
        duration_minutes: int,
        timezone: str,
        step_minutes: int = 15,
    ) -> tuple[datetime, datetime] | None:
        tz = ZoneInfo(timezone)
        window_start = window_start.astimezone(tz).replace(second=0, microsecond=0)
        window_end = window_end.astimezone(tz).replace(second=0, microsecond=0)
        duration = timedelta(minutes=duration_minutes)
        if window_start + duration > window_end:
            return None

        events = await self.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=window_start,
            time_max=window_end,
            timezone=timezone,
            limit=100,
        )
        busy_ranges: list[tuple[datetime, datetime]] = []
        for event in events:
            start = self.parse_event_datetime(event, timezone, "start")
            end = self.parse_event_datetime(event, timezone, "end")
            if start and end:
                busy_ranges.append((start, end))

        def is_free(candidate_start: datetime) -> bool:
            candidate_end = candidate_start + duration
            return all(not (busy_start < candidate_end and busy_end > candidate_start) for busy_start, busy_end in busy_ranges)

        cursor = window_start
        while cursor + duration <= window_end:
            if is_free(cursor):
                return cursor, cursor + duration
            cursor += timedelta(minutes=step_minutes)

        return None

    async def find_free_slots_in_window(
        self,
        *,
        access_token: str | None,
        refresh_token: str,
        window_start: datetime,
        window_end: datetime,
        duration_minutes: int,
        timezone: str,
        count: int = 3,
        step_minutes: int = 15,
    ) -> list[tuple[datetime, datetime]]:
        tz = ZoneInfo(timezone)
        window_start = window_start.astimezone(tz).replace(second=0, microsecond=0)
        window_end = window_end.astimezone(tz).replace(second=0, microsecond=0)
        duration = timedelta(minutes=duration_minutes)
        if window_start + duration > window_end:
            return []

        events = await self.list_events(
            access_token=access_token,
            refresh_token=refresh_token,
            time_min=window_start,
            time_max=window_end,
            timezone=timezone,
            limit=100,
        )
        busy_ranges: list[tuple[datetime, datetime]] = []
        for event in events:
            start = self.parse_event_datetime(event, timezone, "start")
            end = self.parse_event_datetime(event, timezone, "end")
            if start and end:
                busy_ranges.append((start, end))

        def is_free(candidate_start: datetime) -> bool:
            candidate_end = candidate_start + duration
            return all(not (busy_start < candidate_end and busy_end > candidate_start) for busy_start, busy_end in busy_ranges)

        slots: list[tuple[datetime, datetime]] = []
        cursor = window_start
        while cursor + duration <= window_end and len(slots) < count:
            if is_free(cursor):
                slots.append((cursor, cursor + duration))
            cursor += timedelta(minutes=step_minutes)

        return slots

    @staticmethod
    def parse_event_datetime(event: dict, timezone: str, key: str) -> datetime | None:
        value = event.get(key, {})
        tz = ZoneInfo(timezone)
        if "dateTime" in value:
            return datetime.fromisoformat(value["dateTime"]).astimezone(tz)
        if "date" in value:
            return datetime.fromisoformat(value["date"]).replace(tzinfo=tz)
        return None
