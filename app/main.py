from contextlib import asynccontextmanager
import asyncio

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import SessionLocal, engine, get_db_session
from app.models import Base, GoogleAccount, User
from app.security import StateSigner
from app.services.google_calendar import GoogleCalendarService
from app.services.telegram_bot import TelegramBotService


settings = get_settings()
bot_service = TelegramBotService(SessionLocal)
signer = StateSigner()
calendar_service = GoogleCalendarService()


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    polling_task = asyncio.create_task(bot_service.start())
    reminder_task = asyncio.create_task(bot_service.run_reminder_loop())
    try:
        yield
    finally:
        polling_task.cancel()
        reminder_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
        try:
            await reminder_task
        except asyncio.CancelledError:
            pass
        await bot_service.stop()


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/auth/google/start")
async def start_google_auth(tg_id: int = Query(...)) -> dict[str, str]:
    state = signer.dumps({"telegram_id": tg_id})
    return {"url": calendar_service.build_authorize_url(state)}


@app.get("/auth/google/callback")
async def google_callback(
    code: str = Query(...),
    state: str = Query(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, str]:
    payload = signer.loads(state)
    if not payload or "telegram_id" not in payload:
        raise HTTPException(status_code=400, detail="Invalid state")

    telegram_id = int(payload["telegram_id"])
    result = await session.execute(select(User).where(User.telegram_id == telegram_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    tokens = await calendar_service.exchange_code(code)
    email = tokens.get("userinfo", {}).get("email")
    result = await session.execute(select(GoogleAccount).where(GoogleAccount.user_id == user.id))
    account = result.scalar_one_or_none()
    scopes = " ".join(tokens.get("scope", "").split())
    if account:
        account.email = email
        account.refresh_token = tokens.get("refresh_token") or account.refresh_token
        account.access_token = tokens.get("access_token")
        account.scopes = scopes
    else:
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            raise HTTPException(
                status_code=400,
                detail="Google did not return refresh_token. Revoke access and reconnect with prompt=consent.",
            )
        account = GoogleAccount(
            user_id=user.id,
            email=email,
            refresh_token=refresh_token,
            access_token=tokens.get("access_token"),
            scopes=scopes,
        )
        session.add(account)

    user.google_connected = True
    await session.commit()

    await bot_service.bot.send_message(
        telegram_id,
        f"Google Calendar подключён для {email or 'твоего аккаунта'}. Теперь можно присылать текст или голосовые.",
    )
    return {"status": "connected", "email": email or ""}
