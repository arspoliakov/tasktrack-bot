from __future__ import annotations

from datetime import datetime
from typing import Callable

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models import GoogleAccount, User, UserStatus
from app.security import StateSigner
from app.services.deepinfra import DeepInfraClient
from app.services.google_calendar import GoogleCalendarService
from app.services.parser import TaskParser


class TelegramBotService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.settings = get_settings()
        self.bot = Bot(self.settings.telegram_bot_token)
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
        self.dispatcher.message.register(self.cmd_approve, Command("approve"))
        self.dispatcher.message.register(self.cmd_block, Command("block"))
        self.dispatcher.message.register(self.cmd_pending, Command("pending"))
        self.dispatcher.message.register(self.cmd_users, Command("users"))
        self.dispatcher.message.register(self.handle_voice, F.voice)
        self.dispatcher.message.register(self.handle_text, F.text)

    async def start(self) -> None:
        await self.dispatcher.start_polling(self.bot)

    async def stop(self) -> None:
        await self.bot.session.close()

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

    def _connect_url(self, telegram_id: int) -> str:
        state = self.signer.dumps({"telegram_id": telegram_id})
        return self.calendar_service.build_authorize_url(state)

    async def _access_message(self, user: User) -> str:
        if user.status == UserStatus.BLOCKED.value:
            return "Доступ заблокирован. Напиши администратору."
        if user.status == UserStatus.PENDING.value:
            return (
                "Доступ пока не одобрен.\n"
                f"Твой Telegram ID: {user.telegram_id}\n"
                "Отправь его администратору."
            )
        if not user.google_connected:
            return (
                "Доступ уже одобрен, осталось подключить Google Calendar.\n"
                f"Подключение: {self._connect_url(user.telegram_id)}"
            )
        return ""

    async def _ensure_access(self, message: Message, session: AsyncSession) -> User | None:
        user = await self._get_or_create_user(message, session)
        await session.commit()
        if user.status not in (UserStatus.ADMIN.value, UserStatus.APPROVED.value):
            await message.answer(await self._access_message(user))
            return None
        if not user.google_connected:
            await message.answer(await self._access_message(user))
            return None
        return user

    async def cmd_start(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._get_or_create_user(message, session)
            await session.commit()
            if user.status == UserStatus.ADMIN.value:
                await message.answer(
                    "Ты админ.\n"
                    "Команды:\n"
                    "/approve <telegram_id>\n"
                    "/block <telegram_id>\n"
                    "/pending\n"
                    "/users"
                )
                return

            access_message = await self._access_message(user)
            if access_message:
                await message.answer(access_message)
                return

            await message.answer("Готово. Можешь присылать текст или голосовое для создания события в календаре.")

    async def cmd_help(self, message: Message) -> None:
        await message.answer(
            "Бот создаёт события в Google Calendar.\n"
            "Сначала нужен approve от админа, потом подключение Google."
        )

    async def cmd_approve(self, message: Message, command: CommandObject) -> None:
        if message.from_user.id != self.settings.admin_telegram_id:
            await message.answer("Только админ может одобрять пользователей.")
            return
        if not command.args or not command.args.isdigit():
            await message.answer("Используй: /approve <telegram_id>")
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
        await message.answer(f"Пользователь {target_tg_id} одобрен.")
        await self.bot.send_message(
            target_tg_id,
            "Доступ одобрен.\n"
            f"Теперь подключи Google Calendar: {connect_url}",
        )

    async def cmd_block(self, message: Message, command: CommandObject) -> None:
        if message.from_user.id != self.settings.admin_telegram_id:
            await message.answer("Только админ может блокировать пользователей.")
            return
        if not command.args or not command.args.isdigit():
            await message.answer("Используй: /block <telegram_id>")
            return

        target_tg_id = int(command.args)
        async with self.session_factory() as session:
            user = await self._load_user_by_telegram_id(target_tg_id, session)
            if not user:
                await message.answer("Пользователь не найден.")
                return
            user.status = UserStatus.BLOCKED.value
            await session.commit()

        await message.answer(f"Пользователь {target_tg_id} заблокирован.")
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
        lines = [f"{u.telegram_id} | @{u.username or '-'} | {u.first_name or '-'}" for u in users]
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
            f"{u.telegram_id} | {u.status} | google={'yes' if u.google_connected else 'no'} | @{u.username or '-'}"
            for u in users
        ]
        await message.answer("Последние пользователи:\n" + "\n".join(lines))

    async def handle_text(self, message: Message) -> None:
        if message.text.startswith("/"):
            return

        async with self.session_factory() as session:
            user = await self._ensure_access(message, session)
            if not user:
                return
            google_account = (await session.execute(select(GoogleAccount).where(GoogleAccount.user_id == user.id))).scalar_one()

        await self._process_event_request(
            message=message,
            text=message.text,
            refresh_token=google_account.refresh_token,
            access_token=google_account.access_token,
        )

    async def handle_voice(self, message: Message) -> None:
        async with self.session_factory() as session:
            user = await self._ensure_access(message, session)
            if not user:
                return
            google_account = (await session.execute(select(GoogleAccount).where(GoogleAccount.user_id == user.id))).scalar_one()

        await message.answer("Получил голосовое, расшифровываю...")
        file = await self.bot.get_file(message.voice.file_id)
        file_bytes = await self.bot.download_file(file.file_path)
        transcript = await self.deepinfra.transcribe("voice.ogg", file_bytes.read())
        await message.answer(f"Текст: {transcript}")
        await self._process_event_request(
            message=message,
            text=transcript,
            refresh_token=google_account.refresh_token,
            access_token=google_account.access_token,
        )

    async def _process_event_request(
        self,
        *,
        message: Message,
        text: str,
        refresh_token: str,
        access_token: str | None,
    ) -> None:
        parsed = await self.parser.parse_event(text)
        if parsed.get("needs_clarification"):
            await message.answer(parsed.get("clarification_question") or "Нужно уточнение по дате или времени.")
            return
        if not parsed.get("should_create"):
            await message.answer("Не смог уверенно собрать событие. Попробуй сформулировать точнее.")
            return

        link = await self.calendar_service.create_event(
            access_token=access_token,
            refresh_token=refresh_token,
            title=parsed.get("title") or "Новое событие",
            description=parsed.get("description") or text,
            start_iso=parsed["start_iso"],
            end_iso=parsed["end_iso"],
            timezone=parsed.get("timezone") or self.settings.default_timezone,
        )
        await message.answer(f"Событие создано.\n{link}")

