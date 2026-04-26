# TaskTrack Bot

Отдельный backend для Telegram-бота, который:

- держит список пользователей со статусами `pending`, `approved`, `blocked`, `admin`
- позволяет админу одобрять и блокировать пользователей
- даёт каждому одобренному пользователю подключить свой Google Calendar
- принимает текст и голосовые в Telegram
- расшифровывает голос через DeepInfra Whisper
- разбирает задачу через LLM на DeepInfra
- создаёт событие в календаре пользователя

## Что уже есть

- отдельный `docker-compose.yml`
- отдельная БД Postgres внутри этого compose-проекта
- polling Telegram-бота внутри приложения
- OAuth-подключение Google Calendar для каждого пользователя
- базовая обработка текста и голосовых

## Структура

```text
app/
  main.py
  config.py
  db.py
  models.py
  security.py
  services/
    deepinfra.py
    google_calendar.py
    parser.py
    telegram_bot.py
```

## Локальный запуск

1. Скопируй пример env:

```bash
cp .env.example .env
```

2. Заполни `.env`

3. Подними сервис:

```bash
docker compose up --build
```

## Как это работает

### Поток доступа

1. Пользователь пишет `/start`
2. Если это админский `Telegram ID`, он получает статус `admin`
3. Остальные создаются как `pending`
4. Админ пишет `/approve <telegram_id>`
5. Пользователь получает ссылку на подключение Google Calendar
6. После подключения может отправлять голосовые и текст

### Команды админа

- `/approve <telegram_id>`
- `/block <telegram_id>`
- `/pending`
- `/users`

## Пошагово: как выложить на GitHub

1. В папке проекта выполни:

```bash
git init
git add .
git commit -m "Initial TaskTrack bot MVP"
```

2. Создай пустой репозиторий на GitHub

3. Привяжи origin:

```bash
git remote add origin YOUR_GIT_URL
git branch -M main
git push -u origin main
```

## Пошагово: как задеплоить на VPS

Ниже отдельный деплой без вмешательства в другие проекты. Всё живёт в своей папке и своём compose.

1. Подключись к серверу
2. Создай отдельную папку:

```bash
mkdir -p /srv/tasktrack-bot
cd /srv/tasktrack-bot
```

3. Клонируй репозиторий:

```bash
git clone YOUR_GIT_URL .
```

4. Создай env:

```bash
cp .env.example .env
nano .env
```

5. Заполни `.env`

Важно:

- `APP_BASE_URL` должен быть публичным HTTPS-доменом, например `https://tasktrackvoice.duckdns.org`
- `GOOGLE_REDIRECT_URI` должен совпадать с OAuth-настройкой в Google Cloud
- `POSTGRES_PASSWORD` и `APP_SECRET_KEY` задай своими

6. Подними контейнеры:

```bash
docker compose up -d --build
```

7. Проверь:

```bash
docker compose ps
docker compose logs -f app
```

8. Проверь health:

Открой:

`https://YOUR_DOMAIN/health`

Должен прийти JSON с `status: ok`

## Что нужно настроить в Google Cloud

1. Создай OAuth Client ID типа `Web application`
2. Добавь Authorized redirect URI:

```text
http://YOUR_SERVER_IP:8010/auth/google/callback
```

3. Вставь `client id` и `client secret` в `.env`

## Что делать после деплоя

1. Напиши боту `/start`
2. С админского аккаунта выполни `/approve <telegram_id>` для тестового пользователя
3. Открой ссылку на подключение Google
4. Отправь боту текст:

```text
созвон завтра в 15:00 на 1 час
```

5. Потом отправь голосовое

## Ограничения первой версии

- Парсинг дат зависит от качества текста и модели
- Для надёжного production лучше потом добавить Redis/очередь и webhook вместо polling
- Пока создаётся именно календарное событие, не отдельная Google Task
