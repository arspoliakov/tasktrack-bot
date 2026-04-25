from itsdangerous import BadSignature, URLSafeSerializer

from app.config import get_settings


class StateSigner:
    def __init__(self) -> None:
        settings = get_settings()
        self.serializer = URLSafeSerializer(settings.app_secret_key, salt="tasktrack-google-state")

    def dumps(self, payload: dict) -> str:
        return self.serializer.dumps(payload)

    def loads(self, value: str) -> dict | None:
        try:
            return self.serializer.loads(value)
        except BadSignature:
            return None

