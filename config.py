# config.py — 敏感项从 .env 读取，勿在此硬编码密钥

import os

from dotenv import load_dotenv

load_dotenv()


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


# ── Telegram ─────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = _int_env("ADMIN_ID", 0)

# ── 数据库 ───────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": _int_env("DB_PORT", 3306),
    "user": os.getenv("DB_USER", "tg_bot_db"),
    "password": os.getenv("DB_PASSWORD", ""),
    "db": os.getenv("DB_NAME", "tg_bot_db"),
    "charset": "utf8mb4",
    "minsize": 2,
    "maxsize": 10,
}

# ── Redis ─────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

# ── 业务 ─────────────────────────────────────────────────────
DEFAULT_MARKUP_PERCENT = _float_env("DEFAULT_MARKUP_PERCENT", 0.2)

# ── 支付（Phase 2）───────────────────────────────────────────
USDT_WALLET = os.getenv("USDT_WALLET", "")
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")
TRON_POLL_INTERVAL = _int_env("TRON_POLL_INTERVAL", 15)
ORDER_EXPIRE_MINUTES = _int_env("ORDER_EXPIRE_MINUTES", 30)
