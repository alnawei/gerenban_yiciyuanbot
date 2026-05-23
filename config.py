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

# ── 占位符检测（生产启动自检）────────────────────────────────
_PLACEHOLDER_BOT_TOKENS = frozenset({
    "",
    "your_telegram_bot_token",
    "your_bot_token_here",
    "changeme",
})
_PLACEHOLDER_DB_PASSWORDS = frozenset({
    "",
    "your_mysql_password",
    "changeme",
})
_PLACEHOLDER_WALLETS = frozenset({
    "",
    "t_your_wallet_address_here",
    "tyourtronwalletaddress",
    "your_wallet_here",
    "changeme",
})


def _is_placeholder(value: str, placeholders: frozenset[str]) -> bool:
    return value.strip().lower() in placeholders


def validate_production_env() -> None:
    """
    启动前校验核心环境变量；仍为占位符或明显无效时抛出 RuntimeError。
    """
    errors: list[str] = []

    if _is_placeholder(BOT_TOKEN, _PLACEHOLDER_BOT_TOKENS):
        errors.append("BOT_TOKEN 未配置或仍为 .env.example 占位符")

    if ADMIN_ID <= 0:
        errors.append("ADMIN_ID 未配置或无效（需为 Telegram 数字 ID）")

    if _is_placeholder(DB_CONFIG.get("password", ""), _PLACEHOLDER_DB_PASSWORDS):
        errors.append("DB_PASSWORD 未配置或仍为占位符")

    wallet = USDT_WALLET.strip()
    if _is_placeholder(wallet, _PLACEHOLDER_WALLETS):
        errors.append("USDT_WALLET 未配置或仍为占位符")
    elif not wallet.startswith("T") or len(wallet) < 30:
        errors.append("USDT_WALLET 格式无效（TRON 地址应以 T 开头）")

    if errors:
        raise RuntimeError(
            "生产环境配置检查失败，请修正 .env 后重试：\n- "
            + "\n- ".join(errors)
        )
