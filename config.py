# config.py

# --- 机器人配置 ---
BOT_TOKEN = "8605092149:AAGHNPASj2o7qVslXzhH67-mSHOZuKerhZA"
ADMIN_ID = 5423107259  # 你的Telegram数字ID

# --- 数据库配置 (1Panel默认配置) ---
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "tg_bot_db",
    "password": "CTRWnyzbHxcWaNip",
    "db": "tg_bot_db",
    "charset": "utf8mb4"
}

# --- 业务配置 ---
DEFAULT_MARKUP_PERCENT = 0.2  # 默认加价20% (如果商品没单独设置)

# --- Redis 配置 ---
REDIS_URL = "redis://127.0.0.1:6379/0"

# ============================================================
# Phase 2 新增：支付 & 链上监听配置
# ============================================================

# 固定 USDT(TRC20) 收款地址（你自己的钱包）
USDT_WALLET = "T_YOUR_WALLET_ADDRESS_HERE"

# TronGrid API Key（可选，有 key 速率更高，免费申请：https://www.trongrid.io）
TRONGRID_API_KEY = ""

# 链上轮询间隔（秒），建议 15~30
TRON_POLL_INTERVAL = 15

# 订单过期时间（小时），超过此时间未支付自动关闭
ORDER_EXPIRE_HOURS = 2
