# config.py — 所有配置集中在此，不在任何其他文件硬编码

# ── Telegram ─────────────────────────────────────────────────
BOT_TOKEN = "8605092149:AAGHNPASj2o7qVslXzhH67-mSHOZuKerhZA"
ADMIN_ID  = 5423107259  # 管理员 TG 数字 ID

# ── 数据库（1Panel 默认） ──────────────────────────────────────
DB_CONFIG = {
    "host":    "127.0.0.1",
    "port":    3306,
    "user":    "tg_bot_db",
    "password":"CTRWnyzbHxcWaNip",
    "db":      "tg_bot_db",
    "charset": "utf8mb4",
    # 连接池参数（防止连接数爆炸）
    "minsize": 2,
    "maxsize": 10,
}

# ── Redis ─────────────────────────────────────────────────────
REDIS_URL = "redis://127.0.0.1:6379/0"

# ── 业务 ─────────────────────────────────────────────────────
DEFAULT_MARKUP_PERCENT = 0.2  # 商品未单独设价时的默认加价比例

# ── 支付（Phase 2）───────────────────────────────────────────
# 你的 USDT TRC20 固定收款钱包地址（必填）
USDT_WALLET = "T_YOUR_WALLET_ADDRESS_HERE"

# TronGrid API Key（可选，免费申请 https://www.trongrid.io）
# 填写后速率上限大幅提升；留空则使用公共节点（3 req/s）
TRONGRID_API_KEY = ""

# 链上轮询间隔（秒）
# 免费节点建议 >= 15；有 API Key 可调至 5
TRON_POLL_INTERVAL = 15

# 订单有效期（分钟）：超时后 final_amount 坑位自动释放
# 建议 20~30 分钟，与用户付款窗口期匹配
ORDER_EXPIRE_MINUTES = 30
