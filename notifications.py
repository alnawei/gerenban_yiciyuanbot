"""Telegram 管理员告警通知。"""

import logging

from config import ADMIN_ID

logger = logging.getLogger(__name__)


async def notify_admin(bot, message: str) -> bool:
    if not ADMIN_ID:
        logger.warning("ADMIN_ID 未配置，跳过管理员通知")
        return False
    try:
        await bot.send_message(ADMIN_ID, message, parse_mode="Markdown")
        return True
    except Exception as e:
        logger.error("管理员通知发送失败: %s", e)
        return False


def format_fulfillment_alert(
    *,
    order_id: int,
    user_tg_id: int,
    order_type: str,
    reason: str,
    tx_hash: str | None = None,
    final_amount=None,
) -> str:
    lines = [
        "🚨 *发货/入账失败*",
        "",
        f"📋 订单：`#{order_id}`",
        f"👤 用户：`{user_tg_id}`",
        f"📦 类型：`{order_type}`",
    ]
    if final_amount is not None:
        lines.append(f"💰 实付：`{final_amount} USDT`")
    if tx_hash:
        lines.append(f"🔗 Tx：`{tx_hash}`")
    lines.extend(["", f"❗ 原因：{reason}", "", "请尽快人工处理（款项可能已到账）。"])
    return "\n".join(lines)
