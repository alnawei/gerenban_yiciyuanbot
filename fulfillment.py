"""
fulfillment.py — 核销后的用户通知与购买提货（DB 事务在 settlement.py）
"""

import logging
from decimal import Decimal

from aiogram import Bot
from aiogram.utils.keyboard import InlineKeyboardBuilder

from notifications import format_fulfillment_alert, notify_admin
from settlement import atomic_complete_purchase

logger = logging.getLogger(__name__)


async def notify_payment_received(
    bot: Bot,
    user_tg_id: int,
    order_id: int,
    usdt_amount: Decimal,
    tx_hash: str,
):
    try:
        await bot.send_message(
            user_tg_id,
            f"✅ *收款已确认！*\n\n"
            f"📋 订单号：`#{order_id}`\n"
            f"💰 金额：`{usdt_amount} USDT`\n"
            f"🔗 Tx：`{tx_hash}`\n\n"
            f"⏳ 正在为您发货，请稍候...",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("发送收款确认失败 [user %d]: %s", user_tg_id, e)


async def notify_recharge_completed(
    bot: Bot,
    db_pool,
    user_tg_id: int,
    order_id: int,
    base_amount: Decimal,
):
    from database import get_system_config, get_user_balance

    config = await get_system_config(db_pool)
    customer_service = config.get("customer_service", "")
    new_balance = await get_user_balance(db_pool, user_tg_id)

    builder = InlineKeyboardBuilder()
    if customer_service:
        builder.button(
            text="💬 联系客服",
            url=(
                customer_service
                if customer_service.startswith("http")
                else f"https://t.me/{customer_service.lstrip('@')}"
            ),
        )

    try:
        await bot.send_message(
            user_tg_id,
            f"🎉 *充值成功！*\n\n"
            f"📋 订单号：`#{order_id}`\n"
            f"💰 到账：`{base_amount} USDT`\n"
            f"💳 当前余额：`{new_balance:.6f} USDT`\n\n"
            f"可使用 /shop 购买商品。",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("推送充值成功消息失败 [user %d]: %s", user_tg_id, e)


async def fulfill_purchase_after_paid(
    db_pool,
    bot: Bot,
    order_id: int,
    user_tg_id: int,
    product_id: int | None,
    *,
    tx_hash: str | None = None,
    final_amount: Decimal | None = None,
    redis_client=None,
):
    """购买订单：一次元 API 提货 + 事务写入发货记录（支付已在 settlement 完成）。"""
    from database import get_product_by_id, get_system_config
    from yiciyuan_api import YiCiyuanAPI, YiCiyuanAPIError

    if redis_client and final_amount is not None:
        from amount_lock import release_amount_lock
        await release_amount_lock(redis_client, Decimal(str(final_amount)))

    if not product_id:
        await _handle_failure(
            bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            reason="购买订单缺少商品 ID",
            tx_hash=tx_hash,
            final_amount=final_amount,
        )
        return

    product = await get_product_by_id(db_pool, product_id)
    if not product:
        await _handle_failure(
            bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            reason="商品信息不存在",
            tx_hash=tx_hash,
            final_amount=final_amount,
        )
        return

    remote_code = product.get("remote_code")
    product_name = product.get("display_name") or product.get("remote_name")

    config = await get_system_config(db_pool)
    shop_url = config.get("shop_url")
    app_id = config.get("app_id")
    app_key = config.get("app_key")
    customer_service = config.get("customer_service", "")

    if not all([shop_url, app_id, app_key]):
        await _handle_failure(
            bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            reason="系统 API 未配置",
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service=customer_service,
            product_name=product_name,
        )
        return

    api = YiCiyuanAPI(shop_url, app_id, app_key, bot=bot)

    try:
        result = await api.create_order(
            commodity_code=remote_code,
            order_id=str(order_id),
        )
    except YiCiyuanAPIError as e:
        await _handle_failure(
            bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            reason=str(e),
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service=customer_service,
            product_name=product_name,
        )
        return

    logger.info("YiCiYuan 提货 [order #%d]: %s", order_id, result)

    if result.get("code") != 0:
        err_msg = result.get("msg", "未知错误")
        await atomic_complete_purchase(
            db_pool,
            order_id,
            product_id,
            success=False,
            error_msg=err_msg,
        )
        await _handle_failure(
            bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            reason=err_msg,
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service=customer_service,
            product_name=product_name,
        )
        return

    data = result.get("data", {})
    items: list = data.get("list", [])
    if not items and isinstance(data, list):
        items = data

    if not items:
        await atomic_complete_purchase(
            db_pool,
            order_id,
            product_id,
            success=False,
            error_msg="上游返回空卡密",
        )
        await _handle_failure(
            bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            reason="上游返回空卡密",
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service=customer_service,
            product_name=product_name,
        )
        return

    delivery_text = _format_delivery_items(items)
    raw_content = str(items)

    await atomic_complete_purchase(
        db_pool,
        order_id,
        product_id,
        success=True,
        content=raw_content,
    )

    builder = InlineKeyboardBuilder()
    if customer_service:
        builder.button(
            text="💬 联系客服",
            url=(
                customer_service
                if customer_service.startswith("http")
                else f"https://t.me/{customer_service.lstrip('@')}"
            ),
        )

    try:
        await bot.send_message(
            user_tg_id,
            f"🎉 *发货成功！*\n\n"
            f"📦 商品：{product_name}\n"
            f"📋 订单号：`#{order_id}`\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"{delivery_text}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"感谢您的购买！",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("推送发货消息失败: %s", e)
        await notify_admin(
            bot,
            format_fulfillment_alert(
                order_id=order_id,
                user_tg_id=user_tg_id,
                order_type="purchase",
                reason=f"卡密已入库但消息推送失败: {e}",
                tx_hash=tx_hash,
                final_amount=final_amount,
            ),
        )

    logger.info("✅ 购买订单 #%d 完成", order_id)


# 兼容旧调用（若仍有引用）
async def fulfill_order(
    db_pool,
    bot: Bot,
    order_id: int,
    user_tg_id: int,
    product_id: int | None,
    order_type: str,
    base_amount: Decimal,
    *,
    redis_client=None,
    final_amount: Decimal | None = None,
    tx_hash: str | None = None,
):
    if order_type == "recharge":
        await notify_recharge_completed(
            bot, db_pool, user_tg_id, order_id, base_amount
        )
        return
    await fulfill_purchase_after_paid(
        db_pool,
        bot,
        order_id,
        user_tg_id,
        product_id,
        tx_hash=tx_hash,
        final_amount=final_amount,
        redis_client=redis_client,
    )


async def _handle_failure(
    bot: Bot,
    *,
    order_id: int,
    user_tg_id: int,
    reason: str,
    tx_hash: str | None = None,
    final_amount: Decimal | None = None,
    customer_service: str = "",
    product_name: str = "",
):
    builder = InlineKeyboardBuilder()
    if customer_service:
        cs_url = (
            customer_service
            if customer_service.startswith("http")
            else f"https://t.me/{customer_service.lstrip('@')}"
        )
        builder.button(text="💬 联系客服处理", url=cs_url)

    product_line = f"📦 商品：{product_name}\n" if product_name else ""
    try:
        await bot.send_message(
            user_tg_id,
            f"❌ *发货遇到问题*\n\n"
            f"{product_line}"
            f"📋 订单号：`#{order_id}`\n"
            f"原因：{reason}\n\n"
            f"款项已到账，请联系客服处理。",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("用户失败通知异常: %s", e)

    admin_reason = f"[{product_name}] {reason}" if product_name else reason
    await notify_admin(
        bot,
        format_fulfillment_alert(
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type="purchase",
            reason=admin_reason,
            tx_hash=tx_hash,
            final_amount=final_amount,
        ),
    )


def _format_delivery_items(items: list) -> str:
    if not items:
        return "（提货内容为空，请截图联系客服）"

    lines = []
    for i, item in enumerate(items, 1):
        if isinstance(item, dict):
            card = item.get("card_no") or item.get("account") or ""
            pwd = item.get("card_pwd") or item.get("password") or ""
            info = item.get("info") or item.get("content") or ""
            if card and pwd:
                lines.append(f"*#{i}*\n账号：`{card}`\n密码：`{pwd}`")
            elif card:
                lines.append(f"*#{i}*\n`{card}`")
            elif info:
                lines.append(f"*#{i}*\n`{info}`")
            else:
                lines.append(f"*#{i}*\n`{item}`")
        else:
            lines.append(f"*#{i}*\n`{item}`")
    return "\n\n".join(lines)
