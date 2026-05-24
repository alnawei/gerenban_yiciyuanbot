"""
fulfillment.py — 优化重构版（全面采用 HTML 解析模式，杜绝特殊符号解析崩溃）
"""

import logging
import html
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
        # 转为 HTML 格式
        await bot.send_message(
            user_tg_id,
            f"✅ <b>收款已确认！</b>\n\n"
            f"📋 订单号：<code>#{order_id}</code>\n"
            f"💰 金额：<code>{usdt_amount} USDT</code>\n"
            f"🔗 Tx：<code>{html.escape(tx_hash)}</code>\n\n"
            f"⏳ 正在为您发货，请稍候...",
            parse_mode="HTML",
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
            f"🎉 <b>充值成功！</b>\n\n"
            f"📋 订单号：<code>#{order_id}</code>\n"
            f"💰 到账：<code>{base_amount} USDT</code>\n"
            f"💳 当前余额：<code>{new_balance:.2f} USDT</code>\n\n"
            f"可使用 /shop 购买商品。",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="HTML",
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
    product_name = product.get("display_name") or product.get("remote_name") or "未知商品"

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
        # 使用 HTML 实体发送，对 product_name 进行了 HTML 转义
        await bot.send_message(
            user_tg_id,
            f"🎉 <b>发货成功！</b>\n\n"
            f"📦 商品：{html.escape(product_name)}\n"
            f"📋 订单号：<code>#{order_id}</code>\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"{delivery_text}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"感谢您的购买！",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="HTML",
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

    product_line = f"📦 商品：{html.escape(product_name)}\n" if product_name else ""
    try:
        await bot.send_message(
            user_tg_id,
            f"❌ <b>发货遇到问题</b>\n\n"
            f"{product_line}"
            f"📋 订单号：<code>#{order_id}</code>\n"
            f"原因：{html.escape(reason)}\n\n"
            f"款项已到账，请联系客服处理。",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="HTML",
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
            # 卡密等敏感文本全部使用 html.escape 转义，并用 <code> 标签进行等宽渲染
            if card and pwd:
                lines.append(f"<b>#{i}</b>\n账号：<code>{html.escape(str(card))}</code>\n密码：<code>{html.escape(str(pwd))}</code>")
            elif card:
                lines.append(f"<b>#{i}</b>\n<code>{html.escape(str(card))}</code>")
            elif info:
                lines.append(f"<b>#{i}</b>\n<code>{html.escape(str(info))}</code>")
            else:
                lines.append(f"<b>#{i}</b>\n<code>{html.escape(str(item))}</code>")
        else:
            lines.append(f"<b>#{i}</b>\n<code>{html.escape(str(item))}</code>")
    return "\n\n".join(lines)
