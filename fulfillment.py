"""
fulfillment.py
支付核销后收尾：购买订单走一次元提货；充值订单增加用户余额。
发货/入账失败时通知用户并告警管理员。
"""

import logging
from decimal import Decimal

from aiogram import Bot
from aiogram.utils.keyboard import InlineKeyboardBuilder

from notifications import format_fulfillment_alert, notify_admin

logger = logging.getLogger(__name__)


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
    from amount_lock import release_amount_lock

    if redis_client and final_amount is not None:
        await release_amount_lock(redis_client, final_amount)

    try:
        if order_type == "recharge":
            await _fulfill_recharge(
                db_pool,
                bot,
                order_id,
                user_tg_id,
                base_amount,
                tx_hash=tx_hash,
                final_amount=final_amount,
            )
            return

        if not product_id:
            await _handle_failure(
                bot,
                db_pool,
                order_id=order_id,
                user_tg_id=user_tg_id,
                order_type=order_type,
                reason="购买订单缺少商品 ID",
                tx_hash=tx_hash,
                final_amount=final_amount,
                customer_service="",
                notify_user=False,
            )
            return

        await _fulfill_purchase(
            db_pool,
            bot,
            order_id,
            user_tg_id,
            product_id,
            tx_hash=tx_hash,
            final_amount=final_amount,
        )
    except Exception as e:
        logger.error("fulfill_order 异常 #%d: %s", order_id, e, exc_info=True)
        await _handle_failure(
            bot,
            db_pool,
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type=order_type,
            reason=str(e),
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service="",
            notify_user=True,
        )
        raise


async def _fulfill_recharge(
    db_pool,
    bot: Bot,
    order_id: int,
    user_tg_id: int,
    base_amount: Decimal,
    *,
    tx_hash: str | None = None,
    final_amount: Decimal | None = None,
):
    from database import (
        credit_user_balance,
        get_system_config,
        get_user_balance,
        mark_order_fulfilled,
    )

    config = await get_system_config(db_pool)
    customer_service = config.get("customer_service", "")

    try:
        await credit_user_balance(db_pool, user_tg_id, base_amount)
        new_balance = await get_user_balance(db_pool, user_tg_id)
        await mark_order_fulfilled(db_pool, order_id)
    except Exception as e:
        await _handle_failure(
            bot,
            db_pool,
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type="recharge",
            reason=f"余额入账失败: {e}",
            tx_hash=tx_hash,
            final_amount=final_amount,
            notify_user=True,
            customer_service=customer_service,
        )
        raise

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

    logger.info("✅ 充值订单 #%d 已完成", order_id)


async def _fulfill_purchase(
    db_pool,
    bot: Bot,
    order_id: int,
    user_tg_id: int,
    product_id: int,
    *,
    tx_hash: str | None = None,
    final_amount: Decimal | None = None,
):
    from database import (
        get_product_by_id,
        get_system_config,
        mark_order_fulfilled,
        save_delivery_record,
    )
    from yiciyuan_api import YiCiyuanAPI

    product = await get_product_by_id(db_pool, product_id)
    if not product:
        logger.error("提货失败：商品 #%d 不存在", product_id)
        await _handle_failure(
            bot,
            db_pool,
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type="purchase",
            reason="商品信息不存在",
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service="",
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
        logger.error("提货失败：远端 API 未配置")
        await _handle_failure(
            bot,
            db_pool,
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type="purchase",
            reason="系统 API 未配置",
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service=customer_service,
        )
        return

    api = YiCiyuanAPI(shop_url, app_id, app_key)
    result = await api.create_order(
        commodity_code=remote_code,
        order_id=str(order_id),
    )

    logger.info("YiCiYuan 提货结果 [order #%d]: %s", order_id, result)

    if result.get("code") != 0:
        err_msg = result.get("msg", "未知错误")
        logger.error("提货失败 [order #%d]: %s", order_id, err_msg)
        await save_delivery_record(db_pool, order_id, product_id, "failed", "", err_msg)
        await _handle_failure(
            bot,
            db_pool,
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type="purchase",
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
        await save_delivery_record(
            db_pool, order_id, product_id, "failed", "", "上游返回空卡密"
        )
        await _handle_failure(
            bot,
            db_pool,
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type="purchase",
            reason="上游返回空卡密",
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service=customer_service,
            product_name=product_name,
        )
        return

    delivery_text = _format_delivery_items(items, product_name)

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
            f"感谢您的购买！如有问题请联系客服。",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("推送发货消息失败 [user %d]: %s", user_tg_id, e)
        await _handle_failure(
            bot,
            db_pool,
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type="purchase",
            reason=f"消息推送失败: {e}",
            tx_hash=tx_hash,
            final_amount=final_amount,
            customer_service=customer_service,
            product_name=product_name,
            notify_user=False,
        )
        return

    raw_content = str(items)
    await save_delivery_record(db_pool, order_id, product_id, "success", raw_content, "")
    await mark_order_fulfilled(db_pool, order_id)
    logger.info("✅ 订单 #%d 提货完成", order_id)


async def _handle_failure(
    bot: Bot,
    db_pool,
    *,
    order_id: int,
    user_tg_id: int,
    order_type: str,
    reason: str,
    tx_hash: str | None = None,
    final_amount: Decimal | None = None,
    customer_service: str = "",
    product_name: str = "",
    notify_user: bool = True,
):
    """用户提示 + 管理员告警（款项可能已 paid）。"""
    if notify_user:
        await _notify_failure(
            bot, user_tg_id, order_id, reason, customer_service, product_name
        )

    admin_reason = reason
    if product_name:
        admin_reason = f"[{product_name}] {reason}"

    await notify_admin(
        bot,
        format_fulfillment_alert(
            order_id=order_id,
            user_tg_id=user_tg_id,
            order_type=order_type,
            reason=admin_reason,
            tx_hash=tx_hash,
            final_amount=final_amount,
        ),
    )


def _format_delivery_items(items: list, product_name: str) -> str:
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


async def _notify_failure(
    bot: Bot,
    user_tg_id: int,
    order_id: int,
    reason: str,
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
            f"您的款项已到账，请联系客服手动处理，我们会第一时间为您解决！",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("发送失败通知异常: %s", e)
