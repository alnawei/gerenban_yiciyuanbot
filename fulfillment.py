"""
fulfillment.py — 提货模块（完善版）
新增：
  - fulfillment_status 字段写入（区分 pending/success/failed）
  - 失败后自动通知管理员，并附"手动补发"按钮
  - retry_fulfill_order：管理员手动补发入口，支持二次调用
"""

import logging
from aiogram import Bot
from aiogram.utils.keyboard import InlineKeyboardBuilder

logger = logging.getLogger(__name__)


# ============================================================
# 主提货流程
# ============================================================

async def fulfill_order(
    db_pool,
    bot: Bot,
    order_id: int,
    user_tg_id: int,
    product_id: int,
    is_retry: bool = False,        # 管理员手动补发时传 True
):
    """
    提货主流程：
      1. 查商品 remote_code
      2. 读 API 凭证
      3. 调用 YiCiYuan create_order
      4. 推送卡密给用户
      5. 写 delivery_records（fulfillment_status）
      6. 失败时通知管理员 + 附补发按钮
    """
    from database import (
        get_product_by_id,
        get_system_config,
        save_delivery_record,
        mark_order_fulfilled,
        set_fulfillment_status,
    )
    from yiciyuan_api import YiCiyuanAPI
    from config import ADMIN_ID

    # ── 1. 查商品 ──────────────────────────────────────────────────
    product = await get_product_by_id(db_pool, product_id)
    if not product:
        logger.error("提货失败：商品 #%d 不存在 [order #%d]", product_id, order_id)
        await set_fulfillment_status(db_pool, order_id, "failed")
        await _notify_failure(bot, user_tg_id, order_id, "商品信息不存在，请联系客服")
        await _alert_admin(bot, ADMIN_ID, order_id, user_tg_id, "商品不存在")
        return

    remote_code  = product.get("remote_code")
    product_name = product.get("display_name") or product.get("remote_name")

    # ── 2. 读 API 配置 ─────────────────────────────────────────────
    config           = await get_system_config(db_pool)
    shop_url         = config.get("shop_url")
    app_id           = config.get("app_id")
    app_key          = config.get("app_key")
    customer_service = config.get("customer_service", "")

    if not all([shop_url, app_id, app_key]):
        logger.error("提货失败：API 未配置 [order #%d]", order_id)
        await set_fulfillment_status(db_pool, order_id, "failed")
        await _notify_failure(bot, user_tg_id, order_id, "系统配置异常，请联系客服")
        await _alert_admin(bot, ADMIN_ID, order_id, user_tg_id, "API 凭证未配置")
        return

    # ── 3. 标记"提货中"，防止重复触发 ─────────────────────────────
    await set_fulfillment_status(db_pool, order_id, "processing")

    api    = YiCiyuanAPI(shop_url, app_id, app_key)
    result = await api.create_order(
        commodity_code=remote_code,
        order_id=str(order_id),
    )

    logger.info("YiCiYuan 结果 [order #%d]: code=%s", order_id, result.get("code"))

    if result.get("code") != 0:
        err_msg = result.get("msg", "未知错误")
        logger.error("提货失败 [order #%d]: %s", order_id, err_msg)
        await save_delivery_record(db_pool, order_id, product_id, "failed", "", err_msg)
        await set_fulfillment_status(db_pool, order_id, "failed")
        await _notify_failure(bot, user_tg_id, order_id, err_msg, customer_service)
        await _alert_admin(bot, ADMIN_ID, order_id, user_tg_id, err_msg)
        return

    # ── 4. 解析卡密 ────────────────────────────────────────────────
    data  = result.get("data", {})
    items: list = data.get("list", [])
    if not items and isinstance(data, list):
        items = data

    delivery_text = _format_delivery_items(items)

    # ── 5. 推送给用户 ──────────────────────────────────────────────
    kb = InlineKeyboardBuilder()
    if customer_service:
        cs_url = (
            customer_service
            if customer_service.startswith("http")
            else f"https://t.me/{customer_service.lstrip('@')}"
        )
        kb.button(text="💬 联系客服", url=cs_url)

    retry_note = "\n_（管理员已手动补发）_" if is_retry else ""
    try:
        await bot.send_message(
            user_tg_id,
            f"🎉 *发货成功！*{retry_note}\n\n"
            f"📦 商品：{product_name}\n"
            f"📋 订单号：`#{order_id}`\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"{delivery_text}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"感谢您的购买！如有问题请联系客服。",
            reply_markup=kb.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("推送发货消息失败 [user %d]: %s", user_tg_id, e)

    # ── 6. 落库 ────────────────────────────────────────────────────
    await save_delivery_record(db_pool, order_id, product_id, "success", str(items), "")
    await set_fulfillment_status(db_pool, order_id, "success")
    await mark_order_fulfilled(db_pool, order_id)
    logger.info("✅ 订单 #%d 提货完成", order_id)


# ============================================================
# 管理员手动补发入口
# ============================================================

async def retry_fulfill_order(db_pool, bot: Bot, order_id: int):
    """
    管理员点击"手动补发"按钮后调用。
    从 DB 查出订单信息后复用主流程，is_retry=True。
    """
    import aiomysql
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT user_id, product_id, fulfillment_status FROM recharge_orders WHERE id=%s",
                (order_id,),
            )
            order = await cur.fetchone()

    if not order:
        logger.warning("补发失败：订单 #%d 不存在", order_id)
        return

    if order["fulfillment_status"] == "success":
        logger.warning("订单 #%d 已成功发货，跳过补发", order_id)
        return

    await fulfill_order(
        db_pool    = db_pool,
        bot        = bot,
        order_id   = order_id,
        user_tg_id = order["user_id"],
        product_id = order["product_id"],
        is_retry   = True,
    )


# ============================================================
# 内部工具函数
# ============================================================

def _format_delivery_items(items: list) -> str:
    if not items:
        return "（提货内容为空，请截图联系客服）"
    lines = []
    for i, item in enumerate(items, 1):
        if isinstance(item, dict):
            card = item.get("card_no") or item.get("account") or ""
            pwd  = item.get("card_pwd") or item.get("password") or ""
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
):
    """发货失败 → 通知用户"""
    kb = InlineKeyboardBuilder()
    if customer_service:
        cs_url = (
            customer_service
            if customer_service.startswith("http")
            else f"https://t.me/{customer_service.lstrip('@')}"
        )
        kb.button(text="💬 联系客服处理", url=cs_url)
    try:
        await bot.send_message(
            user_tg_id,
            f"❌ *提货遇到问题*\n\n"
            f"📋 订单号：`#{order_id}`\n"
            f"原因：{reason}\n\n"
            f"您的款项已安全到账，请联系客服手动处理，我们会第一时间为您解决！",
            reply_markup=kb.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("发送失败通知异常 [user %d]: %s", user_tg_id, e)


async def _alert_admin(
    bot: Bot,
    admin_id: int,
    order_id: int,
    user_tg_id: int,
    reason: str,
):
    """
    发货失败 → 通知管理员，附"手动补发"按钮。
    管理员点击后触发 retry_fulfill_order。
    """
    kb = InlineKeyboardBuilder()
    kb.button(text="🔁 手动补发", callback_data=f"admin_retry_{order_id}")
    kb.button(text="👤 查看用户", url=f"tg://user?id={user_tg_id}")
    kb.adjust(1)
    try:
        await bot.send_message(
            admin_id,
            f"🚨 *发货失败告警*\n\n"
            f"📋 订单：`#{order_id}`\n"
            f"👤 用户：`{user_tg_id}`\n"
            f"❌ 原因：{reason}\n\n"
            f"款项已到账，请检查 API 余额/配置后点击补发。",
            reply_markup=kb.as_markup(),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("发送管理员告警失败: %s", e)
