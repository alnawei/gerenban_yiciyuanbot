"""
main.py
Telegram 机器人主入口 — Phase 1 + Phase 2 完整版
"""

# 日志初始化必须最先执行
from logger_setup import setup_logging
setup_logging()

import asyncio
import logging
import random
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import aiomysql
import redis.asyncio as redis
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder

# 自定义模块
from config import (
    BOT_TOKEN, ADMIN_ID, REDIS_URL,
    USDT_WALLET, TRONGRID_API_KEY, TRON_POLL_INTERVAL, ORDER_EXPIRE_MINUTES,
)
from database import (
    get_db_pool, get_system_config, update_system_config,
    save_remote_products, get_active_categories, get_user_data, get_shop_stats,
    get_product_by_id, create_order, expire_stale_orders,
    get_failed_fulfillment_orders,
)
from yiciyuan_api import YiCiyuanAPI

# ============================================================
# 初始化
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


class AdminSetup(StatesGroup):
    waiting_for_url = State()
    waiting_for_id = State()
    waiting_for_key = State()
    waiting_for_service = State()
    waiting_for_new_name = State()
    waiting_for_new_cat_name = State()
    waiting_for_price = State()        # Phase 2: 设置价格


# ============================================================
# 管理员指令：/admin
# ============================================================

@dp.message(Command("admin"), F.from_user.id == ADMIN_ID)
async def admin_menu(message: types.Message, db_pool):
    config = await get_system_config(db_pool)
    shop_url = config.get("shop_url")
    total_count, active_count = await get_shop_stats(db_pool)

    builder = InlineKeyboardBuilder()
    if not shop_url:
        text = "⚙️ **管理员面板**\n\n目前尚未配置远程商铺。"
        builder.button(text="➕ 添加商铺", callback_data="setup_shop")
    else:
        service = config.get("customer_service", "未设置")
        text = (
            f"⚙️ **管理员面板**\n\n"
            f"🔗 商铺: `{shop_url}`\n"
            f"☎️ 客服: `{service}`\n\n"
            f"📊 **商品统计**\n"
            f"总计同步: `{total_count}` 个\n"
            f"当前上架: `{active_count}` 个"
        )
        builder.button(text=f"🔄 立即同步 ({total_count})", callback_data="sync_now")
        builder.button(text=f"📦 上架管理 ({active_count})", callback_data="admin_manage_goods")
        builder.button(text="🚨 发货失败订单", callback_data="admin_failed_orders")
        builder.button(text="👤 设置客服地址", callback_data="set_customer_service")
        builder.button(text="❌ 删除商铺配置", callback_data="delete_shop")

    builder.adjust(1)
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")


# ============================================================
# 管理员：商品改名
# ============================================================

@dp.callback_query(F.data.startswith("rename_"), F.from_user.id == ADMIN_ID)
async def start_rename(callback: types.CallbackQuery, state: FSMContext):
    p_id = callback.data.replace("rename_", "")
    await state.update_data(rename_target_id=p_id)
    await callback.message.answer("⌨️ 请输入该【商品】的新名称：\n(输入 'cancel' 取消)")
    await state.set_state(AdminSetup.waiting_for_new_name)
    await callback.answer()


@dp.message(AdminSetup.waiting_for_new_name, F.from_user.id == ADMIN_ID)
async def process_new_name(message: types.Message, state: FSMContext, db_pool):
    new_name = message.text.strip()
    if new_name.lower() == "cancel":
        await state.clear()
        return await message.answer("❌ 已取消商品改名。")

    data = await state.get_data()
    p_id = data.get("rename_target_id")

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("UPDATE products SET display_name=%s WHERE id=%s", (new_name, p_id))
            await conn.commit()
            await cur.execute("SELECT category_name FROM products WHERE id=%s", (p_id,))
            res = await cur.fetchone()
            cat_name = res["category_name"] if res else ""

    kb = InlineKeyboardBuilder()
    if cat_name:
        kb.button(text="⬅️ 返回商品列表", callback_data=f"ashelf_cat_{cat_name}")
    kb.button(text="🔄 重新修改", callback_data=f"rename_{p_id}")
    kb.adjust(1)

    await message.answer(f"✅ 商品名已更新为：\n`{new_name}`", reply_markup=kb.as_markup(), parse_mode="Markdown")
    await state.clear()


# ============================================================
# 管理员：分类改名
# ============================================================

@dp.callback_query(F.data.startswith("rencat_"), F.from_user.id == ADMIN_ID)
async def start_rename_cat(callback: types.CallbackQuery, state: FSMContext):
    old_name = callback.data.replace("rencat_", "")
    await state.update_data(old_cat_name=old_name)
    await callback.message.answer(f"⌨️ 请输入【分类：{old_name}】的新名称：\n(输入 'cancel' 取消)")
    await state.set_state(AdminSetup.waiting_for_new_cat_name)
    await callback.answer()


@dp.message(AdminSetup.waiting_for_new_cat_name, F.from_user.id == ADMIN_ID)
async def process_new_cat_name(message: types.Message, state: FSMContext, db_pool):
    new_cat_name = message.text.strip()
    if new_cat_name.lower() == "cancel":
        await state.clear()
        return await message.answer("❌ 已取消分类改名。")

    data = await state.get_data()
    old_cat_name = data.get("old_cat_name")

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE products SET category_name=%s WHERE category_name=%s",
                (new_cat_name, old_cat_name),
            )
            await conn.commit()

    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ 返回分类列表", callback_data="admin_manage_goods")

    await message.answer(
        f"✅ 分类名已修改：\n`{old_cat_name}` ➡️ `{new_cat_name}`",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown",
    )
    await state.clear()


# ============================================================
# 管理员：设置商品价格（Phase 2 新增）
# ============================================================

@dp.callback_query(F.data.startswith("setprice_"), F.from_user.id == ADMIN_ID)
async def start_set_price(callback: types.CallbackQuery, state: FSMContext, db_pool):
    p_id = callback.data.replace("setprice_", "")
    product = await get_product_by_id(db_pool, int(p_id))
    current = product["price"] if product and product.get("price") else "未设置"
    await state.update_data(price_target_id=p_id)
    await callback.message.answer(
        f"💰 当前售价：`{current}` USDT\n\n请输入新的 USDT 售价（纯数字，如 15.5）：\n(输入 'cancel' 取消)",
        parse_mode="Markdown",
    )
    await state.set_state(AdminSetup.waiting_for_price)
    await callback.answer()


@dp.message(AdminSetup.waiting_for_price, F.from_user.id == ADMIN_ID)
async def process_set_price(message: types.Message, state: FSMContext, db_pool):
    text = message.text.strip()
    if text.lower() == "cancel":
        await state.clear()
        return await message.answer("❌ 已取消设置价格。")

    try:
        price = Decimal(text).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if price <= 0:
            raise ValueError
    except Exception:
        return await message.answer("⚠️ 请输入有效的正数价格，例如：15.5")

    data = await state.get_data()
    p_id = data.get("price_target_id")

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("UPDATE products SET price=%s WHERE id=%s", (price, p_id))
            await conn.commit()
            await cur.execute("SELECT category_name FROM products WHERE id=%s", (p_id,))
            res = await cur.fetchone()
            cat_name = res["category_name"] if res else ""

    kb = InlineKeyboardBuilder()
    if cat_name:
        kb.button(text="⬅️ 返回商品列表", callback_data=f"ashelf_cat_{cat_name}")
    kb.button(text="💰 再次修改", callback_data=f"setprice_{p_id}")
    kb.adjust(1)

    await message.answer(
        f"✅ 售价已更新为：`{price} USDT`",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown",
    )
    await state.clear()


# ============================================================
# 管理员：分类 & 商品管理面板
# ============================================================

@dp.callback_query(F.data == "admin_manage_goods", F.from_user.id == ADMIN_ID)
async def admin_manage_cats(callback: types.CallbackQuery, db_pool):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT category_name, COUNT(*) as total, SUM(is_active) as active_count
                FROM products GROUP BY category_name
            """)
            cats = await cur.fetchall()

    builder = InlineKeyboardBuilder()
    for c in cats:
        cat_name = c["category_name"]
        active = c["active_count"] or 0
        short_cat = (cat_name[:12] + "..") if len(cat_name) > 12 else cat_name
        builder.button(text=f"📂 {short_cat} ({active}/{c['total']})", callback_data=f"ashelf_cat_{cat_name}")
        builder.button(text="📝 编辑", callback_data=f"rencat_{cat_name}")

    builder.adjust(2)
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回主菜单", callback_data="back_to_admin"))

    await callback.message.edit_text(
        "📂 **分类管理面板**\n点击名称管理商品，点击右侧 📝 重命名",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("ashelf_cat_"), F.from_user.id == ADMIN_ID)
async def admin_on_shelf(callback: types.CallbackQuery, db_pool):
    cat_name = callback.data.replace("ashelf_cat_", "")
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT id, remote_name, display_name, is_active FROM products WHERE category_name=%s ORDER BY id ASC",
                (cat_name,),
            )
            prods = await cur.fetchall()

    builder = InlineKeyboardBuilder()
    for p in prods:
        actual_name = p["display_name"] if p["display_name"] else p["remote_name"]
        short_name = (actual_name[:12] + "..") if len(actual_name) > 12 else actual_name
        status_icon = "✅" if p["is_active"] == 1 else "❌"
        builder.button(text=f"📦 {short_name}", callback_data="none")
        builder.button(text=status_icon, callback_data=f"t_{p['id']}")
        builder.button(text="⚙️ 编辑", callback_data=f"edit_opt_{p['id']}")

    builder.adjust(3)
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回分类列表", callback_data="admin_manage_goods"))

    timestamp = time.strftime("%H:%M:%S")
    await callback.message.edit_text(
        f"🛠 **管理分类**：{cat_name}\n(实时状态刷新: {timestamp})",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("edit_opt_"), F.from_user.id == ADMIN_ID)
async def show_edit_options(callback: types.CallbackQuery):
    p_id = callback.data.replace("edit_opt_", "")
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 修改名称", callback_data=f"rename_{p_id}")
    builder.button(text="💰 设置价格", callback_data=f"setprice_{p_id}")
    builder.button(text="⬅️ 取消", callback_data="admin_manage_goods")
    builder.adjust(1)

    await callback.message.edit_text(
        "⚙️ **选择编辑操作**：\n您可以修改商品显示名称或设定售价。",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("t_"), F.from_user.id == ADMIN_ID)
async def toggle_product(callback: types.CallbackQuery, db_pool):
    p_id = callback.data.replace("t_", "")

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT is_active, category_name FROM products WHERE id=%s", (p_id,))
            product = await cur.fetchone()
            if not product:
                return await callback.answer("未找到商品")

            new_status = 0 if product["is_active"] == 1 else 1
            await cur.execute("UPDATE products SET is_active=%s WHERE id=%s", (new_status, p_id))
            await conn.commit()

            cat_name = product["category_name"]
            await cur.execute(
                "SELECT id, remote_name, display_name, is_active FROM products WHERE category_name=%s ORDER BY id ASC",
                (cat_name,),
            )
            prods = await cur.fetchall()

    builder = InlineKeyboardBuilder()
    for p in prods:
        actual_name = p["display_name"] if p["display_name"] else p["remote_name"]
        short_name = (actual_name[:12] + "..") if len(actual_name) > 12 else actual_name
        status_icon = "✅" if p["is_active"] == 1 else "❌"
        builder.button(text=f"📦 {short_name}", callback_data="none")
        builder.button(text=status_icon, callback_data=f"t_{p['id']}")
        builder.button(text="📝", callback_data=f"rename_{p['id']}")

    builder.adjust(3)
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回分类列表", callback_data="admin_manage_goods"))

    timestamp = time.strftime("%H:%M:%S")
    try:
        await callback.message.edit_text(
            f"🛠 **管理分类**：{cat_name}\n(状态已同步 - {timestamp})",
            reply_markup=builder.as_markup(),
            parse_mode="Markdown",
        )
        await callback.answer(f"已{'开启' if new_status else '关闭'}")
    except Exception:
        await callback.answer("状态已更新")


# ============================================================
# 管理员：客服设置
# ============================================================

@dp.callback_query(F.data == "set_customer_service", F.from_user.id == ADMIN_ID)
async def cb_set_service(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("📝 请输入客服地址 (例如: @yourname 或 https://t.me/xxx):")
    await state.set_state(AdminSetup.waiting_for_service)
    await callback.answer()


@dp.message(AdminSetup.waiting_for_service, F.from_user.id == ADMIN_ID)
async def process_service_link(message: types.Message, state: FSMContext, db_pool):
    link = message.text
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
            INSERT INTO system_config (config_key, config_value)
            VALUES ('customer_service', %s)
            ON DUPLICATE KEY UPDATE config_value=%s
            """
            await cur.execute(sql, (link, link))

    await message.answer(f"✅ 客服地址已设置为: {link}")
    await state.clear()


@dp.callback_query(F.data == "back_to_admin", F.from_user.id == ADMIN_ID)
async def cb_back_to_admin(callback: types.CallbackQuery, db_pool):
    await admin_menu(callback.message, db_pool)
    await callback.answer()


# ============================================================
# 用户端：/start  /shop  /me
# ============================================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "🤖 *代理零售机器人已就绪*\n\n"
        "/shop — 浏览商品\n"
        "/me — 个人中心",
        parse_mode="Markdown",
    )


@dp.message(Command("shop"))
async def cmd_shop(message: types.Message, db_pool):
    await show_categories(message, db_pool)


async def show_categories(message: types.Message, db_pool):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT DISTINCT category_name FROM products WHERE is_active=1"
            )
            cats = await cur.fetchall()

    if not cats:
        return await message.answer("😔 暂无上架商品，请稍后再来。")

    builder = InlineKeyboardBuilder()
    for c in cats:
        builder.button(text=c["category_name"], callback_data=f"cat_{c['category_name']}")
    builder.adjust(1)
    await message.answer("请选择商品分类：", reply_markup=builder.as_markup())


@dp.message(Command("me"))
async def cmd_me(message: types.Message, db_pool):
    user = await get_user_data(db_pool, message.from_user.id)
    text = (
        f"👤 **个人中心**\n\n"
        f"🆔 ID: `{message.from_user.id}`\n"
        f"💰 余额: `{user['balance']:.2f}` USDT"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="💳 立即充值", callback_data="recharge")
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")


# ============================================================
# 用户端：分类 → 商品列表
# ============================================================

@dp.callback_query(F.data.startswith("cat_"))
async def show_products_by_cat(callback: types.CallbackQuery, db_pool):
    cat_name = callback.data.replace("cat_", "")

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, remote_name, display_name, price
                FROM products
                WHERE category_name=%s AND is_active=1
                """,
                (cat_name,),
            )
            prods = await cur.fetchall()

    if not prods:
        return await callback.answer("该分类暂无商品上架", show_alert=True)

    builder = InlineKeyboardBuilder()
    for p in prods:
        name = p["display_name"] if p["display_name"] else p["remote_name"]
        price_str = f"{p['price']} USDT" if p.get("price") else "价格面议"
        builder.button(text=f"{name} — {price_str}", callback_data=f"buy_{p['id']}")

    builder.adjust(1)
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回分类", callback_data="back_to_shop"))

    await callback.message.edit_text(
        f"🛒 **{cat_name}** 商品列表：",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )
    await callback.answer()


@dp.callback_query(F.data == "back_to_shop")
async def back_to_shop(callback: types.CallbackQuery, db_pool):
    await show_categories(callback.message, db_pool)
    await callback.answer()


# ============================================================
# 用户端：下单 — Phase 2 核心流程
# ============================================================

@dp.callback_query(F.data.startswith("buy_"))
async def buy_product(callback: types.CallbackQuery, db_pool):
    """
    用户点击商品 → 生成附随机小数的唯一 final_amount → 展示付款地址。

    防冲突：不重复抽取 offset，最多 20 次；冲突报错时提示稍后重试。
    手续费提示：明确告知交易所提现需自补手续费。
    倒计时：消息中显示精确截止时间。
    """
    p_id = int(callback.data.replace("buy_", ""))
    user_tg_id = callback.from_user.id

    product = await get_product_by_id(db_pool, p_id)
    if not product:
        return await callback.answer("❌ 商品不存在", show_alert=True)

    price = product.get("price")
    if not price:
        return await callback.answer("❌ 该商品尚未设置价格，请联系客服", show_alert=True)

    base_amount = Decimal(str(price)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    # ── 生成唯一 final_amount ────────────────────────────────────────
    order_id:      int | None     = None
    final_amount:  Decimal | None = None
    tried_offsets: set[int]       = set()

    for _ in range(20):
        remaining = set(range(1, 100)) - tried_offsets
        if not remaining:
            break
        offset_int = random.choice(list(remaining))
        tried_offsets.add(offset_int)
        candidate = (base_amount + Decimal(offset_int) / Decimal("100")).quantize(
            Decimal("0.01")
        )
        result = await create_order(
            db_pool, user_tg_id, p_id, base_amount, candidate,
            expire_minutes=ORDER_EXPIRE_MINUTES,
        )
        if result:
            order_id, final_amount = result, candidate
            break

    if not order_id:
        logger.error(
            "用户 %d 下单失败：base=%s，已试 %d 个 offset 全部冲突",
            user_tg_id, base_amount, len(tried_offsets),
        )
        return await callback.answer(
            "⚠️ 当前价格档位已满，请稍后重试或联系客服", show_alert=True
        )

    # ── 计算截止时间字符串（UTC+8）──────────────────────────────────
    from datetime import datetime, timedelta
    expire_dt  = datetime.utcnow() + timedelta(minutes=ORDER_EXPIRE_MINUTES, hours=8)
    expire_str = expire_dt.strftime("%H:%M")   # 显示"XX:XX 前"

    product_name     = product.get("display_name") or product.get("remote_name")
    config           = await get_system_config(db_pool)
    customer_service = config.get("customer_service", "")

    builder = InlineKeyboardBuilder()
    if customer_service:
        cs_url = (
            customer_service
            if customer_service.startswith("http")
            else f"https://t.me/{customer_service.lstrip('@')}"
        )
        builder.button(text="💬 联系客服", url=cs_url)
    builder.button(
        text=f"❌ 取消订单（剩余 {ORDER_EXPIRE_MINUTES} 分钟）",
        callback_data=f"cancel_order_{order_id}",
    )
    builder.adjust(1)

    pay_text = (
        f"🛒 *下单成功！请在* `{expire_str}` *前完成付款*\n\n"
        f"📦 商品：{product_name}\n"
        f"📋 订单号：`#{order_id}`\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"💳 *转账信息（TRC20-USDT）*\n\n"
        f"收款地址：\n"
        f"`{USDT_WALLET}`\n\n"
        f"⚠️ *必须转入以下精确金额：*\n"
        f"# `{final_amount} USDT` #\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏦 *从交易所（币安/欧易等）提现请注意：*\n"
        f"交易所会额外扣除手续费（通常 1\~2 USDT），\n"
        f"请在提现金额中*自行加上手续费*，确保\n"
        f"*实际到账金额*恰好为 `{final_amount} USDT`。\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"转账后系统将 *自动核销发货*，无需通知。\n"
        f"⏰ 订单 *{ORDER_EXPIRE_MINUTES} 分钟*后失效，超时请重新下单。"
    )

    await callback.message.answer(
        pay_text, reply_markup=builder.as_markup(), parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order(callback: types.CallbackQuery, db_pool):
    """用户主动取消待支付订单，立即释放 final_amount 坑位。"""
    order_id   = int(callback.data.replace("cancel_order_", ""))
    user_tg_id = callback.from_user.id

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # 同时检查是否已过期，给出不同提示
            await cur.execute(
                "SELECT status FROM recharge_orders WHERE id=%s AND user_id=%s",
                (order_id, user_tg_id),
            )
            row = await cur.fetchone()

    if not row:
        return await callback.answer("❌ 订单不存在", show_alert=True)

    if row["status"] == "expired":
        return await callback.answer(
            "⏰ 该订单已超时失效，无需取消。如需购买请重新下单。",
            show_alert=True,
        )
    if row["status"] != "pending":
        return await callback.answer(
            "该订单已支付或处理中，无法取消。如有问题请联系客服。",
            show_alert=True,
        )

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE recharge_orders SET status='cancelled' "
                "WHERE id=%s AND user_id=%s AND status='pending'",
                (order_id, user_tg_id),
            )
            updated = cur.rowcount

    if updated:
        await callback.message.edit_text(
            f"❌ 订单 `#{order_id}` 已取消，金额坑位已释放。",
            parse_mode="Markdown",
        )
    else:
        await callback.answer("操作失败，请重试", show_alert=True)


# ============================================================
# 管理员：手动补发
# ============================================================

@dp.callback_query(F.data.startswith("admin_retry_"), F.from_user.id == ADMIN_ID)
async def admin_retry_fulfill(callback: types.CallbackQuery, db_pool):
    """管理员点击"手动补发"按钮，调用 fulfillment.retry_fulfill_order。"""
    from fulfillment import retry_fulfill_order
    order_id = int(callback.data.replace("admin_retry_", ""))

    await callback.message.edit_text(
        f"🔁 正在为订单 `#{order_id}` 执行补发...",
        parse_mode="Markdown",
    )
    await callback.answer()

    await retry_fulfill_order(db_pool, bot, order_id)

    # 补发完成后更新按钮消息
    try:
        await callback.message.edit_text(
            f"✅ 订单 `#{order_id}` 补发指令已下发，请查看用户是否收到货。",
            parse_mode="Markdown",
        )
    except Exception:
        pass


@dp.callback_query(F.data == "admin_failed_orders", F.from_user.id == ADMIN_ID)
async def admin_failed_orders(callback: types.CallbackQuery, db_pool):
    """显示最近 20 条发货失败的订单列表。"""
    orders = await get_failed_fulfillment_orders(db_pool, limit=20)

    if not orders:
        return await callback.answer("✅ 暂无发货失败订单", show_alert=True)

    builder = InlineKeyboardBuilder()
    lines   = ["🚨 *发货失败订单列表*\n"]
    for o in orders:
        name      = o.get("display_name") or o.get("remote_name") or "未知商品"
        short_err = (o.get("error_msg") or "")[:20]
        lines.append(
            f"• `#{o['id']}` {name[:10]} | 用户 `{o['user_id']}` | {short_err}"
        )
        builder.button(
            text=f"🔁 补发 #{o['id']}",
            callback_data=f"admin_retry_{o['id']}",
        )

    builder.adjust(1)
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回", callback_data="back_to_admin"))

    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )
    await callback.answer()


# ============================================================
# 后台定时任务
# ============================================================

async def auto_sync_task(db_pool):
    """每 10 分钟同步一次远端商品"""
    while True:
        await asyncio.sleep(600)
        try:
            config = await get_system_config(db_pool)
            shop_url = config.get("shop_url")
            app_id = config.get("app_id")
            app_key = config.get("app_key")
            if not all([shop_url, app_id, app_key]):
                continue
            api = YiCiyuanAPI(shop_url, app_id, app_key)
            result = await api.get_items()
            if result.get("code") == 0:
                categories = result.get("data", {}).get("list", [])
                count = await save_remote_products(db_pool, categories)
                logger.info("自动同步完成：%d 个商品", count)
        except Exception as e:
            logger.error("自动同步异常: %s", e)


async def order_expire_task(db_pool):
    """每 5 分钟清理 expires_at 已过的 pending 订单，释放 final_amount 坑位"""
    while True:
        await asyncio.sleep(300)
        try:
            await expire_stale_orders(db_pool)
        except Exception as e:
            logger.error("过期订单清理异常: %s", e)


async def tron_listener_task(db_pool):
    """启动 TronGrid 链上监听"""
    from tron_listener import TronListener
    listener = TronListener(
        wallet_address=USDT_WALLET,
        db_pool=db_pool,
        bot=bot,
        trongrid_api_key=TRONGRID_API_KEY,
        poll_interval=TRON_POLL_INTERVAL,
    )
    await listener.start()


# ============================================================
# 启动入口
# ============================================================

async def start_bot():
    db_pool = await get_db_pool()
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)

    dp["db_pool"] = db_pool
    dp["redis"] = redis_client

    await bot.delete_webhook(drop_pending_updates=True)

    # 启动后台任务
    asyncio.create_task(auto_sync_task(db_pool))
    asyncio.create_task(order_expire_task(db_pool))
    asyncio.create_task(tron_listener_task(db_pool))

    logger.info("🚀 Bot 已启动，链上监听已开启")

    try:
        await dp.start_polling(bot)
    finally:
        await db_pool.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 机器人已关闭")
