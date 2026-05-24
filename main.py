"""
main.py
Telegram 机器人主入口 — Phase 1 + Phase 2 完整版
"""

import asyncio
import contextlib
import logging
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Any, Awaitable, Callable

import aiomysql
import redis.asyncio as redis
from aiogram import BaseMiddleware, Bot, Dispatcher, types, F
from aiogram.exceptions import TelegramConflictError
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder

from amount_lock import release_amount_lock
from config import (
    BOT_TOKEN,
    ADMIN_ID,
    REDIS_URL,
    USDT_WALLET,
    TRONGRID_API_KEY,
    TRON_POLL_INTERVAL,
    ORDER_EXPIRE_MINUTES,
    validate_production_env,
)
from database import (
    get_db_pool,
    get_system_config,
    update_system_config,
    save_remote_products,
    get_active_categories,
    get_user_data,
    get_shop_stats,
    get_product_by_id,
    get_order_by_id,
    expire_stale_orders,
    EmptyStringError,
    require_non_empty_text,
)
from payments import place_payment_order, quantize_base
from yiciyuan_api import YiCiyuanAPI, YiCiyuanAPIError

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

INSTANCE_LOCK_KEY = "telegram_bot:polling_instance"
INSTANCE_LOCK_TTL = 60


class InjectMiddleware(BaseMiddleware):
    def __init__(self, db_pool, redis_client):
        self.db_pool = db_pool
        self.redis_client = redis_client

    async def __call__(
        self,
        handler: Callable[[Any, dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: dict[str, Any],
    ) -> Any:
        data["db_pool"] = self.db_pool
        data["redis_client"] = self.redis_client
        return await handler(event, data)


class AdminSetup(StatesGroup):
    waiting_for_url = State()
    waiting_for_id = State()
    waiting_for_key = State()
    waiting_for_service = State()
    waiting_for_new_name = State()
    waiting_for_new_cat_name = State()
    waiting_for_price = State()        # Phase 2: 设置价格


class UserRecharge(StatesGroup):
    waiting_for_amount = State()


class InstanceLock:
    def __init__(self, redis_client, key: str, ttl: int):
        self.redis = redis_client
        self.key = key
        self.ttl = ttl
        self.token = uuid.uuid4().hex
        self._renew_task: asyncio.Task | None = None

    async def __aenter__(self):
        ok = await self.redis.set(self.key, self.token, nx=True, ex=self.ttl)
        if not ok:
            raise RuntimeError(
                "检测到已有机器人实例在运行，请先停止旧进程，避免 TelegramConflictError。"
            )
        self._renew_task = asyncio.create_task(self._renew())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._renew_task:
            self._renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._renew_task
        await self.redis.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            1,
            self.key,
            self.token,
        )

    async def _renew(self):
        while True:
            await asyncio.sleep(max(1, self.ttl // 3))
            current = await self.redis.get(self.key)
            if current != self.token:
                raise RuntimeError("机器人单实例锁已丢失")
            await self.redis.expire(self.key, self.ttl)


def _message_text(message: types.Message, field_name: str) -> str:
    return require_non_empty_text(message.text, field_name)


async def _sync_remote_products(db_pool) -> int:
    config = await get_system_config(db_pool)
    shop_url = config.get("shop_url")
    app_id = config.get("app_id")
    app_key = config.get("app_key")
    if not all([shop_url, app_id, app_key]):
        raise EmptyStringError("商铺 API 配置不完整")

    api = YiCiyuanAPI(shop_url, app_id, app_key, bot=bot)
    result = await api.get_items()
    if result.get("code") not in (0, 200, "0", "200"):
        raise YiCiyuanAPIError("/shared/commodity/items", result.get("msg", "同步失败"))
    # 找到 _sync_remote_products 函数内部
    data_content = result.get("data", [])
    if isinstance(data_content, list):
        categories = data_content      # 这里必须缩进 4 个空格
    elif isinstance(data_content, dict):
        categories = data_content.get("list", []) # 这里必须缩进 4 个空格
    else:
        categories = []                # 这里必须缩进 4 个空格
    return await save_remote_products(db_pool, categories)


def _customer_service_url(customer_service: str) -> str:
    if customer_service.startswith("http"):
        return customer_service
    return f"https://t.me/{customer_service.lstrip('@')}"


def build_payment_keyboard(order_id: int, customer_service: str = "") -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if customer_service:
        builder.button(text="💬 联系客服", url=_customer_service_url(customer_service))
    builder.button(text="❌ 取消订单", callback_data=f"cancel_order_{order_id}")
    builder.adjust(1)
    return builder.as_markup()


def format_payment_message(
    *,
    title_line: str,
    detail_lines: str,
    order_id: int,
    final_amount: Decimal,
) -> str:
    amount_display = f"{final_amount:.6f}"
    return (
        f"{title_line}\n\n"
        f"{detail_lines}\n"
        f"📋 订单号：`#{order_id}`\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"💳 *转账信息*\n\n"
        f"  网络：`TRC20 (TRON)`\n"
        f"  币种：`USDT`\n\n"
        f"  收款地址：\n"
        f"  `{USDT_WALLET}`\n\n"
        f"  ⚠️ *必须转入精确金额*：\n"
        f"  # `{amount_display} USDT` #\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"转账后系统将 *自动核销*，无需手动通知。\n"
        f"⏰ 订单有效期 *{ORDER_EXPIRE_MINUTES} 分钟*，超时自动关闭。"
    )


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
    try:
        new_name = _message_text(message, "商品名称")
    except EmptyStringError:
        return await message.answer("⚠️ 商品名称不能为空，请重新输入。")
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
    try:
        new_cat_name = _message_text(message, "分类名称")
    except EmptyStringError:
        return await message.answer("⚠️ 分类名称不能为空，请重新输入。")
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
            await cur.execute(
                """
                INSERT INTO categories (category_name, is_active)
                VALUES (%s, 1)
                ON DUPLICATE KEY UPDATE category_name=VALUES(category_name)
                """,
                (new_cat_name,),
            )
            await cur.execute("DELETE FROM categories WHERE category_name=%s", (old_cat_name,))
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
    try:
        text = _message_text(message, "商品价格")
    except EmptyStringError:
        return await message.answer("⚠️ 价格不能为空，请输入有效的正数价格。")
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
            # 核心修改：从 categories 表直接读取，确保 ID 排序不乱跳
            # 同时关联 products 表统计数量
            await cur.execute("""
                SELECT 
                    c.id, 
                    c.category_name, 
                    c.is_active as cat_active,
                    COUNT(p.id) as total, 
                    SUM(CASE WHEN c.is_active = 1 AND p.is_active = 1 THEN 1 ELSE 0 END) as active_count
                FROM categories c
                LEFT JOIN products p ON c.category_name = p.category_name
                GROUP BY c.id, c.category_name, c.is_active
                ORDER BY c.id ASC
            """)
            cats = await cur.fetchall()

    builder = InlineKeyboardBuilder()
    for c in cats:
        cat_id = c["id"]
        cat_name = c["category_name"]
        
        # 状态显示：分类是否上架
        cat_status_emoji = "✅" if c.get("cat_active", 1) else "❌"
        # 统计显示：(已上架商品数/总商品数)
        stats = f"({c['active_count'] or 0}/{c['total']})"
        
        # 限制名称长度
        display_name = (cat_name[:10] + "..") if len(cat_name) > 10 else cat_name
        
        # 第一列：点击名称进入商品管理
        builder.button(text=f"📂 {display_name} {stats}", callback_data=f"ashelf_cat_{cat_name}")
        # 第二列：点击 ✅/❌ 切换分类显示状态
        builder.button(text=cat_status_emoji, callback_data=f"toggle_cat_status_{cat_id}")
        # 第三列：点击 📝 重命名
        builder.button(text="📝 编辑", callback_data=f"rencat_{cat_name}")

    # 关键：每行显示 3 个按钮
    builder.adjust(3) 
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回主菜单", callback_data="back_to_admin"))

    await callback.message.edit_text(
        "📂 **分类管理面板**\n\n1. 点击 **📂名称**：管理该分类下的商品\n2. 点击 **✅/❌**：一键上架/下架该分类\n3. 点击 **📝编辑**：修改分类名称",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown",
    )

@dp.callback_query(F.data.startswith("toggle_cat_status_"), F.from_user.id == ADMIN_ID)
async def cb_toggle_cat_status(callback: types.CallbackQuery, db_pool):
    cat_id = int(callback.data.split("_")[-1])
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            # 切换 is_active 状态 (0变1, 1变0)
            await cur.execute(
                "UPDATE categories SET is_active = 1 - IFNULL(is_active, 1) WHERE id = %s",
                (cat_id,)
            )
            await conn.commit()
    
    await callback.answer("分类状态已更新")
    # 刷新面板
    await admin_manage_cats(callback, db_pool)


@dp.callback_query(F.data.startswith("ashelf_cat_"), F.from_user.id == ADMIN_ID)
async def admin_on_shelf(callback: types.CallbackQuery, db_pool):
    cat_name = callback.data.replace("ashelf_cat_", "")
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
            SELECT 
                p.*, 
                c.is_active as cat_active 
                FROM products p
                JOIN categories c ON p.category_name = c.category_name
                WHERE p.category_name = %s
                ORDER BY p.id ASC
                """,
                (cat_name,))
            prods = await cur.fetchall()

    builder = InlineKeyboardBuilder()
    for p in prods:
        try:
            actual_name = require_non_empty_text(
                p["display_name"] if p["display_name"] else p["remote_name"],
                "product_name",
            )
        except EmptyStringError:
            continue
        short_name = (actual_name[:12] + "..") if len(actual_name) > 12 else actual_name
        # 逻辑：只有【分类开启】且【商品开启】时，才显示 ✅
# 如果分类关了(p["cat_active"] == 0)，无论商品状态如何，图标都显示为 ❌
        is_actually_on_shelf = (p["is_active"] == 1 and p.get("cat_active", 1) == 1)
        status_icon = "✅" if is_actually_on_shelf else "❌"
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
        try:
            actual_name = require_non_empty_text(
                p["display_name"] if p["display_name"] else p["remote_name"],
                "product_name",
            )
        except EmptyStringError:
            continue
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
    try:
        link = _message_text(message, "客服地址")
    except EmptyStringError:
        return await message.answer("⚠️ 客服地址不能为空，请重新输入。")
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


@dp.callback_query(F.data == "setup_shop", F.from_user.id == ADMIN_ID)
async def cb_setup_shop(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("🔗 请输入一次元商铺 API 地址：")
    await state.set_state(AdminSetup.waiting_for_url)
    await callback.answer()


@dp.message(AdminSetup.waiting_for_url, F.from_user.id == ADMIN_ID)
async def process_shop_url(message: types.Message, state: FSMContext):
    try:
        shop_url = _message_text(message, "商铺地址")
    except EmptyStringError:
        return await message.answer("⚠️ 商铺地址不能为空，请重新输入。")
    await state.update_data(shop_url=shop_url)
    await message.answer("🆔 请输入 app_id：")
    await state.set_state(AdminSetup.waiting_for_id)


@dp.message(AdminSetup.waiting_for_id, F.from_user.id == ADMIN_ID)
async def process_app_id(message: types.Message, state: FSMContext):
    try:
        app_id = _message_text(message, "app_id")
    except EmptyStringError:
        return await message.answer("⚠️ app_id 不能为空，请重新输入。")
    await state.update_data(app_id=app_id)
    await message.answer("🔑 请输入 app_key：")
    await state.set_state(AdminSetup.waiting_for_key)


@dp.message(AdminSetup.waiting_for_key, F.from_user.id == ADMIN_ID)
async def process_app_key(message: types.Message, state: FSMContext, db_pool):
    try:
        app_key = _message_text(message, "app_key")
        data = await state.get_data()
        await update_system_config(db_pool, data.get("shop_url"), data.get("app_id"), app_key)
    except EmptyStringError as e:
        return await message.answer(f"⚠️ 配置不能为空：{e}")

    await state.clear()
    await message.answer("✅ 商铺配置已保存，可在管理员面板执行同步。")


@dp.callback_query(F.data == "sync_now", F.from_user.id == ADMIN_ID)
async def cb_sync_now(callback: types.CallbackQuery, db_pool):
    await callback.answer("正在同步...", show_alert=False)
    try:
        count = await _sync_remote_products(db_pool)
    except (YiCiyuanAPIError, EmptyStringError) as e:
        return await callback.message.answer(f"❌ 同步失败：{e}")
    await callback.message.answer(f"✅ 同步完成：{count} 个商品。")


@dp.callback_query(F.data == "delete_shop", F.from_user.id == ADMIN_ID)
async def cb_delete_shop(callback: types.CallbackQuery, db_pool):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM system_config
                WHERE config_key IN ('shop_url', 'app_id', 'app_key')
                """
            )
    await callback.message.answer("✅ 商铺配置已删除，已同步商品数据不会自动删除。")
    await callback.answer()


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
    cats = await get_active_categories(db_pool)

    if not cats:
        return await message.answer("😔 暂无上架商品，请稍后再来。")

    builder = InlineKeyboardBuilder()
    for c in cats:
        try:
            category_name = require_non_empty_text(c["category_name"], "category_name")
        except EmptyStringError:
            continue
        builder.button(text=category_name, callback_data=f"cat_{category_name}")
    builder.adjust(1)
    await message.answer("请选择商品分类：", reply_markup=builder.as_markup())


@dp.message(Command("me"))
async def cmd_me(message: types.Message, db_pool):
    user = await get_user_data(db_pool, message.from_user.id)
    text = (
        f"👤 **个人中心**\n\n"
        f"🆔 ID: `{message.from_user.id}`\n"
        f"💰 余额: `{user['balance']:.6f}` USDT"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="💳 立即充值", callback_data="recharge")
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")


# ============================================================
# 用户端：余额充值
# ============================================================

@dp.callback_query(F.data == "recharge")
async def cb_recharge(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "💳 *余额充值*\n\n"
        "请输入充值金额（USDT，正数，最多两位小数）：\n"
        "输入 `cancel` 取消",
        parse_mode="Markdown",
    )
    await state.set_state(UserRecharge.waiting_for_amount)
    await callback.answer()


@dp.message(UserRecharge.waiting_for_amount)
async def process_recharge_amount(
    message: types.Message,
    state: FSMContext,
    db_pool,
    redis_client,
):
    try:
        text = _message_text(message, "充值金额")
    except EmptyStringError:
        return await message.answer("⚠️ 金额不能为空，请输入有效的正数金额。")
    if text.lower() == "cancel":
        await state.clear()
        return await message.answer("❌ 已取消充值。")

    try:
        base_amount = quantize_base(Decimal(text))
        if base_amount <= 0:
            raise ValueError
    except Exception:
        return await message.answer("⚠️ 请输入有效的正数金额，例如：100 或 50.5")

    if not USDT_WALLET:
        await state.clear()
        return await message.answer("⚠️ 系统尚未配置收款地址，请联系管理员。")

    user_tg_id = message.from_user.id
    result = await place_payment_order(
        db_pool,
        redis_client,
        user_tg_id,
        base_amount,
        order_type="recharge",
        product_id=None,
        expire_minutes=ORDER_EXPIRE_MINUTES,
    )
    if not result:
        return await message.answer("⚠️ 当前充值通道繁忙，请稍后重试。")

    order_id, final_amount = result
    config = await get_system_config(db_pool)
    customer_service = config.get("customer_service", "")

    pay_text = format_payment_message(
        title_line=f"💳 *充值订单已创建，请在 {ORDER_EXPIRE_MINUTES} 分钟内完成付款*",
        detail_lines=f"💰 充值面额：`{base_amount} USDT`",
        order_id=order_id,
        final_amount=final_amount,
    )
    await message.answer(
        pay_text,
        reply_markup=build_payment_keyboard(order_id, customer_service),
        parse_mode="Markdown",
    )
    await state.clear()


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
        try:
            name = require_non_empty_text(
                p["display_name"] if p["display_name"] else p["remote_name"],
                "product_name",
            )
        except EmptyStringError:
            continue
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
async def buy_product(callback: types.CallbackQuery, db_pool, redis_client):
    """用户购买商品：Redis 预锁 + DB 唯一金额 + 展示付款信息。"""
    if not USDT_WALLET:
        return await callback.answer("系统未配置收款地址", show_alert=True)

    p_id = int(callback.data.replace("buy_", ""))
    user_tg_id = callback.from_user.id

    product = await get_product_by_id(db_pool, p_id)
    if not product:
        return await callback.answer("❌ 商品不存在", show_alert=True)

    price = product.get("price")
    if not price:
        return await callback.answer("❌ 该商品尚未设置价格，请联系客服", show_alert=True)

    base_amount = quantize_base(Decimal(str(price)))
    result = await place_payment_order(
        db_pool,
        redis_client,
        user_tg_id,
        base_amount,
        order_type="purchase",
        product_id=p_id,
        expire_minutes=ORDER_EXPIRE_MINUTES,
    )
    if not result:
        return await callback.answer(
            "⚠️ 当前价格档位已满，请稍后重试或联系客服", show_alert=True
        )

    order_id, final_amount = result
    try:
        product_name = require_non_empty_text(
            product.get("display_name") or product.get("remote_name"),
            "product_name",
        )
    except EmptyStringError:
        return await callback.answer("❌ 商品名称异常，请联系管理员", show_alert=True)
    config = await get_system_config(db_pool)
    customer_service = config.get("customer_service", "")

    pay_text = format_payment_message(
        title_line=f"🛒 *下单成功，请在 {ORDER_EXPIRE_MINUTES} 分钟内完成付款*",
        detail_lines=f"📦 商品：{product_name}",
        order_id=order_id,
        final_amount=final_amount,
    )
    await callback.message.answer(
        pay_text,
        reply_markup=build_payment_keyboard(order_id, customer_service),
        parse_mode="Markdown",
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order(callback: types.CallbackQuery, db_pool, redis_client):
    """用户主动取消待支付订单，并释放 Redis 金额锁。"""
    order_id = int(callback.data.replace("cancel_order_", ""))
    user_tg_id = callback.from_user.id

    order = await get_order_by_id(db_pool, order_id)
    if not order or order["user_id"] != user_tg_id or order["status"] != "pending":
        return await callback.answer("订单已处理，无法取消", show_alert=True)

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE recharge_orders SET status='cancelled'
                WHERE id=%s AND user_id=%s AND status='pending'
                """,
                (order_id, user_tg_id),
            )
            updated = cur.rowcount

    if updated:
        await release_amount_lock(redis_client, Decimal(str(order["final_amount"])))
        await callback.message.edit_text(f"❌ 订单 `#{order_id}` 已取消。", parse_mode="Markdown")
    else:
        await callback.answer("订单已处理，无法取消", show_alert=True)


# ============================================================
# 后台定时任务
# ============================================================

async def auto_sync_task(db_pool):
    """每 10 分钟同步一次远端商品"""
    while True:
        await asyncio.sleep(600)
        try:
            count = await _sync_remote_products(db_pool)
            logger.info("自动同步完成：%d 个商品", count)
        except EmptyStringError:
            continue
        except YiCiyuanAPIError as e:
            logger.error("自动同步 API 失败: %s", e)
        except Exception as e:
            logger.error("自动同步异常: %s", e)


async def order_expire_task(db_pool, redis_client):
    """每 5 分钟清理过期 pending 订单，并释放 Redis 金额锁。"""
    while True:
        await asyncio.sleep(300)
        try:
            expired_amounts = await expire_stale_orders(db_pool)
            for amount in expired_amounts:
                await release_amount_lock(redis_client, amount)
        except Exception as e:
            logger.error("过期订单清理异常: %s", e)


async def tron_listener_task(db_pool, redis_client):
    """启动 TronGrid 链上监听"""
    from tron_listener import TronListener

    listener = TronListener(
        wallet_address=USDT_WALLET,
        db_pool=db_pool,
        bot=bot,
        redis_client=redis_client,
        trongrid_api_key=TRONGRID_API_KEY,
        poll_interval=TRON_POLL_INTERVAL,
    )
    await listener.start()


# ============================================================
# 启动入口
# ============================================================

async def start_bot():
    validate_production_env()

    db_pool = await get_db_pool()
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)

    dp["db_pool"] = db_pool
    dp["redis"] = redis_client
    dp.update.middleware(InjectMiddleware(db_pool, redis_client))

    await bot.delete_webhook(drop_pending_updates=True)

    try:
        async with InstanceLock(redis_client, INSTANCE_LOCK_KEY, INSTANCE_LOCK_TTL):
            asyncio.create_task(auto_sync_task(db_pool))
            asyncio.create_task(order_expire_task(db_pool, redis_client))
            asyncio.create_task(tron_listener_task(db_pool, redis_client))

            logger.info("🚀 Bot 已启动，链上监听已开启")
            await dp.start_polling(bot)
    except TelegramConflictError:
        logger.error("Telegram polling 冲突：已有实例正在 getUpdates，请停止旧进程后重启。")
        raise
    finally:
        await redis_client.aclose()
        db_pool.close()
        await db_pool.wait_closed()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 机器人已关闭")
