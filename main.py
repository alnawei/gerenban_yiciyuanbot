"""
main.py
Telegram 机器人主入口 — Phase 1 + Phase 2 完整版
已修复：管理商品状态更新时，与父分类 is_active 状态不一致、UI 回调混乱等问题。
"""

import asyncio
import contextlib
import logging
import time
import uuid
import html  # 【新增导入】：用于对动态名字执行 HTML 字符安全转义
from decimal import Decimal, ROUND_DOWN
from typing import Any, Awaitable, Callable
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
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
    """全局中间件：为每个 Handler 注入数据库连接池与 Redis 实例"""
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


# main.py — 更新后的状态机

class AdminSetup(StatesGroup):
    waiting_for_url = State()
    waiting_for_id = State()
    waiting_for_key = State()
    waiting_for_service = State()
    waiting_for_new_name = State()
    waiting_for_new_cat_name = State()
    waiting_for_price = State()
    waiting_for_wallet_address = State()  # 【新增】：等待输入 TRC-20 收款地址
    waiting_for_trongrid_key = State()    # 【新增】：等待输入 TronGrid Key


class UserRecharge(StatesGroup):
    waiting_for_amount = State()


class InstanceLock:
    """Redis 机器人单实例锁，防止多 Polling 进程冲突"""
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


# main.py — 重构后的 _sync_remote_products 函数

async def _sync_remote_products(db_pool) -> int:
    """拉取并同步一次元商铺的商品和分类数据，并在第一现场打印调试日志。"""
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

    # 【新增修改 1】：在请求接口拿到数据的第一时间，打印返回体字符原始长度
    raw_payload_len = len(str(result))
    logger.info("🔍 [Sync Debug] 一次元接口请求成功！返回原始 Payload 长度: %d 字符", raw_payload_len)

    data_content = result.get("data", [])
    if isinstance(data_content, list):
        categories = data_content      
    elif isinstance(data_content, dict):
        categories = data_content.get("list", []) 
    else:
        categories = []                
        
    # 【新增修改 2】：自动递归累加接口数据含有的商品总数，不漏掉任何未入库的商品
    total_api_items = 0
    for cat in categories:
        total_api_items += len(cat.get("children", []))
    
    logger.info(
        "🔍 [Sync Debug] 接口数据层解析完毕。含有分类数: %d 个，含有商品原始总数: %d 个", 
        len(categories), total_api_items
    )
    
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


# main.py 中的完整修改函数

def format_payment_message(
    *,
    title_line: str,
    detail_lines: str,
    order_id: int,
    final_amount: Decimal,
) -> str:
    """
    【优化修改】格式化支付展示消息。
    
    优化点：
      - 废弃原有的 final_amount:.6f 强制展示 6 位小数逻辑。
      - 智能检测金额的实际小数位深度 (exponent)：
        - 如果金额为整数或 1~2 位小数，格式化为标准财务 2 位小数（如 5.00, 5.15 USDT）。
        - 如果激活了三位防碰撞，格式化为精确的 3 位小数（如 5.123 USDT），绝不产生 5.123000。
    """
    # 获取金额的实际小数位数
    exponent = abs(final_amount.as_tuple().exponent)
    if exponent <= 2:
        amount_display = f"{final_amount:.2f}"
    else:
        amount_display = f"{final_amount:.3f}"

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

# main.py - 修改后的 admin_menu 函数

# main.py — 恢复为清爽的 admin_menu 函数

# main.py — 补全配置后的管理员主菜单

@dp.message(Command("admin"), F.from_user.id == ADMIN_ID)
async def admin_menu(message: types.Message, db_pool):
    """管理员菜单主页面：整合了客服设置、收款地址设置、API Key 设置"""
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
        
        # 【核心补全】：依次垂直新增以下两个重要的参数配置入口
        builder.button(text="💰 设置收款地址", callback_data="set_wallet_address")
        builder.button(text="🔑 设置 TronGrid Key", callback_data="set_trongrid_key")
        
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


# main.py — 优化商品编辑流，支持改名后直达改价的 process_new_name 函数

# main.py — 重构后的改名成功处理器（增加💰设置价格）

@dp.message(AdminSetup.waiting_for_new_name, F.from_user.id == ADMIN_ID)
async def process_new_name(message: types.Message, state: FSMContext, db_pool):
    """
    管理员：处理输入商品新名称。
    
    闭环设计点：
      - 改名成功后，主操作按钮设为“💰 设置价格”，绑定 setprice_{p_id}。
        点击即可直达价格修改流程，实现编辑链路闭环 [2]。
    """
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
    
    # 【闭环设计 1】：首要按钮设为设置售价，无缝衔接下一步改价流程
    kb.button(text="💰 设置价格", callback_data=f"setprice_{p_id}")
    # 辅助按钮：若名字打错，支持一键“重新修改名称”
    kb.button(text="🔄 重新修改", callback_data=f"rename_{p_id}")
    # 返回按钮：退回原商品列表
    if cat_name:
        kb.button(text="⬅️ 返回商品列表", callback_data=f"ashelf_cat_{cat_name}")
        
    kb.adjust(1)

    await message.answer(
        f"✅ 商品名已成功更新为：\n`{new_name}`", 
        reply_markup=kb.as_markup(), 
        parse_mode="Markdown"
    )
    await state.clear()


# ============================================================
# 管理员：分类改名
# ============================================================

# =====================================================================
# 请在 main.py 中找到旧的 admin_manage_cats、cb_toggle_cat_status 
# 和 cb_delcat_execute 这一整块代码，
# 并用以下这一大段（包含 _render_manage_cats 在内，共 4 个函数）全部覆盖替换：
# =====================================================================

# main.py — 缩短按钮文字、优化手机端排版的 _render_manage_cats 函数

async def _render_manage_cats(callback: types.CallbackQuery, db_pool, use_reply_markup_only: bool = False):
    """
    【分类渲染器 - 文本精简版】根据最新数据重新绘制分类管理面板的键盘布局。
    
    修改点：
      - 将第二行按钮文字进一步精简：“✏️ 改分类名” -> “✏️ 改名”，“🗑️ 删除分类” -> “🗑️ 删除”。
      - 缩短后的字符长度在 3 列并排时极其规整，在小屏幕手机上也有极佳的展现质感。
    """
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
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
        
        cat_active = c.get("cat_active", 1)
        # 获取受分类开关联动影响的实际在售统计数
        active_count = c.get("active_count") or 0
        
        # 1. 第一行（主信息）：[📂 分类名 (包含统计)] 独占一整行全宽，点击进入商品列表
        builder.row(types.InlineKeyboardButton(
            text=f"📂 {cat_name} ({active_count})", 
            callback_data=f"ashelf_cat_{cat_name}"
        ))
        
        # 分类状态 Emoji 标识
        cat_status_emoji = "✅ 开启中" if cat_active == 1 else "❌ 已关闭"
        
        # 2. 第二行（功能带）：并排三个等宽功能按钮（文字已精简，排版更加开阔美观）
        builder.row(
            types.InlineKeyboardButton(text=cat_status_emoji, callback_data=f"toggle_cat_status_{cat_id}"),
            types.InlineKeyboardButton(text="✏️ 改名", callback_data=f"rencat_{cat_name}"),   # 核心修改点：改名
            types.InlineKeyboardButton(text="🗑️ 删除", callback_data=f"delcat_confirm_{cat_id}")  # 核心修改点：删除
        )

    # 底部重置与主菜单按钮（各占一行）
    builder.row(types.InlineKeyboardButton(text="⚠️ 重置所有分类", callback_data="reset_all_categories"))
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回主菜单", callback_data="back_to_admin"))

    if use_reply_markup_only:
        # 仅仅更新键盘，速度极快，文字绝对不产生二次闪烁
        await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    else:
        # 常规完整渲染，覆盖文本与按键
        await callback.message.edit_text(
            "📂 **分类管理面板**\n\n"
            "• 点击 **📂分类名称**：进入管理该分类下的商品列表。\n"
            "• 点击 **✅/❌ 按钮**：实时切换分类状态（下架分类后，其商品统计会自动归零并自动在前台对普通用户隐退） [1]。\n"
            "• 点击 **✏️改名**：修改分类名字。\n"
            "• 点击 **🗑️删除**：物理级联删除分类及其商品（带防误触警告） [1]。",
            reply_markup=builder.as_markup(),
            parse_mode="Markdown",
        )


@dp.callback_query(F.data == "admin_manage_goods", F.from_user.id == ADMIN_ID)
async def admin_manage_cats(callback: types.CallbackQuery, db_pool):
    """分类管理面板主页面入口"""
    await _render_manage_cats(callback, db_pool, use_reply_markup_only=False)
    await callback.answer()


@dp.callback_query(F.data.startswith("toggle_cat_status_"), F.from_user.id == ADMIN_ID)
async def cb_toggle_cat_status(callback: types.CallbackQuery, db_pool):
    """
    管理员：一键实时切换分类状态。
    修改点：
      - 状态变更后，直接调用 `use_reply_markup_only=True` 物理刷新按键布局，
        前台分类图标和数据统计秒刷完毕，杜绝任何页面文字颤动闪烁，手感极其丝滑！
    """
    cat_id = int(callback.data.split("_")[-1])
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE categories SET is_active = 1 - IFNULL(is_active, 1) WHERE id = %s",
                (cat_id,)
            )
            await conn.commit()
            
    await callback.answer("分类状态已实时变更")
    # 无颤抖、无闪烁刷新最新键盘状态
    await _render_manage_cats(callback, db_pool, use_reply_markup_only=True)


@dp.callback_query(F.data.startswith("delcat_execute_"), F.from_user.id == ADMIN_ID)
async def cb_delcat_execute(callback: types.CallbackQuery, db_pool):
    cat_id = int(callback.data.replace("delcat_execute_", ""))
    
    await callback.answer("正在安全执行物理级联删除...", show_alert=False)
    try:
        async with db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT category_name FROM categories WHERE id = %s", (cat_id,))
                cat = await cur.fetchone()
                if not cat:
                    return await callback.answer("该分类已被先一步删除！", show_alert=True)
                cat_name = cat["category_name"]

        async with db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                await cur.execute("START TRANSACTION")  # ⭐️ 显式开启事务
                try:
                    await cur.execute("DELETE FROM products WHERE category_name = %s", (cat_name,))
                    await cur.execute("DELETE FROM categories WHERE id = %s", (cat_id,))
                    await cur.execute("COMMIT")  # ⭐️ 成功提交
                except Exception as db_err:
                    await cur.execute("ROLLBACK")  # ⭐️ 失败回滚
                    raise db_err
                finally:
                    await cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        await callback.answer(f"✅ 分类【{cat_name}】及其商品已被成功物理清除！", show_alert=True)
        await _render_manage_cats(callback, db_pool, use_reply_markup_only=False)
        
    except Exception as e:
        logger.exception("执行分类级联物理删除事务失败")
        await callback.message.answer(f"❌ 物理删除分类失败：{e}")

@dp.callback_query(F.data.startswith("rencat_"), F.from_user.id == ADMIN_ID)
async def start_rename_cat(callback: types.CallbackQuery, state: FSMContext):
    old_name = callback.data.replace("rencat_", "")
    await state.update_data(old_cat_name=old_name)
    await callback.message.answer(f"⌨️ 请输入【分类：{old_name}】的新名称：\n(输入 'cancel' 取消)")
    await state.set_state(AdminSetup.waiting_for_new_cat_name)
    await callback.answer()


# main.py — 彻底修复改名跑位 Bug 的 process_new_cat_name 函数

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
            await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            await cur.execute("START TRANSACTION")  # ⭐️ 显式开启事务
            try:
                # 1. 同步更新商品表 (products) 中的分类名称冗余关联
                await cur.execute(
                    "UPDATE products SET category_name=%s WHERE category_name=%s",
                    (new_cat_name, old_cat_name),
                )
                # 2. 直接 UPDATE 分类表中的名称
                await cur.execute(
                    "UPDATE categories SET category_name=%s WHERE category_name=%s",
                    (new_cat_name, old_cat_name),
                )
                await cur.execute("COMMIT")  # ⭐️ 成功后统一提交
            except Exception as e:
                await cur.execute("ROLLBACK")  # ⭐️ 失败后统一回滚
                logger.error("重命名分类失败，事务已回滚: %s", e)
                return await message.answer(f"❌ 修改分类名失败：{e}")
            finally:
                await cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ 返回分类列表", callback_data="admin_manage_goods")

    await message.answer(
        f"✅ 分类名已成功修改：\n`{old_cat_name}` ➡️ `{new_cat_name}`",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown",
    )
    await state.clear()


# ============================================================
# 管理员：设置商品价格
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


# main.py — 重构后的改价成功处理器（增加✏️修改名称）

@dp.message(AdminSetup.waiting_for_price, F.from_user.id == ADMIN_ID)
async def process_set_price(message: types.Message, state: FSMContext, db_pool):
    """
    管理员：处理输入的商品新售价。
    
    闭环设计点：
      - 改价成功后，主操作按钮设为“✏️ 修改名称”，绑定 rename_{p_id}。
        点击即可直达改名流程，完成“价格修改 ➡️ 商品改名”的双向流动闭环。
    """
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
    
    # 【闭环设计 2】：首要按钮设为修改名称，形成完整的反向流动闭环
    kb.button(text="✏️ 修改名称", callback_data=f"rename_{p_id}")
    # 辅助按钮：若价格输错，支持一键“再次修改价格”
    kb.button(text="💰 再次修改", callback_data=f"setprice_{p_id}")
    # 返回按钮：退回原商品列表
    if cat_name:
        kb.button(text="⬅️ 返回商品列表", callback_data=f"ashelf_cat_{cat_name}")
        
    kb.adjust(1)

    await message.answer(
        f"✅ 售价已成功更新为：`{price} USDT`",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown",
    )
    await state.clear()


# ============================================================
# 管理员：分类 & 商品管理面板
# ============================================================

# main.py — 修改后的 admin_manage_cats 函数

# main.py — 包含重置按钮与固定排序的展示函数

# main.py — 重构后的分类管理面板

# main.py — 彻底解决排版错位的分类管理面板函数




@dp.callback_query(F.data.startswith("delcat_confirm_"), F.from_user.id == ADMIN_ID)
async def cb_delcat_confirm(callback: types.CallbackQuery, db_pool):
    """
    【全新添加】显示物理删除分类的二次确认警示页。
    防止误触删除直接导致下属商品和价格被清除。
    """
    cat_id = int(callback.data.replace("delcat_confirm_", ""))
    
    # 物理安全查询需要被删除的分类原名
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT category_name FROM categories WHERE id = %s", (cat_id,))
            cat = await cur.fetchone()
            if not cat:
                return await callback.answer("该分类已被删除或不存在！", show_alert=True)
            cat_name = cat["category_name"]
            
    builder = InlineKeyboardBuilder()
    # 红色高亮危险选项：物理删除
    builder.button(text="🔥 确定删除（连带下属商品）", callback_data=f"delcat_execute_{cat_id}")
    # 安全退回
    builder.button(text="❌ 我再想想（取消）", callback_data="admin_manage_goods")
    builder.adjust(1)
    
    # 转义分类名，防范 Telegram 节点实体崩溃（请确保 main.py 顶部已 import html）
    safe_cat_name = html.escape(cat_name)
    
    await callback.message.edit_text(
        f"🚨 **物理删除分类确认** 🚨\n\n"
        f"您确定要彻底物理删除分类【<b>{safe_cat_name}</b>】吗？\n\n"
        f"❗ <b>高危风险警告</b>：\n"
        f"1. 该分类记录将从数据库中永久注销。\n"
        f"2. <b>该分类下属的所有商品（products）也将被同步在数据库中物理抹除</b>，无法在前端以及后台继续展示或购买，且不可恢复 [1]！\n\n"
        f"您是否确定要执行此项删除操作？",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()



# main.py — 重构后的商品列表页面（一行一个商品）

# main.py — 物理防崩溃重构版（请完全替换原有函数）

@dp.callback_query(F.data.startswith("ashelf_cat_"), F.from_user.id == ADMIN_ID)
async def admin_on_shelf(callback: types.CallbackQuery, db_pool):
    """
    展示某个分类下的全部商品。
    重构点：使用 HTML 与 html.escape 防御分类名特殊字符带来的页面崩溃。
    """
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
        
        is_actually_on_shelf = (p["is_active"] == 1 and p.get("cat_active", 1) == 1)
        status_icon = "✅" if is_actually_on_shelf else "❌"
        price_str = f"{p['price']} USDT" if p.get("price") else "面议"
        
        builder.row(types.InlineKeyboardButton(
            text=f"[{status_icon}] {actual_name} — {price_str}",
            callback_data=f"edit_opt_{p['id']}"
        ))

    builder.row(types.InlineKeyboardButton(text="⬅️ 返回分类列表", callback_data="admin_manage_goods"))

    # 安全转义分类名称
    safe_cat_name = html.escape(cat_name)
    timestamp = time.strftime("%H:%M:%S")
    await callback.message.edit_text(
        f"🛠 <b>管理分类商品</b>：{safe_cat_name}\n\n"
        f"提示：点击任何一个商品行，即可进入其专属的 <b>[修改名字 / 设置价格 / 下架停用]</b> 专属中控台面板。\n"
        f"<i>(同步刷新时间: {timestamp})</i>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML",
    )


# main.py — 追加以下两个 Callback 处理器

# main.py — 重构后的重置与多表清空全套 Callback 处理器

@dp.callback_query(F.data == "reset_all_categories", F.from_user.id == ADMIN_ID)
async def cb_reset_all_categories_confirm(callback: types.CallbackQuery):
    """
    【二次确认页 - 重构版】管理员点击“重置所有分类”后，展示高危物理清空风险警告。
    """
    builder = InlineKeyboardBuilder()
    # 红色警告：确认完全重置
    builder.button(text="⚠️ 确认完全清空并重置", callback_data="confirm_reset_all_categories")
    # 安全取消
    builder.button(text="❌ 取消并返回分类管理", callback_data="admin_manage_goods")
    builder.adjust(1)

    await callback.message.edit_text(
        "🚨 **高危操作二次确认** 🚨\n\n"
        "您正在请求对商品目录进行最彻底的物理清除，该操作将执行以下底层动作：\n\n"
        "• 🧹 **清空全部分类**：物理清空 `categories` 表，丢弃您手动改过的所有分类名称。\n"
        "• 🧹 **清空全部商品**：物理清空 `products` 表，**彻底擦除包括您手动配置过的价格、Display Name 和同步状态**。\n\n"
        "❗ **高危警告：此操作将清空所有分类、商品名称及手动设置的价格，且不可恢复！**\n\n"
        "重置后，后台主面板的统计数字将自动归零。您必须手动执行 **‘立即同步’** 才能重新从一次元拉取原始上架数据。\n\n"
        "您是否确定要继续进行此项操作？",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data == "confirm_reset_all_categories", F.from_user.id == ADMIN_ID)
async def cb_confirm_reset_all_categories_execution(callback: types.CallbackQuery, db_pool):
    """
    【物理完全清空数据 - 重构版】清空 categories 和 products 表，提示并自动返回归零的主菜单。
    """
    # 1. 立即应答，提示正在处理中
    await callback.answer("正在安全执行多表物理擦除，请稍候...", show_alert=False)
    
    try:
        # 2. 【核心修改】：开启多表 Truncate 物理清空事务
        async with db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                # 2.1 临时关闭外键约束，保障 TRUNCATE 顺利绕开外键检查执行
                await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                try:
                    # 2.2 清空 categories 分类表并重置自增主键 (TRUNCATE)
                    await cur.execute("TRUNCATE TABLE categories")
                    # 2.3 清空 products 商品表并重置自增主键 (TRUNCATE)
                    await cur.execute("TRUNCATE TABLE products")
                    await conn.commit()
                finally:
                    # 2.4 必须恢复外键安全检查
                    await cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        # 3. 弹窗大提示（show_alert=True），强力提醒管理员重新同步
        await callback.answer(
            "✅ 数据已完全清空！\n\n请重新执行“立即同步”以获取原始上架数据。",
            show_alert=True
        )
        
        # 4. 【核心修改】：自动重新加载并跳转回管理员主菜单
        # 因为 categories 和 products 已完全清空，主菜单的 `total_count` 和 `active_count` 
        # 将会通过 get_shop_stats 物理重读，自动并实时刷新为 0。
        await admin_menu(callback.message, db_pool)
        
    except Exception as e:
        logger.exception("物理完全清空数据发生未知系统异常")
        await callback.message.answer(f"❌ 物理完全重置数据失败：{e}")


async def _render_edit_options(callback: types.CallbackQuery, db_pool, p_id: int):
    """
    【独立渲染器 - 物理防解析崩溃版】
    重构点：
      - 彻底废弃敏感的 Markdown，改用坚固的 HTML 模式。
      - 引入 html.escape 对商品名和分类名进行转义，防止商品名中含 [ ]、_、*、< > 引起后台死锁。
    """
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT p.*, c.is_active as cat_active 
                FROM products p
                JOIN categories c ON p.category_name = c.category_name
                WHERE p.id = %s
            """, (p_id,))
            p_full = await cur.fetchone()
            
    if not p_full:
        return await callback.answer("商品加载失败，可能已被物理重置。")

    actual_name = p_full["display_name"] if p_full["display_name"] else p_full["remote_name"]
    is_active = p_full["is_active"]
    cat_active = p_full.get("cat_active", 1)
    cat_name = p_full["category_name"]
    
    # 核心安全：对第三方输入的名字进行物理 HTML 转义，防止不合法 HTML 字符解析失败
    safe_actual_name = html.escape(actual_name)
    safe_cat_name = html.escape(cat_name)
    
    is_actually_on_shelf = (is_active == 1 and cat_active == 1)
    status_emoji = "✅ 销售中" if is_actually_on_shelf else "❌ 未出售 (或分类已关闭)"
    price_display = f"{p_full['price']} USDT" if p_full.get("price") else "面议"

    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="💰 设置价格", callback_data=f"setprice_{p_id}"),
        types.InlineKeyboardButton(text="✏️ 修改名称", callback_data=f"rename_{p_id}")
    )
    toggle_text = "❌ 停用该商品" if is_active == 1 else "✅ 启用该商品"
    builder.row(types.InlineKeyboardButton(text=toggle_text, callback_data=f"t_{p_id}"))
    builder.row(types.InlineKeyboardButton(text="⬅️ 返回商品列表", callback_data=f"ashelf_cat_{cat_name}"))

    # 转义后使用 HTML 安全发送
    await callback.message.edit_text(
        f"⚙️ <b>商品中控面板</b>\n\n"
        f"📦 商品：<code>{safe_actual_name}</code>\n"
        f"📂 分类：<code>{safe_cat_name}</code>\n"
        f"💰 价格：<code>{price_display}</code>\n"
        f"📊 状态：{status_emoji}\n\n"
        f"<i>(提示：只有分类状态正常，且此商品开启启用，普通用户浏览时方可进行选购。)</i>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )

# main.py — 重构后的商品专属中控面板与控制开关

@dp.callback_query(F.data.startswith("edit_opt_"), F.from_user.id == ADMIN_ID)
async def show_edit_options(callback: types.CallbackQuery, db_pool):
    """
    呈现商品专属控制台子菜单。
    """
    p_id = int(callback.data.replace("edit_opt_", ""))
    # 调用物理渲染器
    await _render_edit_options(callback, db_pool, p_id)
    await callback.answer()


@dp.callback_query(F.data.startswith("t_"), F.from_user.id == ADMIN_ID)
async def toggle_product(callback: types.CallbackQuery, db_pool):
    """
    【修复后逻辑】：管理员在控制台内切换商品的上下架状态。
    
    修改点：
      - 彻底重构事件。完成数据库状态更新后，不再假装 callback.data。
      - 直接将 callback 对象传入 _render_edit_options 物理渲染器，
        做到状态切换后 0 毫秒即时刷新文本与按键，实现完美的一体化无缝操作反馈！
    """
    p_id = int(callback.data.replace("t_", ""))

    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT is_active FROM products WHERE id=%s", (p_id,))
            product = await cur.fetchone()
            if not product:
                return await callback.answer("未找到此商品")

            # 翻转状态
            new_status = 0 if product["is_active"] == 1 else 1
            await cur.execute("UPDATE products SET is_active=%s WHERE id=%s", (new_status, p_id))
            await conn.commit()

    # 先通过渲染器物理重刷消息内容（修改文字和按键），实现前台即刻视觉刷新
    await _render_edit_options(callback, db_pool, p_id)
    # 最后进行轻量化气泡通报
    await callback.answer(f"商品状态已成功变更为: {'启用' if new_status else '停用'}")

# main.py - 追加以下 Callback 处理器

# ============================================================
# 管理员：客服设置
# ============================================================

# main.py — 重构与追加后的客服管理全套处理器

@dp.callback_query(F.data == "set_customer_service", F.from_user.id == ADMIN_ID)
async def cb_show_service_menu(callback: types.CallbackQuery, db_pool):
    """
    【重构优化点 1】显示客服设置主界面（状态透明化）
    点击后不直接进入输入模式，而是先展示当前客服信息并提供动作按钮。
    """
    # 动态获取系统当前配置
    from database import get_system_config
    config = await get_system_config(db_pool)
    current_service = config.get("customer_service", "").strip()

    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 更换客服", callback_data="change_customer_service")
    # 只有当已经配置了客服时，才显示“清除客服”按钮，避免页面拥挤
    if current_service:
        builder.button(text="❌ 清除客服", callback_data="clear_customer_service")
    builder.button(text="⬅️ 返回主菜单", callback_data="back_to_admin")
    builder.adjust(1)

    status_text = f"`{current_service}`" if current_service else "⚠️ *未设置*"
    
    await callback.message.edit_text(
        f"👤 **客服设置导航**\n\n"
        f"当前配置状态：{status_text}\n\n"
        f"• **更换客服**：用于修改当前配置的电报用户名（如 `@yourname`）或第三方链接 [1]。\n"
        f"• **清除客服**：清除配置后，用户端相关界面和支付核销失败时会自动隐藏该联系通道 [1]。",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data == "change_customer_service", F.from_user.id == ADMIN_ID)
async def cb_change_service_prompt(callback: types.CallbackQuery, state: FSMContext):
    """
    【追加处理器 2】开启更换客服流程。
    """
    await callback.message.answer(
        "📝 **请输入新的客服地址**\n\n"
        "支持以下格式：\n"
        "1. 电报用户名：`@yourname` 或直接输入 `yourname` [1]\n"
        "2. 网页/群组链接：`https://t.me/yourname` 或第三方客服链接 [1]\n\n"
        "提示：输入 `cancel` 可取消当前操作并退回。",
        parse_mode="Markdown"
    )
    # 将对话状态切入等待客服输入的 FSM 状态机
    await state.set_state(AdminSetup.waiting_for_service)
    await callback.answer()


@dp.callback_query(F.data == "clear_customer_service", F.from_user.id == ADMIN_ID)
async def cb_clear_service_execution(callback: types.CallbackQuery, db_pool):
    """
    【追加处理器 3】物理清除客服配置（数据库配置更新为空）。
    """
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            # 清空 customer_service 对应的键值
            await cur.execute(
                """
                INSERT INTO system_config (config_key, config_value)
                VALUES ('customer_service', '')
                ON DUPLICATE KEY UPDATE config_value = ''
                """
            )
            await conn.commit()

    # 弹窗提示，增强操作确认体感
    await callback.answer("✅ 客服地址已成功清退！", show_alert=True)
    # 无感刷新当前的客服管理导航面板
    await cb_show_service_menu(callback, db_pool)


@dp.message(AdminSetup.waiting_for_service, F.from_user.id == ADMIN_ID)
async def process_service_link(message: types.Message, state: FSMContext, db_pool):
    """
    【重构优化点 4】接收用户输入的客服地址。
    增加 cancel 命令拦截，保障操作安全性。
    """
    try:
        link = _message_text(message, "客服地址")
    except EmptyStringError:
        return await message.answer("⚠️ 客服地址不能为空，请重新输入。")
        
    # 提供退出出口，防止卡死在状态机中
    if link.lower() == "cancel":
        await state.clear()
        return await message.answer("❌ 已取消更换客服。")

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
            INSERT INTO system_config (config_key, config_value)
            VALUES ('customer_service', %s)
            ON DUPLICATE KEY UPDATE config_value=%s
            """
            await cur.execute(sql, (link, link))
            await conn.commit()

    await message.answer(f"✅ 客服地址已成功变更为: `{link}`", parse_mode="Markdown")
    # 清理当前对话挂起的 FSM 状态
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
    """
    管理员：点击“立即同步”后的处理器。
    
    安全升级：
      - 彻底弃用脆弱的 Markdown 编译器，改用兼容性极佳的 HTML 解析模式。
      - 完美解决由于 [接口处理商品] 这种中括号语法导致 Telegram 强制寻找超链接闭合而崩溃的 Bug。
    """
    await callback.answer("正在同步中，请稍候...", show_alert=False)
    try:
        # 1. 执行同步
        processed_count = await _sync_remote_products(db_pool)
        
        # 2. 物理重新统计（SELECT COUNT(*)）
        total_count, active_count = await get_shop_stats(db_pool)
        
    except (YiCiyuanAPIError, EmptyStringError) as e:
        logger.warning("同步业务错误: %s", e)
        # HTML 模式下，普通文本中的报错直接输出即可
        return await callback.message.answer(f"❌ 同步失败（业务错误）：\n{e}")
    except Exception as e:
        logger.exception("同步流程发生未知异常")
        # 移除 parse_mode="Markdown" 和包裹的反引号。最朴素的纯文本才是承载未知系统异常最安全的形式
        return await callback.message.answer(f"❌ 同步发生未知异常，请查看后台日志：\n{e}")
        
    # 【终极修改】：改用 <b> 粗体与 <code> 宽体标签，并以 HTML 模式发送，彻底杜绝转义字符崩溃
    await callback.message.answer(
        f"✅ <b>远端商品同步执行成功！</b>\n\n"
        f"• 接口处理商品：<code>{processed_count}</code> 个\n"
        f"• 数据库总商品数 (重新查询)：<code>{total_count}</code> 个\n"
        f"• 当前实际上架数：<code>{active_count}</code> 个\n\n"
        f"💡 <b>排查提示</b>：\n"
        f"如果 [接口处理商品] 数量与 [数据库总商品数] 冲突，"
        f"说明有商品的 <code>remote_code</code> 编码发生了冲突，已被系统执行合并更新。"
        f"详细报告可以查看日志中带 <code>[Sync Debug]</code> 的行。",
        parse_mode="HTML"
    )


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

# main.py — 重构后的 /start 指令物理分流及穿透回调全套代码

# ── 1. 重构后的 /start 核心消息处理器 ──────────────────────────

# main.py — 重构后的 ReplyKeyboard /start 处理器

@dp.message(Command("start"))
async def cmd_start(message: types.Message, db_pool):
    """
    重构后的 /start 指令。
    实现：使用底部常驻自定义键盘（ReplyKeyboard）对管理员和用户进行物理隔离与布局。
    """
    user_id = message.from_user.id
    
    # 判定当前唤醒者是否为管理员
    is_admin = (user_id == ADMIN_ID)
    
    if is_admin:
        # ──── 1. 管理员端专属自定义键盘 ────
        text = (
            "🤖 **TeleRetail - 管理员专区**\n\n"
            "您好，管理员！欢迎回到控制台。\n\n"
            "下方常驻自定义键盘已为您激活，您可随时通过点击底部按钮快捷跳转相关页面。"
        )
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(text="🛍️ 浏览商品"),
                    KeyboardButton(text="👤 个人中心"),
                ],
                [
                    KeyboardButton(text="⚙️ 管理页面"),  # 独占一整行，突出地位
                ]
            ],
            resize_keyboard=True,  # 强制自适应高度，防止按钮过大影响体验
        )
    else:
        # ──── 2. 普通用户端自定义键盘 ────
        text = (
            "🤖 **欢迎光临发卡零售商店**\n\n"
            "您好！系统已为您自动绑定底部常驻自定义菜单。\n"
            "您可以随时通过点击底部键盘购买商品、查看您的当前账户余额，或者联系在线客服支持。"
        )
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(text="🛍️ 购买商品"),
                    KeyboardButton(text="👤 个人中心"),
                ],
                [
                    KeyboardButton(text="☎️ 联系客服"),  # 独占一整行，方便寻找
                ]
            ],
            resize_keyboard=True,  # 强制自适应高度
        )
        
    await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")


# ── 2. 新增的穿透指令 Inline Callback 响应处理器 ─────────────────

@dp.callback_query(F.data == "cmd_shop")
async def cb_cmd_shop_redirect(callback: types.CallbackQuery, db_pool):
    """【指令穿透】点击“浏览/购买商品”后，自动在当前窗口呼出分类面板"""
    await show_categories(callback.message, db_pool)
    await callback.answer()


@dp.callback_query(F.data == "cmd_me")
async def cb_cmd_me_redirect(callback: types.CallbackQuery, db_pool):
    """【指令穿透】点击“个人中心”后，自动在当前窗口呈现余额与充值卡片"""
    user = await get_user_data(db_pool, callback.from_user.id)
    # 财务标准：严格格式化余额为两位小数
    balance = Decimal(str(user["balance"]))
    
    text = (
        f"👤 **个人中心**\n\n"
        f"🆔 ID: `{callback.from_user.id}`\n"
        f"💰 余额: `{balance:.2f}` USDT"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="💳 立即充值", callback_data="recharge")
    
    # 采用 edit_text，平滑无闪烁地将 /start 消息重写为个人中心
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "cmd_admin", F.from_user.id == ADMIN_ID)
async def cb_cmd_admin_redirect(callback: types.CallbackQuery, db_pool):
    """【指令穿透】管理员点击“管理页面”后，直接进入后台主面板"""
    # 模拟发送 /admin
    await admin_menu(callback.message, db_pool)
    await callback.answer()


@dp.message(Command("shop"))
async def cmd_shop(message: types.Message, db_pool):
    await show_categories(message, db_pool)


# main.py 中的修改目标函数

async def show_categories(message: types.Message, db_pool):
    """
    普通用户端：展示可供购买的商品分类菜单。
    修改点：
      - 当没有可用分类（或分类全部下架）时，返回“商铺装修中，请稍后再来”的友好提示。
    """
    # 此时 get_active_categories 仅能获取到 is_active = 1 的分类
    cats = await get_active_categories(db_pool)

    # 核心修改：如果上架分类列表为空，向用户发送装修中提示
    if not cats:
        return await message.answer("🚧 *商铺装修中，请稍后再来。*", parse_mode="Markdown")

    builder = InlineKeyboardBuilder()
    for c in cats:
        try:
            category_name = require_non_empty_text(c["category_name"], "category_name")
        except EmptyStringError:
            continue
        builder.button(text=category_name, callback_data=f"cat_{category_name}")
    builder.adjust(1)
    await message.answer("请选择商品分类：", reply_markup=builder.as_markup())


# main.py — 余额保留 2 位小数的个人中心

@dp.message(Command("me"))
async def cmd_me(message: types.Message, db_pool):
    """个人中心：余额严格采用标准财务两位小数展示（0.00 USDT）"""
    user = await get_user_data(db_pool, message.from_user.id)
    balance = Decimal(str(user["balance"]))
    
    text = (
        f"👤 **个人中心**\n\n"
        f"🆔 ID: `{message.from_user.id}`\n"
        f"💰 余额: `{balance:.2f}` USDT"  # 修改点：格式化为 2 位小数
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
# 用户端：下单
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

# main.py — 追加以下关于 TRC-20 收款地址与 API Key 维护的整套处理器

def mask_api_key(key: str) -> str:
    """【脱敏处理器】对敏感的 API Key 进行安全展示脱敏（保留头 3 尾 3 字符）"""
    if not key:
        return "未配置"
    if len(key) <= 6:
        return "***"
    return f"{key[:3]}...{key[-3:]}"


# ── 收款地址 (Wallet Address) 逻辑组 ──────────────────────────

@dp.callback_query(F.data == "set_wallet_address", F.from_user.id == ADMIN_ID)
async def cb_show_wallet_menu(callback: types.CallbackQuery, db_pool):
    """TRC-20 收款配置主菜单。优先从 DB 读取，若无则降级采用 .env 初始配置 [1]"""
    config = await get_system_config(db_pool)
    current_wallet = config.get("usdt_wallet", "").strip() or USDT_WALLET
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 更换收款地址", callback_data="change_wallet_address")
    builder.button(text="⬅️ 返回主菜单", callback_data="back_to_admin")
    builder.adjust(1)
    
    status_text = f"`{current_wallet}`" if current_wallet else "⚠️ *未配置*"
    await callback.message.edit_text(
        f"💰 **TRC-20 收款地址设置**\n\n"
        f"当前监听地址：{status_text}\n\n"
        f"• 更换地址后，链上监听器会 **0秒无感知无缝切入新钱包进行监听**，无需重启服务 [1]。",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data == "change_wallet_address", F.from_user.id == ADMIN_ID)
async def cb_change_wallet_prompt(callback: types.CallbackQuery, state: FSMContext):
    """唤醒 FSM 状态机，等待管理员录入新收款地址"""
    await callback.message.answer(
        "📝 **请输入新的 TRC-20 收款钱包地址**：\n"
        "(必须是以大写 T 开头的有效波场地址，输入 `cancel` 退出配置)",
        parse_mode="Markdown"
    )
    await state.set_state(AdminSetup.waiting_for_wallet_address)
    await callback.answer()


@dp.message(AdminSetup.waiting_for_wallet_address, F.from_user.id == ADMIN_ID)
async def process_wallet_address(message: types.Message, state: FSMContext, db_pool):
    """校验新钱包地址合规性并原子持久化写入 system_config 表"""
    try:
        wallet = _message_text(message, "收款地址").strip()
    except EmptyStringError:
        return await message.answer("⚠️ 收款地址不能为空，请重新输入。")
        
    if wallet.lower() == "cancel":
        await state.clear()
        return await message.answer("❌ 已取消更换收款地址。")
    
    if not wallet.startswith("T") or len(wallet) < 30:
        return await message.answer("⚠️ 收款地址格式不合法（TRC-20 地址需大写 T 开头且长度充足），请重新输入。")
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO system_config (config_key, config_value)
                VALUES ('usdt_wallet', %s)
                ON DUPLICATE KEY UPDATE config_value = %s
                """,
                (wallet, wallet)
            )
            await conn.commit()
            
    await message.answer(f"✅ TRC-20 收款地址已成功变更为:\n`{wallet}`", parse_mode="Markdown")
    await state.clear()


# ── TronGrid API Key 逻辑组 ───────────────────────────────────

@dp.callback_query(F.data == "set_trongrid_key", F.from_user.id == ADMIN_ID)
async def cb_show_trongrid_menu(callback: types.CallbackQuery, db_pool):
    """TronGrid API Key 配置主菜单（展示时自动脱敏）"""
    config = await get_system_config(db_pool)
    current_key = config.get("trongrid_api_key", "").strip() or TRONGRID_API_KEY
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 更换 TronGrid Key", callback_data="change_trongrid_key")
    builder.button(text="⬅️ 返回主菜单", callback_data="back_to_admin")
    builder.adjust(1)
    
    status_text = mask_api_key(current_key)
    await callback.message.edit_text(
        f"🔑 **TronGrid API Key 设置**\n\n"
        f"当前配置 Key：`{status_text}`\n\n"
        f"• 设置 API Key 可以提高接口访问限速阈值，有效杜绝由于接口高频轮询引起的高延时或限频阻断。",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data == "change_trongrid_key", F.from_user.id == ADMIN_ID)
async def cb_change_trongrid_prompt(callback: types.CallbackQuery, state: FSMContext):
    """唤醒 FSM 状态机，等待管理员录入新 API Key"""
    await callback.message.answer(
        "📝 **请输入新的 TronGrid API Key**：\n"
        "(输入 `cancel` 退出配置)",
        parse_mode="Markdown"
    )
    await state.set_state(AdminSetup.waiting_for_trongrid_key)
    await callback.answer()


@dp.message(AdminSetup.waiting_for_trongrid_key, F.from_user.id == ADMIN_ID)
async def process_trongrid_key(message: types.Message, state: FSMContext, db_pool):
    """安全保存 TronGrid API Key 到 system_config 表"""
    try:
        key = _message_text(message, "TronGrid Key").strip()
    except EmptyStringError:
        return await message.answer("⚠️ Key 不能为空，请重新输入。")
        
    if key.lower() == "cancel":
        await state.clear()
        return await message.answer("❌ 已取消更换 Key。")
    
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO system_config (config_key, config_value)
                VALUES ('trongrid_api_key', %s)
                ON DUPLICATE KEY UPDATE config_value = %s
                """,
                (key, key)
            )
            await conn.commit()
            
    await message.answer(f"✅ TronGrid API Key 已成功变更为:\n`{mask_api_key(key)}`", parse_mode="Markdown")
    await state.clear()

# main.py — 追加在最底部的文本键盘路由处理器

@dp.message(F.text.in_({"🛍️ 购买商品", "🛍️ 浏览商品"}))
async def text_shop_redirect(message: types.Message, db_pool):
    """【键盘路由】当点击底部键盘的商品按钮时，拦截并原地重定向运行 /shop 的逻辑"""
    await cmd_shop(message, db_pool)


@dp.message(F.text == "👤 个人中心")
async def text_me_redirect(message: types.Message, db_pool):
    """【键盘路由】当点击底部键盘的个人中心按钮时，拦截并原地重定向运行 /me 的逻辑"""
    await cmd_me(message, db_pool)


@dp.message(F.text == "⚙️ 管理页面", F.from_user.id == ADMIN_ID)
async def text_admin_redirect(message: types.Message, db_pool):
    """【键盘路由】仅限管理员：点击管理页面时，安全拦截并重定向运行 /admin 逻辑"""
    await admin_menu(message, db_pool)


@dp.message(F.text == "☎️ 联系客服")
async def text_cs_redirect(message: types.Message, db_pool):
    """【键盘路由】普通用户：点击底部客服时，动态拉取 DB 最新客服配置并展示呼出名片按钮"""
    config = await get_system_config(db_pool)
    customer_service = config.get("customer_service", "").strip()
    
    if customer_service:
        # 转换并绑定跳转，点击一键唤起客服个人名片 [1]
        cs_url = _customer_service_url(customer_service)
        builder = InlineKeyboardBuilder()
        builder.button(text="💬 唤起客服名片", url=cs_url)
        await message.answer(
            "☎️ **客服联系通道**\n\n请点击下方按钮直接在 Telegram 中弹出客服资料卡发起会话：",
            reply_markup=builder.as_markup(),
            parse_mode="Markdown"
        )
    else:
        await message.answer("⚠️ 系统暂未配置客服，请稍后再试。")


if __name__ == "__main__":
    try:
        asyncio.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 机器人已关闭")
