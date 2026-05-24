# TeleRetail — 一次元 Telegram 智能发卡零售系统

TeleRetail 是一款专为数字商品发卡与余额充值设计的高性能 Telegram 机器人系统。系统底层通过接入**一次元商铺共享 API** 自动同步商品，并基于波场（TRON）链上轮询与 **TronGrid** 数据服务监听 TRC-20 USDT 的到账情况，实现了高并发场景下的“自动改价防碰撞、转账自动确认、秒级提卡”全流程闭环。

---

## 🛠 系统核心特性

### 1. 独创的分布式阶梯式防碰撞金额算法
* **高并发 $O(1)$ 锁预检**：下单时系统动态生成微小偏移金额（支持最高 3 位小数精度，完美解决 binary 浮点数漂移）。
* **先抢锁，后查库**：优化了原先的检查机制，系统优先通过 Redis SETNX 分布式锁对金额进行原子抢占，锁定成功后才向 MySQL 发起 `pending` 状态确认，使高并发下的数据库压力从 $O(N)$ 降到 $O(1)$。
* **锁自动释放**：当订单被取消、超时过期（默认 30 分钟）或核销完成时，金额锁自动回收。

### 2. 0 秒热加载与无感知切换
* **配置数据库级持久化**：TRC-20 收款地址、TronGrid API Key 及客服地址优先从 `system_config` 数据库表热读取，并以降级适配 `.env`。
* **平滑变更**：管理员在后台管理面板修改收款地址或 TronGrid Key 后，链上监听器会即刻加载新配置进行轮询，**无需重启守护进程**。

### 3. 极高韧性的链上监听器 (TronListener)
* **防断网挂死设计**：初始化方法（恢复历史去重 Hash 缓存和读取游标）集成于容错轮询循环内部。若由于网络波动或数据库重启导致启动失败，监听器将自动执行**指数退避重试**，防止因单个后台协程静默死亡导致漏单。
* **复用 HTTP 连接池**：基于 `aiohttp.ClientSession` 持久化生命周期，避免频繁的高并发 TCP 握手开销。
* **重启不漏单**：持久化毫秒级时间戳游标（Cursor），重启时自动安全向前覆盖重叠窗口（默认 5 分钟），物理阻断漏网之鱼。

### 4. 极致的前后端安全与排版优化
* **无颤抖更新**：后台分类管理通过按键布局增量修改（`use_reply_markup_only`），切换状态及删除数据时界面文字不颤抖不闪烁。
* **物理隔离菜单**：系统通过自定义 Reply 底部键盘对管理员与普通用户进行常驻菜单物理分流。
* **实体防护（防接口崩溃）**：前台渲染、充值失败通知、管理员中控台一律弃用易碎的 Markdown 解析，全面升级为 `HTML` 模式加 `html.escape` 转义，彻底防御商品名中的下划线 `_`、星号 `*` 等带来的 Telegram 接口解析报错。
* **严格多表事务**：后台分类管理等涉及多表更新的操作（`products` 与 `categories` 关联重命名、级联删除等）已全面补充 `START TRANSACTION` 控制，严防部分 SQL 独立 autocommit 造成的一致性漏洞。

---

## 📡 核心业务架构流

```text
 ユーザー (User) ───[ 1. 请求购买/充值 ]───► 机器人 (main.py)
                                               │ (2. 抢占 Redis 偏移金额锁)
                                               ▼
  链上核销 (settlement) ◄───[ 5. 按精确金额匹配 ]─ 监听器 (tron_listener.py)
   (MySQL START TRANSACTION)                    ▲ (轮询 TRON TRC-20 记录)
         │                                      │
         ├─► [Recharge 充值]: 加用户余额          │
         │                                   USDT (TRC20)
         └─► [Purchase 购买]: YiCiyuan API 发卡 ───► 用户提货/卡密入库



📦 项目依赖与技术栈
开发语言：Python 3.10+
异步框架：aiogram 3.x (基于最新 Telegram Bot API 架构)
数据库：MySQL 5.7+ / MariaDB 10.6+
内存缓存：Redis 6.0+ (提供多层并发金额锁与 Polling 独占锁)
API 与 SDK：aiohttp 3.9+, aiomysql 0.2+, redis 5.0+


⚙️ 环境变量配置 (.env)
项目目录下需创建并配置 .env 文件（请勿将真实私钥和配置提交至代码库）：
code
Env
# ── Telegram 机器人配置 ──────────────────────────────────────────
BOT_TOKEN=123456:your_telegram_bot_token
ADMIN_ID=987654321

# ── 数据库连接（MySQL/MariaDB） ──────────────────────────────────
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=tg_bot_db
DB_PASSWORD=your_mysql_secure_password
DB_NAME=tg_bot_db

# ── 缓存连接 (Redis) ─────────────────────────────────────────────
REDIS_URL=redis://127.0.0.1:6379/0

# ── 支付与链上监听配置 ──────────────────────────────────────────
# USDT_WALLET 为系统起步默认监听地址，管理员可在后台面板无缝更改
USDT_WALLET=TXyourtronwalletaddresshereXXXXXX
TRONGRID_API_KEY=your_trongrid_api_key
TRON_POLL_INTERVAL=15
ORDER_EXPIRE_MINUTES=30

# ── 预留业务配置 ─────────────────────────────────────────────────
DEFAULT_MARKUP_PERCENT=0.2
🗄 数据库结构设计 (Schema)
通过配置好 MySQL 后，执行 migrate_phase2.sql 进行建表和初始化。核心数据库包含 7 个功能表：
system_config：系统级键值对参数，包含远程商铺 API、链上扫描游标等。
categories：商品分类表（含激活状态，与普通端显示直接联动）。
products：同步商品表，外键关联 categories.category_name，具有级联约束。
users：本地用户资产余额。
recharge_orders：待付与已支付订单详情（主流程实体）。
tx_records：去重用的链上已处理交易哈希历史。
delivery_records：提卡卡密及异常日志。
🚀 部署指南 (Production Deployment)
为了保证机器人的稳定性，推荐在独立的不具特权系统用户环境下，使用 Supervisor 守护进程进行托管。
第一步：创建独立系统运行账户
code
Bash
sudo useradd -r -m -d /opt/tgretail tgretail
sudo mkdir -p /opt/tgretail
sudo chown -R tgretail:tgretail /opt/tgretail
第二步：拉取源码与构建虚拟环境
code
Bash
# 切换至低权限用户
sudo -u tgretail -i

# 克隆代码
git clone https://github.com/your-repo/yiiiiciyuan.git /opt/tgretail/app
cd /opt/tgretail/app

# 初始化 Python 虚拟环境与安装依赖
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 准备环境变量
cp .env.example .env
nano .env # 按照上文配置说明填写
exit
第三步：配置 Supervisor 进程管理
创建配置文件 /etc/supervisor/conf.d/tgretail.conf：
code
Ini
[program:tgretail]
directory=/opt/tgretail/app
command=/opt/tgretail/app/.venv/bin/python /opt/tgretail/app/main.py
user=tgretail
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=30
killasgroup=true
stopsignal=TERM
environment=PYTHONUNBUFFERED="1"
stdout_logfile=/var/log/tgretail.out.log
stderr_logfile=/var/log/tgretail.err.log
stdout_logfile_maxbytes=20MB
stderr_logfile_maxbytes=20MB
stdout_logfile_backups=5
stderr_logfile_backups=5
第四步：启动机器人守护
code
Bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start tgretail
sudo supervisorctl status tgretail
🛠 日常运维与监控
查看实时机器人控制台输出
code
Bash
sudo supervisorctl tail -f tgretail stdout
查看链上交易或发卡异常输出
code
Bash
sudo supervisorctl tail -f tgretail stderr
重启机器人（热重载系统配置无需重启，仅代码变更才需要）
code
Bash
sudo supervisorctl restart tgretail
