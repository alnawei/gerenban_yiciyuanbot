# Telegram 一次元零售机器人

这是一个基于 aiogram 的 Telegram 零售机器人，用一次元共享 API 同步商品与自动发货，用 TRON / TronGrid 监听 TRC20 USDT 转账并自动核销订单。

## 核心架构流程

### 模块职责

- `main.py`：Telegram Bot 入口，注册用户端与管理员端 handler，启动后台任务，维护单实例运行锁。
- `database.py`：MySQL 异步数据访问层，负责连接池、商品同步、订单、用户余额、交易哈希去重和监听游标。
- `payments.py`：创建付款订单，使用 Redis 预锁随机尾数金额，避免多个待支付订单金额相同。
- `tron_listener.py`：轮询 TronGrid TRC20 入账记录，按精确 `final_amount` 匹配待支付订单，失败时指数退避重试。
- `settlement.py`：订单核销事务，保证标记支付、记录 tx_hash、充值入账或发货记录写入的原子性。
- `fulfillment.py`：付款成功后的用户通知与一次元 API 提货。
- `yiciyuan_api.py`：一次元共享 API 签名、请求、商品拉取和下单封装。
- `amount_lock.py`：Redis 金额坑位锁，锁 TTL 与订单过期时间一致。
- `notifications.py`：管理员告警通知。

### 业务流程

1. 管理员通过 `/admin` 配置一次元商铺地址、`app_id`、`app_key`。
2. 管理员点击“立即同步”，或后台定时任务每 10 分钟同步一次远端商品。
3. 远端分类写入 `categories.category_name`，商品写入 `products.category_name`，字段统一使用 `category_name`。
4. 用户通过 `/shop` 浏览上架分类和商品。
5. 用户下单或充值时，系统基于基础金额生成带 6 位小数随机尾数的 `final_amount`。
6. `payments.py` 先用 Redis 锁住金额，再写入 `recharge_orders`，订单状态为 `pending`。
7. `tron_listener.py` 轮询 TronGrid，发现转入 `USDT_WALLET` 的 TRC20 USDT 后，按 `final_amount` 精确匹配未过期订单。
8. `settlement.py` 使用事务写入 `tx_hash` 并变更订单状态，充值订单同时增加用户余额，购买订单随后调用一次元 API 发货。
9. 已处理或未匹配的链上交易写入 `tx_records`，防止重启后重复核销。

## 环境变量配置

创建 `.env`：

```env
BOT_TOKEN=123456:telegram_bot_token
ADMIN_ID=123456789

DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=tg_bot_db
DB_PASSWORD=replace_with_strong_password
DB_NAME=tg_bot_db

REDIS_URL=redis://127.0.0.1:6379/0

USDT_WALLET=Txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TRONGRID_API_KEY=your_trongrid_api_key
TRON_POLL_INTERVAL=15
ORDER_EXPIRE_MINUTES=30

DEFAULT_MARKUP_PERCENT=0.2
```

变量说明：

- `BOT_TOKEN`：Telegram BotFather 生成的机器人 token，生产环境必须配置。
- `ADMIN_ID`：管理员 Telegram 数字 ID。所有管理 handler 都使用该 ID 过滤。
- `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` / `DB_NAME`：MySQL 连接配置。连接层强制使用 `utf8mb4`。
- `REDIS_URL`：Redis 地址，用于金额锁和机器人单实例锁。
- `USDT_WALLET`：TRON 收款地址，必须是以 `T` 开头的 TRC20 地址。
- `TRONGRID_API_KEY`：TronGrid API Key，建议生产环境配置，降低限速概率。
- `TRON_POLL_INTERVAL`：链上轮询间隔，最小会被限制为 2 秒。
- `ORDER_EXPIRE_MINUTES`：订单有效期，同时也是 Redis 金额锁 TTL。
- `DEFAULT_MARKUP_PERCENT`：预留业务配置，当前主流程未强依赖。

## 初始化 SQL

完整脚本在 `migrate_phase2.sql`。新库初始化：

```bash
mysql -u root -p -e "CREATE DATABASE tg_bot_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
mysql -u root -p tg_bot_db < migrate_phase2.sql
```

核心建表约束：

```sql
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS categories (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    category_name VARCHAR(191) NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_categories_category_name (category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS products (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    remote_code VARCHAR(191) NOT NULL,
    remote_name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NULL,
    category_name VARCHAR(191) NOT NULL,
    last_sync_price DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    price DECIMAL(20,2) NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_products_remote_code (remote_code),
    KEY idx_products_category_active (category_name, is_active),
    CONSTRAINT fk_products_category_name
        FOREIGN KEY (category_name) REFERENCES categories (category_name)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT chk_products_remote_code_nonempty CHECK (remote_code <> ''),
    CONSTRAINT chk_products_remote_name_nonempty CHECK (remote_name <> ''),
    CONSTRAINT chk_products_category_name_nonempty CHECK (category_name <> ''),
    CONSTRAINT chk_products_display_name_nonempty CHECK (
        display_name IS NULL OR display_name <> ''
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

运行旧库迁移前建议先备份：

```bash
mysqldump -u root -p tg_bot_db > tg_bot_db_backup.sql
mysql -u root -p tg_bot_db < migrate_phase2.sql
```

## 本地运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

启动时会执行生产配置自检。若 `BOT_TOKEN`、`ADMIN_ID`、`DB_PASSWORD` 或 `USDT_WALLET` 仍为空或占位符，程序会拒绝启动。

## Supervisor 部署

建议使用独立系统用户运行：

```bash
sudo useradd -r -m -d /opt/yiiiiciyuan yiiiiciyuan
sudo mkdir -p /opt/yiiiiciyuan
sudo chown -R yiiiiciyuan:yiiiiciyuan /opt/yiiiiciyuan
```

部署代码并安装依赖：

```bash
sudo -u yiiiiciyuan git clone https://github.com/alnawei/yiiiiciyuan.git /opt/yiiiiciyuan/app
cd /opt/yiiiiciyuan/app
sudo -u yiiiiciyuan python3 -m venv .venv
sudo -u yiiiiciyuan .venv/bin/pip install -r requirements.txt
sudo -u yiiiiciyuan cp .env.example .env
```

编辑 `/opt/yiiiiciyuan/app/.env` 后，创建 Supervisor 配置 `/etc/supervisor/conf.d/yiiiiciyuan.conf`：

```ini
[program:yiiiiciyuan]
directory=/opt/yiiiiciyuan/app
command=/opt/yiiiiciyuan/app/.venv/bin/python /opt/yiiiiciyuan/app/main.py
user=yiiiiciyuan
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=30
killasgroup=true
stopsignal=TERM
environment=PYTHONUNBUFFERED="1"
stdout_logfile=/var/log/yiiiiciyuan.out.log
stderr_logfile=/var/log/yiiiiciyuan.err.log
stdout_logfile_maxbytes=50MB
stderr_logfile_maxbytes=50MB
stdout_logfile_backups=5
stderr_logfile_backups=5
```

加载并启动：

```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start yiiiiciyuan
sudo supervisorctl status yiiiiciyuan
```

常用运维命令：

```bash
sudo supervisorctl restart yiiiiciyuan
sudo supervisorctl tail -f yiiiiciyuan stdout
sudo supervisorctl tail -f yiiiiciyuan stderr
```

## 稳定性与安全要点

- 单实例锁：`main.py` 会在 Redis 中创建 `telegram_bot:polling_instance`，防止两个 polling 实例同时运行导致 `TelegramConflictError`。
- 链上重试：TronGrid 超时、网络异常、4xx、5xx 都会指数退避重试，最长等待 300 秒。
- 字符集：MySQL 连接和建表都固定为 `utf8mb4`，支持 Telegram 昵称、商品名和 emoji。
- 空字符串防御：数据库写入入口会拒绝关键业务字段为空，避免空按钮文本导致 Telegram inline keyboard 渲染失败。
- 权限校验：所有管理员配置、同步、重命名、上下架、定价 handler 均带 `F.from_user.id == ADMIN_ID` 过滤。
