"""
logger_setup.py — 统一日志配置
功能：
  - RotatingFileHandler：单文件最大 10MB，保留 5 个备份，防磁盘打满
  - StreamHandler：同时输出到控制台（Supervisor 可收集）
  - 调用方只需在 main.py 顶部 `from logger_setup import setup_logging; setup_logging()`
"""

import logging
import logging.handlers
import os

LOG_DIR  = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

# 单文件最大字节数（10 MB）
MAX_BYTES   = 10 * 1024 * 1024
BACKUP_COUNT = 5   # 保留 bot.log.1 ~ bot.log.5


def setup_logging(level: int = logging.INFO) -> None:
    """
    初始化全局日志。在 main.py 最顶部调用一次即可，
    之后所有模块的 logging.getLogger(__name__) 自动继承格式。
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── 滚动文件 Handler ────────────────────────────────────────────
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    # ── 控制台 Handler ──────────────────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    # ── Root logger ─────────────────────────────────────────────────
    root = logging.getLogger()
    root.setLevel(level)
    # 防止重复添加（热重载场景）
    if not root.handlers:
        root.addHandler(file_handler)
        root.addHandler(console_handler)

    # 压制 aiogram / aiohttp 的 DEBUG 噪音
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
