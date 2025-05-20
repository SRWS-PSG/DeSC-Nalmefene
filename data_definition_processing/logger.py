import logging
import json
import os
from typing import Dict, Any


def setup_logger(config_path: str = "config.json") -> logging.Logger:
    """
    ロガーを設定する

    Args:
        config_path: 設定ファイルのパス

    Returns:
        設定されたロガー
    """
    # 設定ファイルを読み込む
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # ロガーの設定
    logger = logging.getLogger("excel_processor")
    logger.setLevel(getattr(logging, config["logging"]["level"]))

    # 既存のハンドラーをクリア
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # ハンドラーの設定
    handlers = []

    # ファイルハンドラー
    if "file" in config["logging"]:
        file_handler = logging.FileHandler(config["logging"]["file"])
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        handlers.append(file_handler)

    # コンソールハンドラー
    if config["logging"].get("console", False):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        handlers.append(console_handler)

    # ハンドラーをロガーに追加
    for handler in handlers:
        logger.addHandler(handler)

    return logger


def get_config(config_path: str = "config.json") -> Dict[str, Any]:
    """
    設定ファイルを読み込む

    Args:
        config_path: 設定ファイルのパス

    Returns:
        設定データ
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
