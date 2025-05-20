"""
設定ファイルの読み込みと環境変数を使ったパス解決を行うモジュール
"""

import json
import os
import sys

# ルートディレクトリをパスに追加して、utilsモジュールをインポートできるようにする
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.env_loader import get_data_path, get_output_path


def load_config(config_path="config.json"):
    """
    設定ファイルを読み込み、環境変数を使ってパスを解決する

    Args:
        config_path: 設定ファイルのパス（data_definition_processingディレクトリからの相対パス）

    Returns:
        dict: 設定情報を含む辞書
    """
    # 設定ファイルのフルパスを取得
    full_config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), config_path
    )

    # 設定ファイルを読み込む
    with open(full_config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # 入力パスを環境変数を使って解決
    for key, path in config["input_paths"].items():
        if path.startswith("../"):
            # 相対パスを変換
            relative_path = path.replace("../", "")
            config["input_paths"][key] = get_data_path(relative_path)

    # 出力パスを環境変数を使って解決
    for key, path in config["output_paths"].items():
        if path.startswith("../"):
            # 相対パスを変換
            relative_path = path.replace("../", "")
            config["output_paths"][key] = get_output_path(relative_path)

    return config
