"""
環境変数読み込み用のユーティリティ
.envファイルから環境変数を読み込み、パス解決のためのヘルパー関数を提供します
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

# 環境変数から設定を取得（デフォルト値も設定）
DATA_ROOT_DIR = os.getenv("DATA_ROOT_DIR", "D:\\")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(DATA_ROOT_DIR, "output"))

# 出力ディレクトリが存在しない場合は作成
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_data_path(relative_path):
    """データディレクトリ内のファイルパスを取得"""
    return os.path.join(DATA_ROOT_DIR, relative_path)


def get_output_path(filename):
    """出力ディレクトリ内のファイルパスを取得"""
    return os.path.join(OUTPUT_DIR, filename)
