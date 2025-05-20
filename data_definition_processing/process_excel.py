#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
データ定義書処理プロセスの統合実行スクリプト

このスクリプトは、Excelの「data定義書」ファイルをAIが参照しやすいJSON形式に変換する
一連の処理を実行します。
"""

import argparse
import json
import os
import sys
import time

from excel_to_json_converter import ExcelToJsonConverter
from create_ai_friendly_json import create_ai_friendly_json
from optimize_ai_friendly_json import optimize_ai_friendly_json
from logger import setup_logger, get_config


def main():
    """
    メイン処理
    """
    # コマンドライン引数の設定
    parser = argparse.ArgumentParser(description="Excelデータ定義書処理プロセス")
    parser.add_argument("--config", default="config.json", help="設定ファイルのパス")
    parser.add_argument("--excel", help="Excelファイルのパス（設定ファイルより優先）")
    parser.add_argument("--output-dir", help="出力ディレクトリ（設定ファイルより優先）")
    parser.add_argument(
        "--skip-steps",
        nargs="+",
        choices=["convert", "friendly", "optimize"],
        help="スキップするステップ（convert: Excel→JSON, friendly: JSON→AI用JSON, optimize: 最適化）",
    )
    args = parser.parse_args()

    # 設定ファイルを読み込む
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        print(f"設定ファイルの読み込みに失敗しました: {str(e)}")
        sys.exit(1)

    # ロガーの設定
    logger = setup_logger(args.config)

    # パス設定
    excel_file = args.excel or config["input_paths"]["excel_file"]
    output_dir = args.output_dir or os.path.dirname(
        config["output_paths"]["database_schema"]
    )

    # 出力ディレクトリの確認
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # スキップするステップを設定
    skip_steps = args.skip_steps or []

    # 処理時間の計測開始
    start_time = time.time()

    try:
        # ステップ1: Excelファイルの変換
        if "convert" not in skip_steps:
            logger.info("ステップ1: Excelファイルを JSON に変換します")
            converter = ExcelToJsonConverter(
                excel_file, config["output_paths"]["database_schema"], args.config
            )
            converter.process()
        else:
            logger.info("ステップ1: Excelファイルの変換をスキップします")

        # ステップ2: AIが参照しやすいJSONの作成
        if "friendly" not in skip_steps:
            logger.info("ステップ2: AIが参照しやすいJSONを作成します")
            create_ai_friendly_json(
                config["output_paths"]["database_schema"],
                config["output_paths"]["ai_friendly_schema"],
            )
        else:
            logger.info("ステップ2: AIが参照しやすいJSONの作成をスキップします")

        # ステップ3: JSONの最適化
        if "optimize" not in skip_steps:
            logger.info("ステップ3: JSONを最適化します")
            optimize_ai_friendly_json(
                config["output_paths"]["ai_friendly_schema"],
                config["output_paths"]["optimized_schema"],
            )
        else:
            logger.info("ステップ3: JSONの最適化をスキップします")

        # 処理時間の計測終了
        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(f"処理が完了しました（処理時間: {elapsed_time:.2f}秒）")

        # 生成されたファイルの情報を表示
        if os.path.exists(config["output_paths"]["database_schema"]):
            db_schema_size = (
                os.path.getsize(config["output_paths"]["database_schema"]) / 1024
            )
            logger.info(
                f"生成ファイル: {config['output_paths']['database_schema']} ({db_schema_size:.2f} KB)"
            )

        if os.path.exists(config["output_paths"]["ai_friendly_schema"]):
            ai_schema_size = (
                os.path.getsize(config["output_paths"]["ai_friendly_schema"]) / 1024
            )
            logger.info(
                f"生成ファイル: {config['output_paths']['ai_friendly_schema']} ({ai_schema_size:.2f} KB)"
            )

        if os.path.exists(config["output_paths"]["optimized_schema"]):
            opt_schema_size = (
                os.path.getsize(config["output_paths"]["optimized_schema"]) / 1024
            )
            logger.info(
                f"生成ファイル: {config['output_paths']['optimized_schema']} ({opt_schema_size:.2f} KB)"
            )

    except Exception as e:
        import traceback

        logger.error(f"処理中にエラーが発生しました: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
