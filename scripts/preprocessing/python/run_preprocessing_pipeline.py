#!/usr/bin/env python3
"""
DeSC-Nalmefene 前処理パイプライン実行スクリプト

このスクリプトは、F10.2患者の抽出から分析用データセット作成まで
の全工程を順次実行します。

作成者: Devin
作成日: 2023-05-20
"""

import os
import sys
import subprocess
import logging
import time
from pathlib import Path

# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.env_loader import OUTPUT_DIR

# Create local logs directory before setting up logging
os.makedirs("outputs/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outputs/logs/preprocessing_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_path: str, script_name: str) -> bool:
    """指定されたスクリプトを実行し、成功/失敗を返す"""
    logger.info(f"\n{'='*50}")
    logger.info(f"{script_name} を開始します")
    logger.info(f"スクリプト: {script_path}")
    logger.info(f"{'='*50}")
    
    start_time = time.time()
    
    try:
        # スクリプトを実行
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=3600  # 1時間のタイムアウト
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            logger.info(f"{script_name} が正常に完了しました（実行時間: {duration:.2f}秒）")
            if result.stdout:
                logger.info(f"出力:\n{result.stdout}")
            return True
        else:
            logger.error(f"{script_name} が失敗しました（終了コード: {result.returncode}）")
            logger.error(f"エラー出力:\n{result.stderr}")
            if result.stdout:
                logger.info(f"標準出力:\n{result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"{script_name} がタイムアウトしました（1時間）")
        return False
    except Exception as e:
        logger.error(f"{script_name} の実行中にエラーが発生しました: {e}")
        return False

def check_output_files() -> dict:
    """出力ファイルの存在確認"""
    logger.info("出力ファイルの確認を開始します")
    
    # 期待される出力ファイル
    expected_files = {
        "f10_2_extraction": [
            "f10_2_patients_primary_cohort.feather",
            "f10_2_patients_sensitivity_cohort1.feather", 
            "f10_2_patients_sensitivity_cohort2.feather",
            "f10_2_patients_all.feather"
        ],
        "analysis_datasets": [
            "primary_cohort_baseline.feather",
            "primary_cohort_longitudinal.feather",
            "sensitivity1_cohort_baseline.feather",
            "sensitivity2_cohort_baseline.feather",
            "all_cohort_baseline.feather"
        ]
    }
    
    file_status = {}
    
    for category, files in expected_files.items():
        file_status[category] = {}
        logger.info(f"\n--- {category} ---")
        
        for filename in files:
            file_path = os.path.join(OUTPUT_DIR, filename)
            exists = os.path.exists(file_path)
            file_status[category][filename] = exists
            
            if exists:
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                logger.info(f"✓ {filename} (サイズ: {file_size:.2f} MB)")
            else:
                logger.warning(f"✗ {filename} が見つかりません")
    
    return file_status

def generate_summary_report(file_status: dict, pipeline_start_time: float):
    """実行サマリーレポートの生成"""
    pipeline_end_time = time.time()
    total_duration = pipeline_end_time - pipeline_start_time
    
    logger.info(f"\n{'='*60}")
    logger.info("DeSC-Nalmefene 前処理パイプライン実行サマリー")
    logger.info(f"{'='*60}")
    logger.info(f"総実行時間: {total_duration:.2f}秒 ({total_duration/60:.1f}分)")
    logger.info(f"出力ディレクトリ: {OUTPUT_DIR}")
    
    # ファイル生成状況
    logger.info(f"\n--- 生成されたファイル ---")
    total_files = 0
    created_files = 0
    
    for category, files in file_status.items():
        category_created = sum(1 for exists in files.values() if exists)
        category_total = len(files)
        total_files += category_total
        created_files += category_created
        
        logger.info(f"{category}: {category_created}/{category_total} ファイル")
        
        for filename, exists in files.items():
            status = "✓" if exists else "✗"
            logger.info(f"  {status} {filename}")
    
    logger.info(f"\n全体: {created_files}/{total_files} ファイルが正常に作成されました")
    
    # 推奨される次のステップ
    logger.info(f"\n--- 次のステップ ---")
    if created_files == total_files:
        logger.info("✓ 全ての前処理が正常に完了しました")
        logger.info("✓ Rスクリプトでの記述統計・Table生成に進むことができます")
        logger.info("  実行コマンド例:")
        logger.info("  Rscript scripts/analysis/table1_generator.R")
    else:
        logger.info("⚠ 一部のファイルが作成されていません")
        logger.info("⚠ エラーログを確認し、問題を解決してから再実行してください")
    
    logger.info(f"{'='*60}")

def main():
    """メイン処理"""
    pipeline_start_time = time.time()
    
    logger.info("DeSC-Nalmefene 前処理パイプラインを開始します")
    logger.info(f"プロジェクトルート: {project_root}")
    logger.info(f"出力ディレクトリ: {OUTPUT_DIR}")
    
    # 実行するスクリプトの定義
    scripts = [
        {
            "path": "scripts/preprocessing/python/extract_f10_2_patients.py",
            "name": "F10.2患者抽出",
            "description": "DeSCデータベースからF10.2患者を抽出し、インデックス日を設定"
        },
        {
            "path": "scripts/preprocessing/python/create_analysis_dataset.py", 
            "name": "分析用データセット作成",
            "description": "抽出された患者に必要な変数を結合し、分析用データセットを作成"
        }
    ]
    
    # 各スクリプトを順次実行
    success_count = 0
    for i, script_info in enumerate(scripts, 1):
        script_path = os.path.join(project_root, script_info["path"])
        
        if not os.path.exists(script_path):
            logger.error(f"スクリプトが見つかりません: {script_path}")
            continue
        
        logger.info(f"\nステップ {i}/{len(scripts)}: {script_info['description']}")
        
        success = run_script(script_path, script_info["name"])
        
        if success:
            success_count += 1
            logger.info(f"ステップ {i} 完了")
        else:
            logger.error(f"ステップ {i} 失敗 - パイプラインを中断します")
            break
    
    # 出力ファイルの確認
    file_status = check_output_files()
    
    # サマリーレポートの生成
    generate_summary_report(file_status, pipeline_start_time)
    
    # 終了ステータス
    if success_count == len(scripts):
        logger.info("全ての処理が正常に完了しました")
        return 0
    else:
        logger.error(f"処理が失敗しました（{success_count}/{len(scripts)} ステップ完了）")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
