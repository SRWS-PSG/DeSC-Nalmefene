#!/usr/bin/env python3
"""
DeSC-Nalmefene F10.2患者抽出スクリプト

このスクリプトは、DeSCデータベースからF10.2（アルコール依存症）患者を抽出し、
インデックス日を設定します。

作成者: Devin
作成日: 2023-05-20
"""

import os
import sys
# Add project root to sys.path to allow importing from 'utils'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import logging
import polars as pl
from pathlib import Path
import gc
from typing import Dict, List, Set, Optional
import psutil
from tqdm import tqdm
import time
from utils.env_loader import DATA_ROOT_DIR as ENV_DATA_ROOT_DIR, OUTPUT_DIR as ENV_OUTPUT_DIR

# Create local logs directory before setting up logging
os.makedirs("outputs/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outputs/logs/extract_f10_2_patients.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Config:
    DATA_ROOT_DIR = ENV_DATA_ROOT_DIR
    OUTPUT_DIR = ENV_OUTPUT_DIR
    
    F10_2_CODE = "F10.2"
    
    STUDY_PERIOD_START = "2014-04-01"
    STUDY_PERIOD_END = "2023-09-30"
    
    PRIMARY_WASHOUT_WEEKS = 52

def optimize_parameters():
    """システムリソースに基づく最適なパラメータの設定"""
    n_threads = max(1, int(psutil.cpu_count(logical=True) * 0.75))
    
    available_memory = psutil.virtual_memory().available
    chunk_size = min(500_000, int(available_memory * 0.3 / 1024))
    
    batch_size = n_threads * 2
    
    return {
        'n_threads': n_threads,
        'chunk_size': chunk_size,
        'batch_size': batch_size
    }

def load_icd10_master(base_dir: str) -> pl.DataFrame:
    """ICD10マスターデータの読み込み"""
    logger.info("ICD10マスターデータの読み込みを開始します")
    
    file_path = os.path.join(base_dir, "m_icd10.feather")
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB単位
        logger.info(f"m_icd10.feather (サイズ: {file_size:.2f} MB) を読み込んでいます")
        return pl.read_ipc(file_path)
    else:
        logger.error(f"ICD10マスターファイルが見つかりません: {file_path}")
        return pl.DataFrame()

def get_diseases_codes_for_icd10(icd10_master: pl.DataFrame, 
                                icd10_code: str,
                                icd10_kbn_code: str = "1") -> List[str]:
    """
    指定されたICD10コードに対応するレセプト病名コード（7桁）のリストを取得
    
    Args:
        icd10_master: ICD10マスターデータ
        icd10_code: ICD10コード（例：F102）
        icd10_kbn_code: ICD10区分コード（デフォルト：1=基本疾患）
    
    Returns:
        List[str]: レセプト病名コードのリスト
    """
    logger.info(f"ICD10コード '{icd10_code}' (区分: {icd10_kbn_code}) に対応するレセプト病名コードを検索")
    
    filtered_df = icd10_master.filter(
        (pl.col("icd10_code") == icd10_code) & 
        (pl.col("icd10_kbn_code") == icd10_kbn_code)
    )
    
    diseases_codes = filtered_df.select("diseases_code").to_series().to_list()
    
    logger.info(f"見つかったレセプト病名コード: {len(diseases_codes)}件")
    for code in diseases_codes:
        logger.info(f"  - {code}")
    
    return diseases_codes

def get_disease_files(disease_dir: str) -> List[str]:
    """疾患ファイルのリストを取得"""
    disease_files = []
    if os.path.exists(disease_dir) and os.path.isdir(disease_dir):
        disease_files = [os.path.join(disease_dir, f)
                       for f in os.listdir(disease_dir)
                       if f.startswith("receipt_diseases") and f.endswith(".feather")]
        logger.info(f"疾患ファイル {len(disease_files)} 件を検出しました")
    else:
        logger.warning(f"疾患ファイルディレクトリが見つかりません: {disease_dir}")
    
    return sorted(disease_files)

def extract_f10_2_patients(disease_files: List[str], 
                          f10_2_diseases_codes: List[str],
                          params: Dict) -> pl.DataFrame:
    """F10.2（アルコール依存症）患者の抽出"""
    start_time = time.time()
    logger.info("F10.2患者の抽出を開始します")
    
    results = []
    
    logger.info("疾患ファイルの処理状況:")
    for file_path in tqdm(disease_files, desc="疾患ファイル処理", unit="file"):
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB単位
        logger.info(f"処理中: {os.path.basename(file_path)} (サイズ: {file_size:.2f} MB)")
        
        df_lazy = (pl.scan_ipc(
            source=file_path,
            memory_map=True,
            n_rows=params['chunk_size']
        )
        .filter(pl.col("diseases_code").is_in(f10_2_diseases_codes))
        .select([
            "kojin_id",
            "receipt_id",
            "receipt_ym",
            "diseases_code",
            "sinryo_start_ymd",  # 診療開始日
            "shubyomei_flg",     # 主病名フラグ
            "tenki_kbn_code",    # 転帰区分コード
            "utagai_flg"         # 疑いフラグ
        ]))
        
        result = df_lazy.collect(streaming=True, n_threads=params['n_threads'])
        
        if not result.is_empty():
            # Log unique disease codes found for F10.2 for verification
            unique_codes_found = result.select(pl.col("diseases_code").unique()).to_series().to_list()
            logger.info(f"ファイル {os.path.basename(file_path)} で見つかったF10.2関連のdiseases_code: {unique_codes_found}")
            results.append(result)
    
    if not results:
        logger.warning("F10.2患者が見つかりませんでした")
        return pl.DataFrame()
    
    # 全結果を結合
    patients_df = pl.concat(results)
    logger.info(f"抽出された延べレコード数: {len(patients_df)}")
    
    # 各患者の初回診断日（インデックス日）を特定
    index_dates = (patients_df
        .sort(["kojin_id", "sinryo_start_ymd", "receipt_ym"])
        .group_by("kojin_id")
        .agg([
            pl.col("sinryo_start_ymd").first().alias("index_date"),
            pl.col("receipt_id").first().alias("first_receipt_id"),
            pl.col("receipt_ym").first().alias("first_receipt_ym"),
            pl.col("diseases_code").first().alias("first_diseases_code"),
            pl.col("shubyomei_flg").first().alias("first_shubyomei_flg"),
            pl.col("tenki_kbn_code").first().alias("first_tenki_kbn_code"),
            pl.col("utagai_flg").first().alias("first_utagai_flg"),
            pl.count().alias("total_f10_2_records")  # F10.2関連の総レコード数
        ]))
    
    # 日付フィルタリング（研究期間内）
    index_dates = index_dates.filter(
        (pl.col("index_date") >= Config.STUDY_PERIOD_START) &
        (pl.col("index_date") <= Config.STUDY_PERIOD_END)
    )
    
    logger.info(f"研究期間内（{Config.STUDY_PERIOD_START}〜{Config.STUDY_PERIOD_END}）の患者数: {len(index_dates)}")
    
    end_time = time.time()
    logger.info(f"F10.2患者の抽出処理が完了しました。処理時間: {end_time - start_time:.2f}秒")
    
    return index_dates

def apply_washout_criteria(patients_df: pl.DataFrame, washout_weeks: int = 52) -> pl.DataFrame:
    """ウォッシュアウト期間の適用"""
    logger.info(f"ウォッシュアウト期間 {washout_weeks} 週の適用を開始します")
    
    # index_dateから指定週数前の日付を計算
    filtered_df = patients_df.with_columns([
        pl.col("index_date").str.to_date(format="%Y/%m/%d").alias("index_date_parsed")
    ]).filter(
        pl.col("index_date_parsed") >= pl.date(2014, 4, 1).dt.offset_by(f"{washout_weeks}w")
    ).drop("index_date_parsed")
    
    logger.info(f"ウォッシュアウト期間適用後の患者数: {len(filtered_df)}")
    
    return filtered_df

def save_results(patients_df: pl.DataFrame, output_dir: str):
    """結果の保存"""
    logger.info("結果の保存を開始します")
    
    # Primary cohort (52週ウォッシュアウト)
    primary_cohort = apply_washout_criteria(patients_df, 52)
    primary_output_path = os.path.join(output_dir, "f10_2_patients_primary_cohort.feather")
    primary_cohort.write_ipc(primary_output_path, compression="zstd")
    logger.info(f"Primary cohort (52週ウォッシュアウト) を保存しました: {primary_output_path}")
    logger.info(f"Primary cohort 患者数: {len(primary_cohort)}")
    
    # Sensitivity cohort 1 (26週ウォッシュアウト)
    sensitivity_cohort1 = apply_washout_criteria(patients_df, 26)
    sens1_output_path = os.path.join(output_dir, "f10_2_patients_sensitivity_cohort1.feather")
    sensitivity_cohort1.write_ipc(sens1_output_path, compression="zstd")
    logger.info(f"Sensitivity cohort 1 (26週ウォッシュアウト) を保存しました: {sens1_output_path}")
    logger.info(f"Sensitivity cohort 1 患者数: {len(sensitivity_cohort1)}")
    
    # Sensitivity cohort 2 (156週ウォッシュアウト)
    sensitivity_cohort2 = apply_washout_criteria(patients_df, 156)
    sens2_output_path = os.path.join(output_dir, "f10_2_patients_sensitivity_cohort2.feather")
    sensitivity_cohort2.write_ipc(sens2_output_path, compression="zstd")
    logger.info(f"Sensitivity cohort 2 (156週ウォッシュアウト) を保存しました: {sens2_output_path}")
    logger.info(f"Sensitivity cohort 2 患者数: {len(sensitivity_cohort2)}")
    
    # 全患者（ウォッシュアウト適用前）
    all_output_path = os.path.join(output_dir, "f10_2_patients_all.feather")
    patients_df.write_ipc(all_output_path, compression="zstd")
    logger.info(f"全患者データを保存しました: {all_output_path}")
    logger.info(f"全患者数: {len(patients_df)}")
    
    # サマリー統計の表示
    logger.info("\n=== F10.2患者抽出サマリー ===")
    logger.info(f"全患者数（研究期間内）: {len(patients_df)}")
    logger.info(f"Primary cohort（52週ウォッシュアウト）: {len(primary_cohort)}")
    logger.info(f"Sensitivity cohort 1（26週ウォッシュアウト）: {len(sensitivity_cohort1)}")
    logger.info(f"Sensitivity cohort 2（156週ウォッシュアウト）: {len(sensitivity_cohort2)}")

def main():
    """メイン処理"""
    logger.info("DeSC-Nalmefene F10.2患者抽出を開始します")
    
    params = optimize_parameters()
    logger.info(f"最適化パラメータ: {params}")
    
    # ICD10マスターデータの読み込み
    icd10_master = load_icd10_master(Config.DATA_ROOT_DIR)
    if icd10_master.is_empty():
        logger.error("ICD10マスターデータが読み込めないため処理を終了します")
        return
    
    # F10.2に対応するレセプト病名コードを取得
    f10_2_diseases_codes = get_diseases_codes_for_icd10(icd10_master, "F102", "1")
    if not f10_2_diseases_codes:
        logger.error("F10.2に対応するレセプト病名コードが見つからないため処理を終了します")
        return
    
    # 疾患ファイルの取得
    disease_dir = os.path.join(Config.DATA_ROOT_DIR, "receipt_diseases")
    disease_files = get_disease_files(disease_dir)
    if not disease_files:
        logger.error("疾患ファイルが見つからないため処理を終了します")
        return
    
    # F10.2患者の抽出
    patients_df = extract_f10_2_patients(disease_files, f10_2_diseases_codes, params)
    if patients_df.is_empty():
        logger.error("F10.2患者が見つからないため処理を終了します")
        return
    
    # 結果の保存
    save_results(patients_df, Config.OUTPUT_DIR)
    
    logger.info("F10.2患者抽出処理が完了しました")

if __name__ == "__main__":
    main()
