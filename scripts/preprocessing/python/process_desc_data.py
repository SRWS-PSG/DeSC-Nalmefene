#!/usr/bin/env python3
"""
DeSC-Nalmefene データ前処理スクリプト

このスクリプトは、DeSCデータベースの.featherファイルを読み込み、
解析用の中間データセットを作成します。

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
        logging.FileHandler('outputs/logs/preprocessing.log'), # Path relative to CWD
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Config:
    DATA_ROOT_DIR = ENV_DATA_ROOT_DIR
    OUTPUT_DIR = ENV_OUTPUT_DIR
    
    F10_2_CODE = "F10.2"
    
    DRUG_CODES = {
        "nalmefene": "N07BB05",      # ナルメフェン (飲酒量低減)
        "acamprosate": "N07BB03",    # アカンプロサート (断酒)
        "disulfiram": "N07BB01",     # ジスルフィラム (断酒) 
        "cyanamide": "3932001S1041"  # シアナミド (断酒)
    }
    
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

def load_master_data(base_dir: str) -> Dict[str, pl.DataFrame]:
    """マスターデータの読み込み"""
    logger.info("マスターデータの読み込みを開始します")
    
    master_files = {
        "medical": "m_med_treat_all.feather",
        "disease": "m_disease.feather",
        "drug_main": "m_drug_main.feather",
        "drug_rece": "m_drug_rece_all.feather",
        "drug_who_atc": "m_drug_who_atc.feather",
        "icd10": "m_icd10.feather"  # ICD10マスターを追加
    }
    
    master_data = {}
    logger.info("マスターファイルの読み込み状況:")
    for key, filename in tqdm(master_files.items(), desc="マスターファイル読み込み", unit="file"):
        file_path = os.path.join(base_dir, filename)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path) / (1024 * 1024) # MB単位
            logger.info(f"{filename} (サイズ: {file_size:.2f} MB) を読み込んでいます")
            master_data[key] = pl.read_ipc(file_path)
        else:
            logger.warning(f"{filename}が見つかりません: {file_path}")
    
    return master_data

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

def identify_f10_2_patients(disease_files: List[str], 
                           master_data: Dict[str, pl.DataFrame],
                           output_path: str,
                           params: Dict) -> pl.DataFrame:
    """F10.2（アルコール依存症）患者の特定"""
    start_time = time.time()
    logger.info("F10.2患者の特定を開始します")
    
    # ICD10マスターからF10.2に対応するレセプト病名コードを取得
    if "icd10" not in master_data:
        logger.error("ICD10マスターデータが読み込まれていません")
        return pl.DataFrame()
    
    f10_2_diseases_codes = get_diseases_codes_for_icd10(master_data["icd10"], "F102", "1")
    
    if not f10_2_diseases_codes:
        logger.error("F10.2に対応するレセプト病名コードが見つかりませんでした")
        return pl.DataFrame()
    
    logger.info(f"F10.2患者の検索に使用するレセプト病名コード: {f10_2_diseases_codes}")
    
    results = []
    
    logger.info("疾患ファイルの処理状況:")
    for file_path in tqdm(disease_files, desc="疾患ファイル処理", unit="file"):
        file_size = os.path.getsize(file_path) / (1024 * 1024) # MB単位
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
            "sinryo_start_ymd"  # スキーマでは YYYY/MM/DD の char 型
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
    
    patients_df = pl.concat(results)
    
    index_dates = (patients_df
        .sort(["kojin_id", "sinryo_start_ymd"])
        .group_by("kojin_id")
        .agg([
            pl.col("sinryo_start_ymd").first().alias("index_date"),
            pl.col("receipt_id").first().alias("first_receipt_id"),
            pl.col("receipt_ym").first().alias("first_receipt_ym")
        ]))
    
    index_dates.write_ipc(output_path, compression="zstd")
    logger.info(f"F10.2患者データを保存しました: {output_path}")
    
    end_time = time.time()
    logger.info(f"F10.2患者の特定処理が完了しました。処理時間: {end_time - start_time:.2f}秒")
    return index_dates

def classify_treatment_groups(patient_df: pl.DataFrame, 
                             drug_files: List[str],
                             output_path: str,
                             params: Dict) -> pl.DataFrame:
    """治療群の分類 (飲酒量低減群、断酒群、治療目標不明群)"""
    start_time = time.time()
    logger.info("治療群の分類を開始します")
    
    patient_ids = set(patient_df["kojin_id"].to_list())
    
    treatment_groups = []
    
    logger.info("薬剤ファイルの処理状況:")
    for file_path in tqdm(drug_files, desc="薬剤ファイル処理", unit="file"):
        file_size = os.path.getsize(file_path) / (1024 * 1024) # MB単位
        logger.info(f"処理中: {os.path.basename(file_path)} (サイズ: {file_size:.2f} MB)")
        df_lazy = (pl.scan_ipc(
            source=file_path,
            memory_map=True,
            n_rows=params['chunk_size']
        )
        .filter(pl.col("kojin_id").is_in(patient_ids))
        .select([
            "kojin_id", # 患者のインデックス日との結合に必要
            "receipt_id",
            "line_no", # 処方日情報との結合キー
            "receipt_ym",
            "drug_code"  # shohou_ymd や dispensing_ymd は別テーブル
        ]))
        
        df_drug_info = df_lazy.collect(streaming=True, n_threads=params['n_threads'])
        
        if not df_drug_info.is_empty():
            # 薬剤処方日・調剤日情報を取得するために receipt_drug_santei_ymd を読み込む
            # この処理はファイルごとに行う必要があるため、drug_files のループ内で処理する
            # santei_ymd_file_path を drug_file の情報から構築する
            base_filename = os.path.basename(file_path) # e.g., receipt_drug_202201.feather
            # Assuming filenames like receipt_drug_YYYYMM.feather and receipt_drug_santei_ymd_YYYYMM.feather
            santei_base_filename = base_filename.replace("receipt_drug_", "receipt_drug_santei_ymd_")
            santei_ymd_file_path = os.path.join(Config.DATA_ROOT_DIR, "receipt_drug_santei_ymd", santei_base_filename)
            
            if not os.path.exists(santei_ymd_file_path):
                logger.warning(f"薬剤処方日ファイルが見つかりません: {santei_ymd_file_path} (元ファイル: {file_path})")
                continue

            df_santei_lazy = (pl.scan_ipc(
                source=santei_ymd_file_path,
                memory_map=True,
                n_rows=params['chunk_size']
            )
            # `receipt_drug` との結合のために `kojin_id` は不要だが、
            # `patient_ids` でのフィルタリングは効率化のために残す
            .filter(pl.col("kojin_id").is_in(patient_ids)) 
            .select([
                "receipt_id", # receipt_drug との結合キー
                "line_no",    # receipt_drug との結合キー
                "shohou_ymd", # YYYY/MM/DD の char 型
                "dispensing_ymd" # YYYY/MM/DD の char 型
            ]))
            df_santei = df_santei_lazy.collect(streaming=True, n_threads=params['n_threads'])

            if df_santei.is_empty():
                continue

            # 薬剤情報(df_drug_info)と処方日情報(df_santei)を結合
            # receipt_drug と receipt_drug_santei_ymd は、
            # receipt_id と line_no を複合キーとして結合する。
            
            # drug_code と処方日を結合
            df_merged_drug_date = df_drug_info.join(
                df_santei.select(["receipt_id", "line_no", "shohou_ymd", "dispensing_ymd"]),
                on=["receipt_id", "line_no"], # 正しい結合キー
                how="inner" 
            )
            
            if df_merged_drug_date.is_empty():
                continue

            reduction_codes = [Config.DRUG_CODES["nalmefene"]]
            abstinence_codes = [
                Config.DRUG_CODES["acamprosate"],
                Config.DRUG_CODES["disulfiram"],
                Config.DRUG_CODES["cyanamide"]
            ]
            
            # 患者のインデックス日と結合
            merged_df = df_merged_drug_date.join(
                patient_df.select(["kojin_id", "index_date"]),
                on="kojin_id",
                how="inner"
            )
            
            # 日付文字列をDate型に変換して比較
            merged_df = merged_df.with_columns([
                pl.col("shohou_ymd").str.to_date(format="%Y/%m/%d", strict=False),
                pl.col("index_date").str.to_date(format="%Y/%m/%d", strict=False) # index_dateも変換が必要な場合
            ])

            merged_df = merged_df.filter(
                (pl.col("shohou_ymd") >= pl.col("index_date")) &
                (pl.col("shohou_ymd") <= pl.col("index_date").dt.offset_by("12w")) # Polarsの期間加算
            )
            
            if not merged_df.is_empty():
                grouped = (merged_df
                    .with_columns([
                        (pl.col("drug_code").is_in(reduction_codes)).alias("is_reduction"),
                        (pl.col("drug_code").is_in(abstinence_codes)).alias("is_abstinence")
                    ])
                    .sort(["kojin_id", "shohou_ymd"])
                    .group_by("kojin_id")
                    .agg([
                        pl.col("is_reduction").max().alias("has_reduction"),
                        pl.col("is_abstinence").max().alias("has_abstinence"),
                        pl.col("shohou_ymd").min().alias("first_drug_date") # 最小の処方日
                    ]))
                
                treatment_groups.append(grouped)
    
    if treatment_groups:
        combined_groups = pl.concat(treatment_groups).unique(subset=["kojin_id"])
        
        classified = (patient_df
            .join(
                combined_groups,
                on="kojin_id",
                how="left"
            )
            .with_columns([
                pl.when(pl.col("has_reduction") == True)
                  .then(pl.lit(1))  # 飲酒量低減治療群
                .when(pl.col("has_abstinence") == True)
                  .then(pl.lit(2))  # 断酒治療群
                .otherwise(pl.lit(3))  # 治療目標不明群（薬物療法なし）
                .alias("treatment_group")
            ]))
        
        classified.write_ipc(output_path, compression="zstd")
        logger.info(f"治療群分類データを保存しました: {output_path}")
        
        return classified
    else:
        logger.warning("治療群を分類できませんでした")
        
        classified = (patient_df
            .with_columns(pl.lit(3).alias("treatment_group")))
        
        classified.write_ipc(output_path, compression="zstd")
        logger.info(f"全患者を治療目標不明群として分類しました: {output_path}")
        
        end_time = time.time()
        logger.info(f"治療群の分類処理が完了しました。処理時間: {end_time - start_time:.2f}秒")
        return classified

def main():
    """メイン処理"""
    logger.info("DeSC-Nalmefene データ前処理を開始します")
    
    # OUTPUT_DIR is created by env_loader.py
    # Local logs directory 'outputs/logs' is created before logging setup globally
    
    params = optimize_parameters()
    logger.info(f"最適化パラメータ: {params}")
    
    master_data = load_master_data(Config.DATA_ROOT_DIR)
    
    disease_dir = os.path.join(Config.DATA_ROOT_DIR, "receipt_diseases")
    disease_files = []
    if os.path.exists(disease_dir) and os.path.isdir(disease_dir):
        disease_files = [os.path.join(disease_dir, f)
                       for f in os.listdir(disease_dir)
                       if f.startswith("receipt_diseases") and f.endswith(".feather")]
    else:
        logger.warning(f"疾患ファイルディレクトリが見つかりません: {disease_dir}")

    patients_df = identify_f10_2_patients(
        disease_files,
        master_data,
        os.path.join(Config.OUTPUT_DIR, "f10_2_patients.feather"),
        params
    )
    
    if patients_df.is_empty():
        logger.error("F10.2患者が見つからないため処理を終了します")
        return
    
    # receipt_drug と receipt_drug_santei_ymd の両方が必要
    # classify_treatment_groups 内で receipt_drug_santei_ymd も読み込むように修正済み
    drug_dir = os.path.join(Config.DATA_ROOT_DIR, "receipt_drug")
    drug_files = []
    if os.path.exists(drug_dir) and os.path.isdir(drug_dir):
        drug_files = [os.path.join(drug_dir, f)
                    for f in os.listdir(drug_dir)
                    if f.startswith("receipt_drug_") and f.endswith(".feather")] # santei_ymd files are handled separately by path construction
    else:
        logger.warning(f"薬剤ファイルディレクトリが見つかりません: {drug_dir}")
        
    # Check if receipt_drug_santei_ymd directory exists, as it's crucial
    santei_ymd_dir = os.path.join(Config.DATA_ROOT_DIR, "receipt_drug_santei_ymd")
    if not (os.path.exists(santei_ymd_dir) and os.path.isdir(santei_ymd_dir)):
        logger.warning(f"薬剤処方日ファイルディレクトリが見つかりません: {santei_ymd_dir}. 治療群分類に影響する可能性があります。")

    treatment_groups = classify_treatment_groups(
        patients_df,
        drug_files,
        os.path.join(Config.OUTPUT_DIR, "treatment_groups.feather"),
        params
    )
    
    logger.info("前処理が完了しました")

if __name__ == "__main__":
    main()
