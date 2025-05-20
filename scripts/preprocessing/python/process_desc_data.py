#!/usr/bin/env python3
"""
DeSC-Nalmefene データ前処理スクリプト

このスクリプトは、DeSCデータベースの.featherファイルを読み込み、
解析用の中間データセットを作成します。

作成者: Devin
作成日: 2023-05-20
"""

import os
import logging
import polars as pl
from pathlib import Path
import gc
from typing import Dict, List, Set, Optional
import psutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outputs/logs/preprocessing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Config:
    DATA_ROOT_DIR = os.environ.get('DATA_ROOT_DIR', '/path/to/desc/data')
    OUTPUT_DIR = os.environ.get('OUTPUT_DIR', 'data/interim')
    
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
        "drug": "m_drug.feather"
    }
    
    master_data = {}
    for key, filename in master_files.items():
        file_path = os.path.join(base_dir, filename)
        if os.path.exists(file_path):
            logger.info(f"{filename}を読み込んでいます")
            master_data[key] = pl.read_ipc(file_path)
        else:
            logger.warning(f"{filename}が見つかりません: {file_path}")
    
    return master_data

def identify_f10_2_patients(disease_files: List[str], 
                           output_path: str,
                           params: Dict) -> pl.DataFrame:
    """F10.2（アルコール依存症）患者の特定"""
    logger.info("F10.2患者の特定を開始します")
    
    results = []
    
    for file_path in disease_files:
        df_lazy = (pl.scan_ipc(
            source=file_path,
            memory_map=True,
            n_rows=params['chunk_size']
        )
        .filter(pl.col("diseases_code").str.contains(Config.F10_2_CODE))
        .select([
            "kojin_id",
            "receipt_id",
            "receipt_ym",
            "diseases_code",
            "sinryo_start_ymd"
        ]))
        
        result = df_lazy.collect(streaming=True, n_threads=params['n_threads'])
        
        if not result.is_empty():
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
    
    return index_dates

def classify_treatment_groups(patient_df: pl.DataFrame, 
                             drug_files: List[str],
                             output_path: str,
                             params: Dict) -> pl.DataFrame:
    """治療群の分類 (飲酒量低減群、断酒群、治療目標不明群)"""
    logger.info("治療群の分類を開始します")
    
    patient_ids = set(patient_df["kojin_id"].to_list())
    
    treatment_groups = []
    
    for file_path in drug_files:
        df_lazy = (pl.scan_ipc(
            source=file_path,
            memory_map=True,
            n_rows=params['chunk_size']
        )
        .filter(pl.col("kojin_id").is_in(patient_ids))
        .select([
            "kojin_id",
            "receipt_id",
            "receipt_ym",
            "drug_code",
            "drug_ymd"
        ]))
        
        df = df_lazy.collect(streaming=True, n_threads=params['n_threads'])
        
        if not df.is_empty():
            reduction_codes = [Config.DRUG_CODES["nalmefene"]]
            abstinence_codes = [
                Config.DRUG_CODES["acamprosate"],
                Config.DRUG_CODES["disulfiram"],
                Config.DRUG_CODES["cyanamide"]
            ]
            
            merged_df = df.join(
                patient_df.select(["kojin_id", "index_date"]),
                on="kojin_id",
                how="inner"
            )
            
            merged_df = merged_df.filter(
                (pl.col("drug_ymd") >= pl.col("index_date")) &
                (pl.col("drug_ymd") <= pl.date_add(pl.col("index_date"), pl.lit(12), pl.lit("weeks")))
            )
            
            if not merged_df.is_empty():
                grouped = (merged_df
                    .with_columns([
                        (pl.col("drug_code").is_in(reduction_codes)).alias("is_reduction"),
                        (pl.col("drug_code").is_in(abstinence_codes)).alias("is_abstinence")
                    ])
                    .sort(["kojin_id", "drug_ymd"])
                    .group_by("kojin_id")
                    .agg([
                        pl.col("is_reduction").max().alias("has_reduction"),
                        pl.col("is_abstinence").max().alias("has_abstinence"),
                        pl.col("drug_ymd").first().alias("first_drug_date")
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
        
        return classified

def main():
    """メイン処理"""
    logger.info("DeSC-Nalmefene データ前処理を開始します")
    
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    os.makedirs("outputs/logs", exist_ok=True)
    
    params = optimize_parameters()
    logger.info(f"最適化パラメータ: {params}")
    
    master_data = load_master_data(Config.DATA_ROOT_DIR)
    
    disease_files = [os.path.join(Config.DATA_ROOT_DIR, f) 
                   for f in os.listdir(Config.DATA_ROOT_DIR) 
                   if f.startswith("receipt_disease") and f.endswith(".feather")]
    
    patients_df = identify_f10_2_patients(
        disease_files,
        os.path.join(Config.OUTPUT_DIR, "f10_2_patients.feather"),
        params
    )
    
    if patients_df.is_empty():
        logger.error("F10.2患者が見つからないため処理を終了します")
        return
    
    drug_files = [os.path.join(Config.DATA_ROOT_DIR, f) 
                for f in os.listdir(Config.DATA_ROOT_DIR) 
                if f.startswith("receipt_drug") and f.endswith(".feather")]
    
    treatment_groups = classify_treatment_groups(
        patients_df,
        drug_files,
        os.path.join(Config.OUTPUT_DIR, "treatment_groups.feather"),
        params
    )
    
    logger.info("前処理が完了しました")

if __name__ == "__main__":
    main()
