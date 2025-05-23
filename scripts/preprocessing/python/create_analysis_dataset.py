#!/usr/bin/env python3
"""
DeSC-Nalmefene 分析用データセット作成スクリプト

このスクリプトは、抽出されたF10.2患者に対して、
研究計画書で定義された全ての変数を結合し、分析用データセットを作成します。

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
from typing import Dict, List, Set, Optional, Tuple
import psutil
from tqdm import tqdm
import time
from datetime import datetime, timedelta
from utils.env_loader import DATA_ROOT_DIR as ENV_DATA_ROOT_DIR, OUTPUT_DIR as ENV_OUTPUT_DIR

# Create local logs directory before setting up logging
os.makedirs("outputs/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outputs/logs/create_analysis_dataset.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Config:
    DATA_ROOT_DIR = ENV_DATA_ROOT_DIR
    OUTPUT_DIR = ENV_OUTPUT_DIR
    
    # 薬剤コード（修正版：レセコード数値型）
    DRUG_CODES = {
        "nalmefene": 622607601,      # ナルメフェン (飲酒量低減) - 数値型レセコード
        "acamprosate": 622243701,    # アカンプロサート (断酒) - 数値型レセコード
        "disulfiram": 620008676,     # ジスルフィラム (断酒) - 数値型レセコード
        "cyanamide": 621320701         # シアナミド (断酒) - 数値型レセコード
    }
    
    # 併存疾患のICD10コード
    COMORBIDITY_ICD10_CODES = {
        "hypertension": ["I10", "I11", "I12", "I13", "I15"],
        "diabetes": ["E10", "E11", "E12", "E13", "E14"],
        "dyslipidemia": ["E78"],
        "mental_disorders": ["F20", "F21", "F22", "F23", "F24", "F25", "F28", "F29",  # 統合失調症
                           "F30", "F31", "F32", "F33", "F34", "F38", "F39",  # 気分障害
                           "F40", "F41", "F42", "F43", "F44", "F45", "F48"]   # 神経症性障害
    }

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

def load_patient_cohorts(output_dir: str) -> Dict[str, pl.DataFrame]:
    """患者コホートファイルの読み込み"""
    logger.info("患者コホートファイルの読み込みを開始します")
    
    cohort_files = {
        "primary": "f10_2_patients_primary_cohort.feather",
        "sensitivity1": "f10_2_patients_sensitivity_cohort1.feather", 
        "sensitivity2": "f10_2_patients_sensitivity_cohort2.feather",
        "all": "f10_2_patients_all.feather"
    }
    
    cohorts = {}
    for cohort_name, filename in cohort_files.items():
        file_path = os.path.join(output_dir, filename)
        if os.path.exists(file_path):
            cohorts[cohort_name] = pl.read_ipc(file_path)
            logger.info(f"{cohort_name} cohort: {len(cohorts[cohort_name])} 患者")
        else:
            logger.warning(f"コホートファイルが見つかりません: {file_path}")
    
    return cohorts

def load_master_data(base_dir: str) -> Dict[str, pl.DataFrame]:
    """マスターデータの読み込み"""
    logger.info("マスターデータの読み込みを開始します")
    
    master_files = {
        "icd10": "m_icd10.feather",
        "drug_main": "m_drug_main.feather",
        "drug_who_atc": "m_drug_who_atc.feather",
        "hco_med": "m_hco_med.feather",
        "hco_specialty": "m_hco_xref_specialty.feather",
        "disease": "m_disease.feather"
    }
    
    master_data = {}
    for key, filename in tqdm(master_files.items(), desc="マスターファイル読み込み", unit="file"):
        file_path = os.path.join(base_dir, filename)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB単位
            logger.info(f"{filename} (サイズ: {file_size:.2f} MB) を読み込んでいます")
            master_data[key] = pl.read_ipc(file_path)
        else:
            logger.warning(f"{filename}が見つかりません: {file_path}")
    
    return master_data

def get_tekiyo_data(base_dir: str, patient_ids: Set[int]) -> pl.DataFrame:
    """適用（被保険者台帳）データの読み込み"""
    logger.info("適用（被保険者台帳）データの読み込みを開始します")
    
    tekiyo_file = os.path.join(base_dir, "tekiyo.feather")
    if not os.path.exists(tekiyo_file):
        logger.error(f"適用ファイルが見つかりません: {tekiyo_file}")
        return pl.DataFrame()
    
    tekiyo_df = (pl.read_ipc(tekiyo_file)
                .filter(pl.col("kojin_id").is_in(patient_ids))
                .select([
                    "kojin_id",
                    "birth_ym",
                    "sex_code", 
                    "honin_kazoku_code",
                    "kazoku_id",
                    "oyako_id",
                    "insurer_shubetsu",
                    "kazoku_id_riyouka",
                    "oyako_id_riyouka",
                    "kenshin_data_ari",
                    "chiiki_code"
                ]))
    
    logger.info(f"適用データ: {len(tekiyo_df)} レコード")
    return tekiyo_df

def calculate_age_at_index(tekiyo_df: pl.DataFrame, patients_df: pl.DataFrame) -> pl.DataFrame:
    """インデックス日時点での年齢計算"""
    logger.info("インデックス日時点での年齢計算を開始します")
    
    # 患者データと適用データを結合
    merged_df = patients_df.join(tekiyo_df, on="kojin_id", how="left")
    
    # 年齢計算
    age_calculated = merged_df.with_columns([
        # birth_ymとindex_dateから年齢を計算
        pl.when(pl.col("birth_ym").is_not_null())
        .then(
            pl.col("index_date").str.to_date(format="%Y/%m/%d").dt.year() - 
            pl.col("birth_ym").str.slice(0, 4).cast(pl.Int32)
        )
        .otherwise(None)
        .alias("age_at_index")
    ])
    
    return age_calculated

def get_exam_data_time_series(base_dir: str, 
                             patients_df: pl.DataFrame, 
                             params: Dict) -> pl.DataFrame:
    """健診データの時系列取得"""
    logger.info("健診データの時系列取得を開始します")
    
    exam_file = os.path.join(base_dir, "exam_interview_processed.feather")
    if not os.path.exists(exam_file):
        logger.error(f"健診ファイルが見つかりません: {exam_file}")
        return pl.DataFrame()
    
    patient_ids = set(patients_df["kojin_id"].to_list())
    
    # 健診データを読み込み
    exam_df = (pl.read_ipc(exam_file)
              .filter(pl.col("kojin_id").is_in(patient_ids)))
    
    if exam_df.is_empty():
        logger.warning("対象患者の健診データが見つかりませんでした")
        return pl.DataFrame()
    
    # 患者のインデックス日と結合
    exam_with_index = exam_df.join(
        patients_df.select(["kojin_id", "index_date"]),
        on="kojin_id",
        how="inner"
    )
    
    # 日付変換
    exam_with_index = exam_with_index.with_columns([
        pl.col("exam_ymd").str.to_date(format="%Y/%m/%d"),
        pl.col("index_date").str.to_date(format="%Y/%m/%d")
    ])
    
    # 時系列ポイントの定義
    exam_time_series = exam_with_index.with_columns([
        pl.when(
            (pl.col("exam_ymd") < pl.col("index_date")) &
            (pl.col("exam_ymd") >= pl.col("index_date").dt.offset_by("-2y"))
        ).then(pl.lit("before_index"))
        .when(
            (pl.col("exam_ymd") >= pl.col("index_date")) &
            (pl.col("exam_ymd") <= pl.col("index_date").dt.offset_by("6mo"))
        ).then(pl.lit("after_index"))
        .when(
            (pl.col("exam_ymd") >= pl.col("index_date").dt.offset_by("9mo")) &
            (pl.col("exam_ymd") <= pl.col("index_date").dt.offset_by("18mo"))
        ).then(pl.lit("year1"))
        .when(
            (pl.col("exam_ymd") >= pl.col("index_date").dt.offset_by("21mo")) &
            (pl.col("exam_ymd") <= pl.col("index_date").dt.offset_by("30mo"))
        ).then(pl.lit("year2"))
        .otherwise(None)
        .alias("time_point")
    ]).filter(pl.col("time_point").is_not_null())
    
    # 各時点で最も近い健診データを選択
    exam_closest = (exam_time_series
                   .with_columns([
                       pl.when(pl.col("time_point") == "before_index")
                       .then(pl.col("index_date") - pl.col("exam_ymd"))
                       .otherwise(pl.col("exam_ymd") - pl.col("index_date"))
                       .alias("days_diff")
                   ])
                   .sort(["kojin_id", "time_point", "days_diff"])
                   .group_by(["kojin_id", "time_point"])
                   .first())
    
    logger.info(f"健診時系列データ: {len(exam_closest)} レコード")
    return exam_closest

def classify_treatment_groups(patients_df: pl.DataFrame, 
                             base_dir: str,
                             params: Dict) -> pl.DataFrame:
    """治療群の分類"""
    logger.info("治療群の分類を開始します")
    
    patient_ids = set(patients_df["kojin_id"].to_list())
    
    # 薬剤ファイルの取得
    drug_dir = os.path.join(base_dir, "receipt_drug")
    santei_ymd_dir = os.path.join(base_dir, "receipt_drug_santei_ymd")
    
    if not os.path.exists(drug_dir) or not os.path.exists(santei_ymd_dir):
        logger.error("薬剤ファイルディレクトリが見つかりません")
        return patients_df.with_columns(pl.lit(3).alias("treatment_group"))
    
    drug_files = [os.path.join(drug_dir, f) for f in os.listdir(drug_dir) 
                  if f.startswith("receipt_drug_") and f.endswith(".feather")]
    
    treatment_results = []
    
    for file_path in tqdm(drug_files[:5], desc="薬剤ファイル処理", unit="file"):  # テスト用に最初の5ファイル
        try:
            # 薬剤情報を読み込み
            df_drug = (pl.read_ipc(file_path)
                      .filter(pl.col("kojin_id").is_in(patient_ids))
                      .select(["kojin_id", "receipt_id", "line_no", "drug_code"]))
            
            if df_drug.is_empty():
                continue
            
            # 対応する処方日ファイルを読み込み
            base_filename = os.path.basename(file_path)
            santei_filename = base_filename.replace("receipt_drug_", "receipt_drug_santei_ymd_")
            santei_file_path = os.path.join(santei_ymd_dir, santei_filename)
            
            if not os.path.exists(santei_file_path):
                continue
                
            df_santei = (pl.read_ipc(santei_file_path)
                        .filter(pl.col("kojin_id").is_in(patient_ids))
                        .select(["receipt_id", "line_no", "shohou_ymd"]))
            
            # 薬剤情報と処方日を結合
            df_merged = df_drug.join(df_santei, on=["receipt_id", "line_no"], how="inner")
            
            if df_merged.is_empty():
                continue
            
            # 患者のインデックス日と結合
            df_with_index = df_merged.join(
                patients_df.select(["kojin_id", "index_date"]),
                on="kojin_id",
                how="inner"
            )
            
            # 日付変換とフィルタリング（インデックス日から12週以内）
            df_filtered = df_with_index.with_columns([
                pl.col("shohou_ymd").str.to_date(format="%Y/%m/%d"),
                pl.col("index_date").str.to_date(format="%Y/%m/%d")
            ]).filter(
                (pl.col("shohou_ymd") >= pl.col("index_date")) &
                (pl.col("shohou_ymd") <= pl.col("index_date").dt.offset_by("12w"))
            )
            
            if df_filtered.is_empty():
                continue
            
            # 治療群分類
            reduction_codes = [Config.DRUG_CODES["nalmefene"]]
            abstinence_codes = [
                Config.DRUG_CODES["acamprosate"],
                Config.DRUG_CODES["disulfiram"],
                Config.DRUG_CODES["cyanamide"]
            ]
            
            grouped = (df_filtered
                      .with_columns([
                          (pl.col("drug_code").is_in(reduction_codes)).alias("is_reduction"),
                          (pl.col("drug_code").is_in(abstinence_codes)).alias("is_abstinence")
                      ])
                      .group_by("kojin_id")
                      .agg([
                          pl.col("is_reduction").max().alias("has_reduction"),
                          pl.col("is_abstinence").max().alias("has_abstinence"),
                          pl.col("shohou_ymd").min().alias("first_drug_date")
                      ]))
            
            treatment_results.append(grouped)
            
        except Exception as e:
            logger.warning(f"ファイル {file_path} の処理中にエラー: {e}")
            continue
    
    # 治療群の統合と分類
    if treatment_results:
        combined_treatment = pl.concat(treatment_results).unique(subset=["kojin_id"])
        
        classified = (patients_df
                     .join(combined_treatment, on="kojin_id", how="left")
                     .with_columns([
                         pl.when(pl.col("has_reduction") == True)
                         .then(pl.lit(1))  # 飲酒量低減治療群
                         .when(pl.col("has_abstinence") == True)
                         .then(pl.lit(2))  # 断酒治療群
                         .otherwise(pl.lit(3))  # 治療目標不明群
                         .alias("treatment_group")
                     ]))
    else:
        classified = patients_df.with_columns(pl.lit(3).alias("treatment_group"))
    
    # 治療群分布の表示
    treatment_counts = (classified
                       .group_by("treatment_group")
                       .count()
                       .sort("treatment_group"))
    
    logger.info("治療群分布:")
    for row in treatment_counts.iter_rows():
        group, count = row
        group_name = {1: "飲酒量低減治療群", 2: "断酒治療群", 3: "治療目標不明群"}.get(group, "不明")
        logger.info(f"  {group_name}: {count} 人")
    
    return classified

def get_comorbidities(base_dir: str, 
                     patients_df: pl.DataFrame,
                     master_data: Dict[str, pl.DataFrame],
                     params: Dict) -> pl.DataFrame:
    """併存疾患の取得"""
    logger.info("併存疾患の取得を開始します")
    
    patient_ids = set(patients_df["kojin_id"].to_list())
    
    # ICD10マスターから併存疾患のレセプト病名コードを取得
    icd10_master = master_data.get("icd10")
    if icd10_master is None:
        logger.error("ICD10マスターデータがありません")
        return pl.DataFrame()
    
    comorbidity_codes = {}
    for disease, icd10_codes in Config.COMORBIDITY_ICD10_CODES.items():
        disease_codes = []
        for icd10_code in icd10_codes:
            codes = (icd10_master
                    .filter(
                        (pl.col("icd10_code").str.starts_with(icd10_code)) &
                        (pl.col("icd10_kbn_code") == "1")
                    )
                    .select("diseases_code")
                    .to_series()
                    .to_list())
            disease_codes.extend(codes)
        comorbidity_codes[disease] = list(set(disease_codes))
        logger.info(f"{disease}: {len(comorbidity_codes[disease])} コード")
    
    # 疾患ファイルから併存疾患を検索
    disease_dir = os.path.join(base_dir, "receipt_diseases")
    disease_files = [os.path.join(disease_dir, f) for f in os.listdir(disease_dir)
                    if f.startswith("receipt_diseases") and f.endswith(".feather")]
    
    comorbidity_results = []
    
    for file_path in tqdm(disease_files[:3], desc="併存疾患検索", unit="file"):  # テスト用
        try:
            df_diseases = (pl.read_ipc(file_path)
                          .filter(pl.col("kojin_id").is_in(patient_ids)))
            
            if df_diseases.is_empty():
                continue
            
            # 各併存疾患の有無をチェック
            for disease, codes in comorbidity_codes.items():
                if codes:
                    disease_patients = (df_diseases
                                      .filter(pl.col("diseases_code").is_in(codes))
                                      .select("kojin_id")
                                      .unique()
                                      .with_columns(pl.lit(True).alias(f"has_{disease}")))
                    comorbidity_results.append(disease_patients)
                    
        except Exception as e:
            logger.warning(f"併存疾患検索中にエラー: {e}")
            continue
    
    # 併存疾患データの統合
    if comorbidity_results:
        combined_comorbidities = comorbidity_results[0]
        for result in comorbidity_results[1:]:
            combined_comorbidities = combined_comorbidities.join(
                result, on="kojin_id", how="outer", coalesce=True
            )
        
        # 患者データと結合
        patients_with_comorbidities = patients_df.join(
            combined_comorbidities, on="kojin_id", how="left"
        )
        
        # 欠損値をFalseで埋める
        for disease in Config.COMORBIDITY_ICD10_CODES.keys():
            col_name = f"has_{disease}"
            if col_name in patients_with_comorbidities.columns:
                patients_with_comorbidities = patients_with_comorbidities.with_columns(
                    pl.col(col_name).fill_null(False)
                )
    else:
        patients_with_comorbidities = patients_df
        for disease in Config.COMORBIDITY_ICD10_CODES.keys():
            patients_with_comorbidities = patients_with_comorbidities.with_columns(
                pl.lit(False).alias(f"has_{disease}")
            )
    
    return patients_with_comorbidities

def create_analysis_datasets(cohorts: Dict[str, pl.DataFrame],
                           base_dir: str,
                           output_dir: str,
                           params: Dict):
    """分析用データセットの作成"""
    logger.info("分析用データセットの作成を開始します")
    
    # マスターデータの読み込み
    master_data = load_master_data(base_dir)
    
    for cohort_name, patients_df in cohorts.items():
        if patients_df.is_empty():
            continue
            
        logger.info(f"\n=== {cohort_name.upper()} COHORT の処理開始 ===")
        
        patient_ids = set(patients_df["kojin_id"].to_list())
        
        # 1. 基本情報（適用データ）の結合
        tekiyo_df = get_tekiyo_data(base_dir, patient_ids)
        if not tekiyo_df.is_empty():
            patients_with_demo = calculate_age_at_index(tekiyo_df, patients_df)
        else:
            patients_with_demo = patients_df
        
        # 2. 治療群の分類
        patients_with_treatment = classify_treatment_groups(patients_with_demo, base_dir, params)
        
        # 3. 併存疾患の取得
        patients_with_comorbidities = get_comorbidities(base_dir, patients_with_treatment, master_data, params)
        
        # 4. 健診データの時系列取得
        exam_time_series = get_exam_data_time_series(base_dir, patients_with_comorbidities, params)
        
        # 5. ベースラインデータセットの保存
        baseline_output_path = os.path.join(output_dir, f"{cohort_name}_cohort_baseline.feather")
        patients_with_comorbidities.write_ipc(baseline_output_path, compression="zstd")
        logger.info(f"{cohort_name} cohort ベースラインデータを保存: {baseline_output_path}")
        
        # 6. 時系列データセットの保存
        if not exam_time_series.is_empty():
            longitudinal_output_path = os.path.join(output_dir, f"{cohort_name}_cohort_longitudinal.feather")
            exam_time_series.write_ipc(longitudinal_output_path, compression="zstd")
            logger.info(f"{cohort_name} cohort 時系列データを保存: {longitudinal_output_path}")
        
        logger.info(f"{cohort_name} cohort 処理完了: {len(patients_with_comorbidities)} 患者")

def main():
    """メイン処理"""
    logger.info("DeSC-Nalmefene 分析用データセット作成を開始します")
    
    params = optimize_parameters()
    logger.info(f"最適化パラメータ: {params}")
    
    # 患者コホートの読み込み
    cohorts = load_patient_cohorts(Config.OUTPUT_DIR)
    if not cohorts:
        logger.error("患者コホートファイルが見つかりません")
        return
    
    # 分析用データセットの作成
    create_analysis_datasets(cohorts, Config.DATA_ROOT_DIR, Config.OUTPUT_DIR, params)
    
    logger.info("分析用データセット作成が完了しました")

if __name__ == "__main__":
    main()
