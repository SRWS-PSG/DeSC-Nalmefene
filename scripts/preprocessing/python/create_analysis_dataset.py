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
    level=logging.DEBUG,  # ログレベルをDEBUGに変更
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
    logger.debug("optimize_parameters: 開始")
    n_threads = max(1, int(psutil.cpu_count(logical=True) * 0.75))
    logger.debug(f"optimize_parameters: n_threads = {n_threads}")
    
    available_memory = psutil.virtual_memory().available
    logger.debug(f"optimize_parameters: available_memory = {available_memory}")
    chunk_size = min(500_000, int(available_memory * 0.3 / 1024))
    logger.debug(f"optimize_parameters: chunk_size = {chunk_size}")
    
    batch_size = n_threads * 2
    logger.debug(f"optimize_parameters: batch_size = {batch_size}")
    
    optimized_params = {
        'n_threads': n_threads,
        'chunk_size': chunk_size,
        'batch_size': batch_size
    }
    logger.debug(f"optimize_parameters: 戻り値 = {optimized_params}")
    logger.debug("optimize_parameters: 終了")
    return optimized_params

def load_patient_cohorts(output_dir: str) -> Dict[str, pl.DataFrame]:
    """患者コホートファイルの読み込み"""
    logger.info("患者コホートファイルの読み込みを開始します")
    logger.debug(f"load_patient_cohorts: output_dir = {output_dir}")
    
    cohort_files = {
        "primary": "f10_2_patients_primary_cohort.feather",
        "sensitivity1": "f10_2_patients_sensitivity_cohort1.feather",
        "sensitivity2": "f10_2_patients_sensitivity_cohort2.feather",
        "all": "f10_2_patients_all.feather"
    }
    logger.debug(f"load_patient_cohorts: cohort_files = {cohort_files}")
    
    cohorts = {}
    for cohort_name, filename in cohort_files.items():
        logger.debug(f"load_patient_cohorts: cohort_name = {cohort_name}, filename = {filename}")
        file_path = os.path.join(output_dir, filename)
        logger.debug(f"load_patient_cohorts: file_path = {file_path}")
        if os.path.exists(file_path):
            logger.debug(f"load_patient_cohorts: {file_path} が存在します。読み込みます。")
            cohorts[cohort_name] = pl.read_ipc(file_path)
            logger.info(f"{cohort_name} cohort: {len(cohorts[cohort_name])} 患者")
            logger.debug(f"load_patient_cohorts: {cohort_name} の患者数 = {len(cohorts[cohort_name])}")
        else:
            logger.warning(f"コホートファイルが見つかりません: {file_path}")
    
    logger.debug(f"load_patient_cohorts: 戻り値 cohorts のキー = {list(cohorts.keys())}")
    logger.info("患者コホートファイルの読み込みを終了します")
    return cohorts

def load_master_data(base_dir: str) -> Dict[str, pl.DataFrame]:
    """マスターデータの読み込み"""
    logger.info("マスターデータの読み込みを開始します")
    logger.debug(f"load_master_data: base_dir = {base_dir}")
    
    master_files = {
        "icd10": "m_icd10.feather",
        "drug_main": "m_drug_main.feather",
        "drug_who_atc": "m_drug_who_atc.feather",
        "hco_med": "m_hco_med.feather",
        "hco_specialty": "m_hco_xref_specialty.feather",
        "disease": "m_disease.feather"
    }
    logger.debug(f"load_master_data: master_files = {master_files}")
    
    master_data = {}
    for key, filename in tqdm(master_files.items(), desc="マスターファイル読み込み", unit="file"):
        logger.debug(f"load_master_data: key = {key}, filename = {filename}")
        file_path = os.path.join(base_dir, filename)
        logger.debug(f"load_master_data: file_path = {file_path}")
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB単位
            logger.info(f"{filename} (サイズ: {file_size:.2f} MB) を読み込んでいます")
            logger.debug(f"load_master_data: {file_path} が存在します。読み込みます。サイズ = {file_size:.2f} MB")
            master_data[key] = pl.read_ipc(file_path)
            logger.debug(f"load_master_data: {key} の読み込み完了")
        else:
            logger.warning(f"{filename}が見つかりません: {file_path}")
            
    logger.debug(f"load_master_data: 戻り値 master_data のキー = {list(master_data.keys())}")
    logger.info("マスターデータの読み込みを終了します")
    return master_data

def get_tekiyo_data(base_dir: str, patient_ids: Set[int]) -> pl.DataFrame:
    """適用（被保険者台帳）データの読み込み"""
    logger.info("適用（被保険者台帳）データの読み込みを開始します")
    logger.debug(f"get_tekiyo_data: base_dir = {base_dir}, patient_ids 数 = {len(patient_ids)}")
    
    tekiyo_file = os.path.join(base_dir, "tekiyo.feather")
    logger.debug(f"get_tekiyo_data: tekiyo_file = {tekiyo_file}")
    if not os.path.exists(tekiyo_file):
        logger.error(f"適用ファイルが見つかりません: {tekiyo_file}")
        logger.debug("get_tekiyo_data: 適用ファイルが存在しないため空のDataFrameを返します")
        return pl.DataFrame()
    
    logger.debug("get_tekiyo_data: 適用ファイルを読み込み、フィルタリングと選択を行います")
    tekiyo_df = (pl.read_ipc(tekiyo_file)
                .filter(pl.col("kojin_id").is_in(list(patient_ids))) # SetをListに変換
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
    logger.debug(f"get_tekiyo_data: 読み込んだ適用データ数 = {len(tekiyo_df)}")
    logger.debug("get_tekiyo_data: 終了")
    return tekiyo_df

def calculate_age_at_index(tekiyo_df: pl.DataFrame, patients_df: pl.DataFrame) -> pl.DataFrame:
    """インデックス日時点での年齢計算"""
    logger.info("インデックス日時点での年齢計算を開始します")
    logger.debug(f"calculate_age_at_index: tekiyo_df shape = {tekiyo_df.shape}, patients_df shape = {patients_df.shape}")
    
    logger.debug("calculate_age_at_index: 患者データと適用データを結合します")
    merged_df = patients_df.join(tekiyo_df, on="kojin_id", how="left")
    logger.debug(f"calculate_age_at_index: 結合後の merged_df shape = {merged_df.shape}")
    
    logger.debug("calculate_age_at_index: 年齢計算を行います")
    age_calculated = merged_df.with_columns([
        pl.when(pl.col("birth_ym").is_not_null())
        .then(
            pl.col("index_date").str.to_date(format="%Y/%m/%d").dt.year() -
            pl.col("birth_ym").str.slice(0, 4).cast(pl.Int32)
        )
        .otherwise(None)
        .alias("age_at_index")
    ])
    logger.debug(f"calculate_age_at_index: 年齢計算後の age_calculated shape = {age_calculated.shape}")
    
    logger.debug("calculate_age_at_index: 終了")
    return age_calculated

def get_exam_data_time_series(base_dir: str,
                             patients_df: pl.DataFrame,
                             params: Dict) -> pl.DataFrame:
    """健診データの時系列取得"""
    logger.info("健診データの時系列取得を開始します")
    logger.debug(f"get_exam_data_time_series: base_dir = {base_dir}, patients_df shape = {patients_df.shape}, params = {params}")
    
    exam_file = os.path.join(base_dir, "exam_interview_processed.feather")
    logger.debug(f"get_exam_data_time_series: exam_file = {exam_file}")
    if not os.path.exists(exam_file):
        logger.error(f"健診ファイルが見つかりません: {exam_file}")
        logger.debug("get_exam_data_time_series: 健診ファイルが存在しないため空のDataFrameを返します")
        return pl.DataFrame()
    
    patient_ids = set(patients_df["kojin_id"].to_list())
    logger.debug(f"get_exam_data_time_series: patient_ids 数 = {len(patient_ids)}")
    
    logger.debug("get_exam_data_time_series: 健診データを読み込み、フィルタリングします")
    exam_df = (pl.read_ipc(exam_file)
              .filter(pl.col("kojin_id").is_in(list(patient_ids)))) # SetをListに変換
    logger.debug(f"get_exam_data_time_series: 読み込んだ健診データ数 = {len(exam_df)}")
    
    if exam_df.is_empty():
        logger.warning("対象患者の健診データが見つかりませんでした")
        logger.debug("get_exam_data_time_series: 対象患者の健診データが空のため空のDataFrameを返します")
        return pl.DataFrame()
    
    logger.debug("get_exam_data_time_series: 患者のインデックス日と結合します")
    exam_with_index = exam_df.join(
        patients_df.select(["kojin_id", "index_date"]),
        on="kojin_id",
        how="inner"
    )
    logger.debug(f"get_exam_data_time_series: インデックス日結合後の exam_with_index shape = {exam_with_index.shape}")
    
    logger.debug("get_exam_data_time_series: 日付変換を行います")
    exam_with_index = exam_with_index.with_columns([
        pl.col("exam_ymd").str.to_date(format="%Y/%m/%d"),
        pl.col("index_date").str.to_date(format="%Y/%m/%d")
    ])
    
    logger.debug("get_exam_data_time_series: 時系列ポイントの定義を行います")
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
    logger.debug(f"get_exam_data_time_series: 時系列ポイント定義後の exam_time_series shape = {exam_time_series.shape}")
    
    logger.debug("get_exam_data_time_series: 各時点で最も近い健診データを選択します")
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
    logger.debug(f"get_exam_data_time_series: 最も近い健診データ選択後の exam_closest shape = {exam_closest.shape}")
    
    logger.info(f"健診時系列データ: {len(exam_closest)} レコード")
    logger.debug("get_exam_data_time_series: 終了")
    return exam_closest

def classify_treatment_groups(patients_df: pl.DataFrame,
                             base_dir: str,
                             params: Dict) -> pl.DataFrame:
    """治療群の分類"""
    logger.info("治療群の分類を開始します")
    logger.debug(f"classify_treatment_groups: patients_df shape = {patients_df.shape}, base_dir = {base_dir}, params = {params}")
    
    patient_ids = set(patients_df["kojin_id"].to_list())
    logger.debug(f"classify_treatment_groups: patient_ids 数 = {len(patient_ids)}")
    
    drug_dir = os.path.join(base_dir, "receipt_drug")
    santei_ymd_dir = os.path.join(base_dir, "receipt_drug_santei_ymd")
    logger.debug(f"classify_treatment_groups: drug_dir = {drug_dir}, santei_ymd_dir = {santei_ymd_dir}")
    
    if not os.path.exists(drug_dir) or not os.path.exists(santei_ymd_dir):
        logger.error("薬剤ファイルディレクトリが見つかりません")
        logger.debug("classify_treatment_groups: 薬剤ファイルディレクトリが存在しないため、全患者を治療目標不明群(3)として返します")
        return patients_df.with_columns(pl.lit(3).alias("treatment_group"))
    
    drug_files_all = [os.path.join(drug_dir, f) for f in os.listdir(drug_dir)
                      if f.startswith("receipt_drug_") and f.endswith(".feather")]
    logger.debug(f"classify_treatment_groups: 発見された薬剤ファイル数 = {len(drug_files_all)}")
    
    # ファイル名から年月を抽出し、それでソートして最新のものを選択
    def get_yyyymm_from_filename(filename):
        # filename is like 'receipt_drug_YYYYMM.feather'
        basename = os.path.basename(filename)
        # Extract YYYYMM part, assuming it's always 6 digits before '.feather'
        yyyymm_str = basename.split('_')[-1].split('.')[0]
        try:
            return int(yyyymm_str)
        except ValueError:
            logger.warning(f"Could not parse YYYYMM from filename: {filename}. Treating as oldest.")
            return 0 # Treat unparseable names as oldest

    drug_files_all.sort(key=get_yyyymm_from_filename, reverse=True)
    # drug_files_to_process = drug_files_all[:3] # 最新3ファイルを処理対象とする -> 全てのファイルに変更
    drug_files_to_process = drug_files_all # 全ての薬剤ファイルを処理対象とする
    logger.debug(f"classify_treatment_groups: 処理対象の薬剤ファイル数 = {len(drug_files_to_process)}")

    treatment_results = []
    
    for file_path in tqdm(drug_files_to_process, desc="薬剤ファイル処理", unit="file"):
        logger.debug(f"classify_treatment_groups: 薬剤ファイル処理中: {file_path}")
        try:
            logger.debug(f"classify_treatment_groups: {file_path} を読み込みます")
            df_drug_raw = pl.read_ipc(file_path)
            
            # kojin_id を文字列型にキャストしてからフィルタリング
            logger.debug(f"classify_treatment_groups: {file_path} の kojin_id を文字列型にキャストし、フィルタリングします")
            df_drug = (df_drug_raw
                      .with_columns(pl.col("kojin_id").cast(pl.String)) # 文字列型にキャスト
                      .filter(pl.col("kojin_id").is_in(list(patient_ids))) 
                      .select(["kojin_id", "receipt_id", "line_no", "drug_code"]))
            logger.debug(f"classify_treatment_groups: {file_path} の薬剤データ数 (フィルタ後) = {len(df_drug)}")
            
            if df_drug.is_empty():
                logger.debug(f"classify_treatment_groups: {file_path} の薬剤データが空のためスキップします")
                continue
            
            base_filename = os.path.basename(file_path)
            santei_filename = base_filename.replace("receipt_drug_", "receipt_drug_santei_ymd_")
            santei_file_path = os.path.join(santei_ymd_dir, santei_filename)
            logger.debug(f"classify_treatment_groups: 対応する算定日ファイル: {santei_file_path}")
            
            if not os.path.exists(santei_file_path):
                logger.warning(f"classify_treatment_groups: 算定日ファイルが見つかりません: {santei_file_path}。スキップします。")
                continue
            
            logger.debug(f"classify_treatment_groups: {santei_file_path} を読み込み、フィルタリングし、データ型を調整します")
            df_santei = (pl.read_ipc(santei_file_path)
                        .with_columns([ # kojin_id, receipt_id, line_no を適切な型にキャスト
                            pl.col("kojin_id").cast(pl.String),
                            pl.col("receipt_id").cast(pl.Int64), # df_drug側がInt64であると仮定 (エラーメッセージより)
                            pl.col("line_no").cast(pl.Int64)     # df_drug側がInt64であると仮定
                        ])
                        .filter(pl.col("kojin_id").is_in(list(patient_ids)))
                        .select(["receipt_id", "line_no", "shohou_ymd"]))
            logger.debug(f"classify_treatment_groups: {santei_file_path} の算定日データ数 = {len(df_santei)}")
            
            logger.debug("classify_treatment_groups: 薬剤情報と処方日を結合します")
            df_merged = df_drug.join(df_santei, on=["receipt_id", "line_no"], how="inner")
            logger.debug(f"classify_treatment_groups: 結合後の df_merged shape = {df_merged.shape}")
            
            if df_merged.is_empty():
                logger.debug("classify_treatment_groups: 結合後のデータが空のためスキップします")
                continue
            
            logger.debug("classify_treatment_groups: 患者のインデックス日と結合します")
            df_with_index = df_merged.join(
                patients_df.select(["kojin_id", "index_date"]),
                on="kojin_id",
                how="inner"
            )
            logger.debug(f"classify_treatment_groups: インデックス日結合後の df_with_index shape = {df_with_index.shape}")
            
            logger.debug("classify_treatment_groups: 日付変換とフィルタリング（インデックス日から12週以内）を行います")
            df_filtered = df_with_index.with_columns([
                pl.col("shohou_ymd").str.to_date(format="%Y/%m/%d"),
                pl.col("index_date").str.to_date(format="%Y/%m/%d")
            ]).filter(
                (pl.col("shohou_ymd") >= pl.col("index_date")) &
                (pl.col("shohou_ymd") <= pl.col("index_date").dt.offset_by("52w")) # 120週から52週に変更
            )
            logger.debug(f"classify_treatment_groups: フィルタリング後の df_filtered shape = {df_filtered.shape}")
            
            if df_filtered.is_empty():
                logger.debug("classify_treatment_groups: フィルタリング後のデータが空のためスキップします")
                continue
            
            logger.debug("classify_treatment_groups: 治療群分類の準備をします")
            reduction_codes = [Config.DRUG_CODES["nalmefene"]]
            abstinence_codes = [
                Config.DRUG_CODES["acamprosate"],
                Config.DRUG_CODES["disulfiram"],
                Config.DRUG_CODES["cyanamide"]
            ]
            logger.debug(f"classify_treatment_groups: reduction_codes = {reduction_codes}, abstinence_codes = {abstinence_codes}")
            
            logger.debug("classify_treatment_groups: 治療群の集計を行います")
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
            logger.debug(f"classify_treatment_groups: 集計後の grouped shape = {grouped.shape}")
            
            treatment_results.append(grouped)
            logger.debug(f"classify_treatment_groups: treatment_results に {len(grouped)} 件の結果を追加しました")
            
        except Exception as e:
            logger.warning(f"ファイル {file_path} の処理中にエラー: {e}")
            logger.exception(f"classify_treatment_groups: エラー詳細:")
            continue
    
    logger.debug(f"classify_treatment_groups: 薬剤ファイル処理ループ終了。treatment_results の要素数 = {len(treatment_results)}")
    if treatment_results:
        logger.debug("classify_treatment_groups: 治療群の統合と最終分類を行います")
        combined_treatment = pl.concat(treatment_results).unique(subset=["kojin_id"])
        logger.debug(f"classify_treatment_groups: 統合後の combined_treatment shape = {combined_treatment.shape}")
        
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
        logger.debug(f"classify_treatment_groups: 最終分類後の classified shape = {classified.shape}")
    else:
        logger.warning("classify_treatment_groups: 処理可能な薬剤データがなかったため、全患者を治療目標不明群(3)とします")
        classified = patients_df.with_columns(pl.lit(3).alias("treatment_group"))
    
    logger.debug("classify_treatment_groups: 治療群分布の表示準備")
    treatment_counts = (classified
                       .group_by("treatment_group")
                       .count()
                       .sort("treatment_group"))
    
    logger.info("治療群分布:")
    for row in treatment_counts.iter_rows():
        group, count = row
        group_name = {1: "飲酒量低減治療群", 2: "断酒治療群", 3: "治療目標不明群"}.get(group, "不明")
        logger.info(f"  {group_name}: {count} 人")
        logger.debug(f"classify_treatment_groups: 治療群 {group_name} ({group}): {count} 人")
    
    logger.debug("classify_treatment_groups: 終了")
    return classified

def get_comorbidities(base_dir: str,
                     patients_df: pl.DataFrame,
                     master_data: Dict[str, pl.DataFrame],
                     params: Dict) -> pl.DataFrame:
    """併存疾患の取得"""
    logger.info("併存疾患の取得を開始します")
    logger.debug(f"get_comorbidities: base_dir = {base_dir}, patients_df shape = {patients_df.shape}, master_data keys = {list(master_data.keys())}, params = {params}")
    
    patient_ids = set(patients_df["kojin_id"].to_list())
    logger.debug(f"get_comorbidities: patient_ids 数 = {len(patient_ids)}")
    
    icd10_master = master_data.get("icd10")
    if icd10_master is None:
        logger.error("ICD10マスターデータがありません")
        logger.debug("get_comorbidities: ICD10マスターが存在しないため、併存疾患なしとして元のDataFrameを返します")
        # 元のDataFrameに併存疾患カラムを追加して返す方が後続処理でエラーになりにくい
        patients_with_comorbidities = patients_df.clone() # cloneして元のDFに影響を与えないようにする
        for disease_key in Config.COMORBIDITY_ICD10_CODES.keys():
            patients_with_comorbidities = patients_with_comorbidities.with_columns(pl.lit(False).alias(f"has_{disease_key}"))
        return patients_with_comorbidities

    logger.debug("get_comorbidities: ICD10マスターから併存疾患のレセプト病名コードを取得します")
    comorbidity_codes = {}
    for disease, icd10_codes_list in Config.COMORBIDITY_ICD10_CODES.items():
        logger.debug(f"get_comorbidities: 併存疾患 '{disease}' のICD10コードリスト: {icd10_codes_list}")
        disease_specific_codes = []
        for icd10_code_prefix in icd10_codes_list:
            logger.debug(f"get_comorbidities: ICD10コードプレフィックス '{icd10_code_prefix}' で検索")
            codes = (icd10_master
                    .filter(
                        (pl.col("icd10_code").str.starts_with(icd10_code_prefix)) &
                        (pl.col("icd10_kbn_code") == "1") # 標準病名マスターのみ
                    )
                    .select("diseases_code")
                    .to_series()
                    .to_list())
            logger.debug(f"get_comorbidities: '{icd10_code_prefix}' に一致する diseases_code 数: {len(codes)}")
            disease_specific_codes.extend(codes)
        comorbidity_codes[disease] = list(set(disease_specific_codes)) # 重複除去
        logger.info(f"{disease}: {len(comorbidity_codes[disease])} コード")
        logger.debug(f"get_comorbidities: 併存疾患 '{disease}' の diseases_code 数 (重複除去後): {len(comorbidity_codes[disease])}")
    
    disease_dir = os.path.join(base_dir, "receipt_diseases")
    logger.debug(f"get_comorbidities: disease_dir = {disease_dir}")
    
    all_disease_files = [
        os.path.join(disease_dir, f)
        for f in os.listdir(disease_dir)
        if f.startswith("receipt_diseases") and f.endswith(".feather")
    ]
    logger.debug(f"get_comorbidities: 発見された全疾患ファイル数 = {len(all_disease_files)}")

    # ファイルを最終更新日時でソートし、最新の3ファイルを選択
    all_disease_files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    disease_files_to_process = all_disease_files[:3]
    logger.info(f"処理対象の疾患ファイル (最新3件): {[os.path.basename(f) for f in disease_files_to_process]}")
    logger.debug(f"get_comorbidities: 処理対象の疾患ファイル数 = {len(disease_files_to_process)}")

    comorbidity_results = []
    
    for file_path in tqdm(disease_files_to_process, desc="併存疾患検索", unit="file"):
        logger.debug(f"get_comorbidities: 併存疾患検索中: {file_path}")
        try:
            logger.debug(f"get_comorbidities: {file_path} を読み込み、フィルタリングします")
            df_diseases = (pl.read_ipc(file_path)
                          .filter(pl.col("kojin_id").is_in(list(patient_ids)))) # SetをListに変換
            logger.debug(f"get_comorbidities: {file_path} の疾患データ数 = {len(df_diseases)}")
            
            if df_diseases.is_empty():
                logger.debug(f"get_comorbidities: {file_path} の疾患データが空のためスキップします")
                continue
            
            # 各併存疾患の有無をチェック
            for disease, codes_list in comorbidity_codes.items():
                logger.debug(f"get_comorbidities: 併存疾患 '{disease}' の有無をチェック (コード数: {len(codes_list)})")
                if codes_list: # コードリストが空でない場合のみ処理
                    disease_patients = (df_diseases
                                      .filter(pl.col("diseases_code").is_in(codes_list))
                                      .select("kojin_id")
                                      .unique()
                                      .with_columns(pl.lit(True).alias(f"has_{disease}")))
                    logger.debug(f"get_comorbidities: '{disease}' を持つ患者数 (このファイル内): {len(disease_patients)}")
                    if not disease_patients.is_empty(): # 結果が空でなければ追加
                        comorbidity_results.append(disease_patients)
                        logger.debug(f"get_comorbidities: comorbidity_results に '{disease}' の結果を追加")
                    
        except Exception as e:
            logger.warning(f"併存疾患検索中にエラー ({file_path}): {e}")
            logger.exception(f"get_comorbidities: エラー詳細:")
            continue
    
    logger.debug(f"get_comorbidities: 疾患ファイル処理ループ終了。comorbidity_results の要素数 = {len(comorbidity_results)}")
    
    # 患者データに併存疾患フラグを結合
    patients_with_comorbidities = patients_df.clone() # 元のDFを汚さないようにコピー

    # まず全ての併存疾患カラムをFalseで初期化
    for disease_key in Config.COMORBIDITY_ICD10_CODES.keys():
        patients_with_comorbidities = patients_with_comorbidities.with_columns(
            pl.lit(False).alias(f"has_{disease_key}")
        )
    logger.debug(f"get_comorbidities: 全ての併存疾患カラムをFalseで初期化完了。カラム: {patients_with_comorbidities.columns}")


    if comorbidity_results:
        logger.debug("get_comorbidities: 併存疾患データの統合を開始します")
        # 最初に全患者IDを持つDataFrameを作成し、それに各疾患の有無をleft joinしていく
        # これにより、いずれかのファイルにしか登場しない患者も正しく処理される
        
        # 統合用のベースとなるDataFrame (kojin_idのみ)
        # ここでは、patients_dfからkojin_idを使い、それに各疾患情報をマージしていく
        # 既にpatients_with_comorbiditiesはpatients_dfのコピーで、has_xxxカラムがFalseで初期化されているのでこれを使う

        # 各疾患の結果を統合
        # comorbidity_resultsには、各ファイル・各疾患で見つかった患者の (kojin_id, has_disease=True) のDFが入っている
        # これらを疾患ごとに集約し、最終的にpatients_with_comorbiditiesにマージする

        # 疾患ごとの結果を一時的に格納する辞書
        aggregated_disease_dfs: Dict[str, pl.DataFrame] = {}

        for res_df in comorbidity_results:
            # res_dfは (kojin_id, has_DISEASE_NAME=True) の形
            disease_col_name = [col for col in res_df.columns if col.startswith("has_")][0]
            
            if disease_col_name not in aggregated_disease_dfs:
                aggregated_disease_dfs[disease_col_name] = res_df
            else:
                # 既に同じ疾患の結果がある場合はconcatしてunique
                aggregated_disease_dfs[disease_col_name] = pl.concat([
                    aggregated_disease_dfs[disease_col_name],
                    res_df
                ]).unique(subset=["kojin_id"])
        
        logger.debug(f"get_comorbidities: 集約された疾患別DataFrameのキー: {list(aggregated_disease_dfs.keys())}")

        # 集約された疾患別DataFrameをpatients_with_comorbiditiesに結合
        for disease_col_name, disease_df in aggregated_disease_dfs.items():
            logger.debug(f"get_comorbidities: '{disease_col_name}' を結合 (shape: {disease_df.shape})")
            # 一旦既存のhas_DISEASE_NAMEカラムを削除し、新しい情報で更新する
            # ただし、pl.DataFrame.dropは新しいDFを返すので、再代入が必要
            if disease_col_name in patients_with_comorbidities.columns:
                 patients_with_comorbidities = patients_with_comorbidities.drop(disease_col_name)
            
            patients_with_comorbidities = patients_with_comorbidities.join(
                disease_df, on="kojin_id", how="left"
            ).with_columns(
                pl.col(disease_col_name).fill_null(False) # joinで見つからなかった場合はFalse
            )
            logger.debug(f"get_comorbidities: '{disease_col_name}' 結合後の shape: {patients_with_comorbidities.shape}")

    else:
        logger.warning("get_comorbidities: 処理可能な併存疾患データが見つかりませんでした。全ての併存疾患フラグはFalseのままです。")
        # この場合、既に全てのhas_xxxカラムはFalseで初期化されているので追加の処理は不要

    # 念のため、全てのhas_xxxカラムが存在し、bool型であることを確認・強制
    for disease_key in Config.COMORBIDITY_ICD10_CODES.keys():
        col_name = f"has_{disease_key}"
        if col_name not in patients_with_comorbidities.columns:
            # このパスは通常通らないはずだが、念のため
            patients_with_comorbidities = patients_with_comorbidities.with_columns(pl.lit(False).cast(pl.Boolean).alias(col_name))
        else:
            # 既存カラムをbool型にキャスト（fill_null(False)の後なので問題ないはず）
            patients_with_comorbidities = patients_with_comorbidities.with_columns(pl.col(col_name).cast(pl.Boolean))
    
    logger.debug(f"get_comorbidities: 最終的な併存疾患データ shape = {patients_with_comorbidities.shape}, columns = {patients_with_comorbidities.columns}")
    logger.debug("get_comorbidities: 終了")
    return patients_with_comorbidities

def create_analysis_datasets(cohorts: Dict[str, pl.DataFrame],
                           base_dir: str, # この引数は実質的に使われなくなる
                           output_dir: str,
                           params: Dict):
    """分析用データセットの作成"""
    logger.info("分析用データセットの作成を開始します")
    logger.debug(f"create_analysis_datasets: cohorts keys = {list(cohorts.keys())}, output_dir = {output_dir}, params = {params}")
    
    logger.debug("create_analysis_datasets: マスターデータを読み込みます")
    # マスターデータは 'master/' ディレクトリから読み込む
    master_data_dir = "master" 
    master_data = load_master_data(master_data_dir)
    
    # その他のデータは 'data/raw/' ディレクトリから読み込む
    raw_data_dir = os.path.join(Config.DATA_ROOT_DIR, "raw") # Config.DATA_ROOT_DIR は 'data' を想定

    for cohort_name, patients_df_original in cohorts.items():
        logger.info(f"\n=== {cohort_name.upper()} COHORT の処理開始 ===")
        logger.debug(f"create_analysis_datasets: コホート '{cohort_name}' の処理開始. patients_df_original shape = {patients_df_original.shape}")
        
        # --- デバッグコード削除: patients_df_original のスキーマ情報は不要になったため削除 ---
        # logger.info(f"--- スキーマ情報 (patients_df_original for {cohort_name}) ---")
        # logger.info(patients_df_original.schema)
        # --- デバッグコード終了 ---

        if patients_df_original.is_empty():
            logger.warning(f"create_analysis_datasets: コホート '{cohort_name}' の患者データが空のためスキップします")
            continue
        
        # 各コホート処理の開始時に元の患者DFをコピーして使用する
        patients_df = patients_df_original.clone()
        logger.debug(f"create_analysis_datasets: patients_df をコピーしました. shape = {patients_df.shape}")
            
        patient_ids = set(patients_df["kojin_id"].to_list())
        logger.debug(f"create_analysis_datasets: patient_ids 数 = {len(patient_ids)}")
        
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 1. 基本情報（適用データ）の結合を開始します")
        tekiyo_df = get_tekiyo_data(raw_data_dir, patient_ids) # 適用データは raw_data_dir から
        if not tekiyo_df.is_empty():
            logger.debug(f"create_analysis_datasets: ({cohort_name}) 適用データを取得しました. 年齢計算を行います.")
            patients_with_demo = calculate_age_at_index(tekiyo_df, patients_df)
        else:
            logger.warning(f"create_analysis_datasets: ({cohort_name}) 適用データが空でした。年齢計算はスキップします。")
            # 年齢カラムが存在しない可能性があるので、Noneで追加しておく
            patients_with_demo = patients_df.with_columns(pl.lit(None, dtype=pl.Int32).alias("age_at_index"))
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 基本情報結合後の patients_with_demo shape = {patients_with_demo.shape}")
        
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 2. 治療群の分類を開始します")
        patients_with_treatment = classify_treatment_groups(patients_with_demo, raw_data_dir, params) # 薬剤関連データは raw_data_dir から
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 治療群分類後の patients_with_treatment shape = {patients_with_treatment.shape}")
        
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 3. 併存疾患の取得を開始します")
        # get_comorbidities は master_data を引数に取るので、raw_data_dir も渡す
        patients_with_comorbidities = get_comorbidities(raw_data_dir, patients_with_treatment, master_data, params) 
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 併存疾患取得後の patients_with_comorbidities shape = {patients_with_comorbidities.shape}")
        
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 4. 健診データの時系列取得を開始します")
        exam_time_series = get_exam_data_time_series(raw_data_dir, patients_with_comorbidities, params) # 健診データは raw_data_dir から
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 健診データ時系列取得後の exam_time_series shape = {exam_time_series.shape}")
        
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 5. ベースラインデータセットの保存を開始します")
        baseline_output_path = os.path.join(output_dir, f"{cohort_name}_cohort_baseline.feather")
        logger.debug(f"create_analysis_datasets: ({cohort_name}) ベースラインデータ保存先: {baseline_output_path}")
        patients_with_comorbidities.write_ipc(baseline_output_path, compression="zstd")
        logger.info(f"{cohort_name} cohort ベースラインデータを保存: {baseline_output_path}")
        
        # 6. 時系列データセットの保存 (exam_time_series を patients_with_comorbidities に結合)
        logger.debug(f"create_analysis_datasets: ({cohort_name}) 6. 時系列データセットの作成と保存を開始します")
        if not exam_time_series.is_empty():
            logger.debug(f"create_analysis_datasets: ({cohort_name}) 健診時系列データをベースラインデータに結合します")
            # 結合キーは kojin_id と time_point だが、ここでは単純に kojin_id で left join し、
            # exam_time_series の持つ time_point ごとの情報を横持ちにするか、縦持ちのまま別のファイルにするか検討が必要。
            # 現在のコードでは exam_time_series はそのまま保存されていない。
            # ここでは、exam_time_series を別途保存する形にする。
            timeseries_output_path = os.path.join(output_dir, f"{cohort_name}_cohort_timeseries_exam.feather")
            exam_time_series.write_ipc(timeseries_output_path, compression="zstd")
            logger.info(f"{cohort_name} cohort 健診時系列データを保存: {timeseries_output_path}")
            logger.debug(f"create_analysis_datasets: ({cohort_name}) 健診時系列データ保存完了: {timeseries_output_path}")
        else:
            logger.warning(f"create_analysis_datasets: ({cohort_name}) 健診時系列データが空のため、保存をスキップします")
        
        # ループの最後に処理完了ログを追加
        logger.info(f"{cohort_name} cohort 処理完了: {len(patients_with_comorbidities)} 患者")
        logger.info(f"=== {cohort_name.upper()} COHORT の処理終了 ===")
        gc.collect() # メモリ解放
        logger.debug(f"create_analysis_datasets: ({cohort_name}) ガーベッジコレクション実行")

    logger.info("分析用データセットの作成を終了します")
    logger.debug("create_analysis_datasets: 終了")

def main():
    """メイン処理"""
    logger.info("DeSC-Nalmefene 分析用データセット作成スクリプトを開始します")
    start_time = time.time()

    logger.debug("main: 設定値の読み込み")
    config = Config()
    # data_root は Config.DATA_ROOT_DIR を使用する (通常 'data/')
    # output_root は Config.OUTPUT_DIR を使用する (通常 'outputs/')
    data_root = Path(config.DATA_ROOT_DIR) 
    output_root = Path(config.OUTPUT_DIR)
    
    # 出力ディレクトリ作成
    os.makedirs(output_root, exist_ok=True)
    logger.debug(f"main: 出力ディレクトリ '{output_root}' を確認/作成しました")

    logger.debug("main: 最適パラメータの計算")
    params = optimize_parameters()
    logger.info(f"最適化パラメータ: {params}")

    logger.debug("main: 患者コホートの読み込み")
    # 患者コホートは output_root (outputs/) から読み込む想定は変わらない
    cohorts = load_patient_cohorts(str(output_root))
    if not cohorts:
        logger.error("処理対象の患者コホートが見つかりませんでした。スクリプトを終了します。")
        return

    logger.debug("main: 分析用データセットの作成")
    # create_analysis_datasets に渡す base_dir は、各関数内で適切に master_data_dir と raw_data_dir を使い分けるため、
    # ここでは Config.DATA_ROOT_DIR ('data') を渡す形は維持し、create_analysis_datasets内で調整する。
    # ただし、今回の修正で create_analysis_datasets 内で直接 master_data_dir と raw_data_dir を定義したので、
    # create_analysis_datasets の base_dir 引数は実質的に使われなくなる。
    # より明確にするため、create_analysis_datasets のシグネチャと呼び出しを変更することも検討できるが、
    # まずは最小限の変更で対応する。
    create_analysis_datasets(cohorts, str(data_root), str(output_root), params)

    end_time = time.time()
    processing_time = end_time - start_time
    logger.info(f"全ての処理が完了しました。処理時間: {processing_time:.2f} 秒")
    logger.debug("main: 終了")

if __name__ == "__main__":
    main()
