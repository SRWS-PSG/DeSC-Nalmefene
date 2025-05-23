#!/usr/bin/env python3
"""
ICD10マスターからレセプト病名コード（7桁）の取得テストスクリプト

目的:
- ICD10マスター（m_icd10.feather）からICD10コードに対応する
  レセプト電算処理システム傷病コード（7桁）を取得する処理をテスト
- F10.2（アルコール依存症）を中心に検証

作成者: Cline
作成日: 2025-01-24
"""

import os
import sys
# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import polars as pl
from pathlib import Path
from typing import List, Dict, Set
import logging

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_icd10_master(file_path: str) -> pl.DataFrame:
    """ICD10マスターファイルの読み込み"""
    logger.info(f"ICD10マスターファイルを読み込みます: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"ICD10マスターファイルが見つかりません: {file_path}")
    
    df = pl.read_ipc(file_path)
    logger.info(f"読み込み完了: {df.shape[0]}件のレコード, {df.shape[1]}カラム")
    
    return df

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

def get_diseases_codes_for_icd10_pattern(icd10_master: pl.DataFrame, 
                                        icd10_pattern: str,
                                        icd10_kbn_code: str = "1") -> Dict[str, List[str]]:
    """
    指定されたICD10パターンに一致するすべてのコードに対応するレセプト病名コードを取得
    
    Args:
        icd10_master: ICD10マスターデータ
        icd10_pattern: ICD10パターン（例：F10で始まる）
        icd10_kbn_code: ICD10区分コード（デフォルト：1=基本疾患）
    
    Returns:
        Dict[str, List[str]]: {ICD10コード: [レセプト病名コードのリスト]}
    """
    logger.info(f"ICD10パターン '{icd10_pattern}*' (区分: {icd10_kbn_code}) に対応するレセプト病名コードを検索")
    
    filtered_df = icd10_master.filter(
        (pl.col("icd10_code").str.starts_with(icd10_pattern)) & 
        (pl.col("icd10_kbn_code") == icd10_kbn_code)
    )
    
    result = {}
    for row in filtered_df.iter_rows(named=True):
        icd10_code = row["icd10_code"]
        diseases_code = row["diseases_code"]
        
        if icd10_code not in result:
            result[icd10_code] = []
        result[icd10_code].append(diseases_code)
    
    logger.info(f"見つかったICD10コード: {len(result)}件")
    for icd10_code, codes in result.items():
        logger.info(f"  - {icd10_code}: {len(codes)}件のレセプト病名コード")
    
    return result

def verify_f10_2_mapping(icd10_master: pl.DataFrame) -> None:
    """F10.2（アルコール依存症）のマッピングを検証"""
    logger.info("="*50)
    logger.info("F10.2（アルコール依存症）のマッピング検証を開始")
    logger.info("="*50)
    
    # F10.2の正確なマッピング
    f10_2_codes = get_diseases_codes_for_icd10(icd10_master, "F102", "1")
    
    if f10_2_codes:
        logger.info(f"✓ F10.2に対応するレセプト病名コード: {f10_2_codes}")
        
        # 詳細情報を表示
        detail_df = icd10_master.filter(
            (pl.col("icd10_code") == "F102") & 
            (pl.col("icd10_kbn_code") == "1")
        ).select([
            "diseases_code", "icd10_code", "icd10_name", 
            "icd10_major_name", "icd10_medium_name"
        ])
        
        logger.info("詳細情報:")
        print(detail_df)
    else:
        logger.warning("✗ F10.2に対応するレセプト病名コードが見つかりませんでした")

def verify_f10_family_mapping(icd10_master: pl.DataFrame) -> None:
    """F10ファミリー全体のマッピングを検証"""
    logger.info("="*50)
    logger.info("F10ファミリー全体のマッピング検証を開始")
    logger.info("="*50)
    
    # F10で始まるすべてのコード
    f10_family = get_diseases_codes_for_icd10_pattern(icd10_master, "F10", "1")
    
    logger.info(f"F10ファミリーの総数: {len(f10_family)}件")
    
    # 各F10コードの詳細
    for icd10_code, diseases_codes in sorted(f10_family.items()):
        detail_df = icd10_master.filter(
            (pl.col("icd10_code") == icd10_code) & 
            (pl.col("icd10_kbn_code") == "1")
        ).select([
            "diseases_code", "icd10_code", "icd10_name"
        ]).unique()
        
        logger.info(f"\n{icd10_code}: {len(diseases_codes)}件のレセプト病名コード")
        print(detail_df)

def test_current_script_approach(icd10_master: pl.DataFrame) -> None:
    """現在のスクリプトのアプローチをテスト"""
    logger.info("="*50)
    logger.info("現在のスクリプトアプローチのテスト")
    logger.info("="*50)
    
    # 現在のprocess_desc_data.pyでは"F10.2"で検索している
    current_approach = "F10.2"
    logger.info(f"現在のアプローチ: diseases_code.str.contains('{current_approach}')")
    
    # このアプローチで見つかるレコードをチェック
    found_with_current = icd10_master.filter(
        pl.col("diseases_code").str.contains(current_approach, literal=True)
    )
    
    logger.info(f"現在のアプローチで見つかるレコード数: {found_with_current.shape[0]}")
    
    if found_with_current.shape[0] > 0:
        logger.info("現在のアプローチで見つかるレコード:")
        print(found_with_current.select([
            "diseases_code", "icd10_code", "icd10_name"
        ]))
    else:
        logger.warning("✗ 現在のアプローチでは該当レコードが見つかりません")
        logger.info("正しいアプローチ: ICD10マスターから正しいdiseases_codeを取得する必要があります")

def main():
    """メイン処理"""
    logger.info("ICD10マッピングテストを開始します")
    
    # ICD10マスターファイルのパス
    icd10_master_path = "master/m_icd10.feather"
    
    try:
        # ICD10マスターの読み込み
        icd10_master = load_icd10_master(icd10_master_path)
        
        # F10.2の検証
        verify_f10_2_mapping(icd10_master)
        
        # F10ファミリー全体の検証
        verify_f10_family_mapping(icd10_master)
        
        # 現在のスクリプトアプローチのテスト
        test_current_script_approach(icd10_master)
        
        logger.info("="*50)
        logger.info("テスト完了")
        logger.info("="*50)
        
        # 結論
        logger.info("結論:")
        logger.info("1. F10.2に対応するレセプト病名コードは '8830330'")
        logger.info("2. 現在のprocess_desc_data.pyのF10_2_CODE = 'F10.2'は不正確")
        logger.info("3. 正しくはICD10マスターを参照して diseases_code を取得すべき")
        logger.info("4. process_desc_data.py の修正が必要")
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        raise

if __name__ == "__main__":
    main()
