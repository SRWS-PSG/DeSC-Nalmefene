#!/usr/bin/env python3
"""
薬剤コード検証スクリプト

ATCコードからレセコードのマッピングを確認し、
実際の薬剤ファイルで該当薬剤が検索できるかを検証します。

作成者: Cline
作成日: 2025-05-24
"""

import os
import sys
# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import polars as pl
from pathlib import Path
import logging
from utils.env_loader import DATA_ROOT_DIR
from tqdm import tqdm

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_drug_masters(base_dir: str):
    """薬剤マスターファイルの読み込み"""
    logger.info("薬剤マスターファイルを読み込んでいます...")
    
    master_files = {
        "drug_main": "m_drug_main.feather",
        "drug_who_atc": "m_drug_who_atc.feather",
        "drug_rece_all": "m_drug_rece_all.feather"
    }
    
    masters = {}
    for key, filename in master_files.items():
        file_path = os.path.join(base_dir, filename)
        if os.path.exists(file_path):
            masters[key] = pl.read_ipc(file_path)
            logger.info(f"{filename}: {len(masters[key])} レコード")
        else:
            logger.warning(f"{filename} が見つかりません")
    
    return masters

def get_rece_codes_from_atc(masters: dict, target_atc_codes: dict):
    """ATCコードからレセコードを取得"""
    logger.info("ATCコードからレセコードのマッピングを取得しています...")
    
    drug_mapping = {}
    
    for drug_name, atc_code in target_atc_codes.items():
        logger.info(f"\n=== {drug_name} (ATC: {atc_code}) ===")
        
        rece_codes = []
        
        # m_drug_who_atcからATCコードに対応するレセコード（drug_code）を直接取得
        if "drug_who_atc" in masters:
            atc_matches = masters["drug_who_atc"].filter(
                pl.col("atc_code") == atc_code
            )
            logger.info(f"ATCマスターでの一致: {len(atc_matches)} 件")
            
            if len(atc_matches) > 0:
                # drug_codeを直接取得（これがレセコード）
                codes = atc_matches["drug_code"].to_list()
                rece_codes.extend(codes)
                logger.info(f"ATCコード {atc_code} → レセコード: {codes}")
                
                # サンプルの薬剤名も表示
                if "drug_name" in atc_matches.columns:
                    sample_names = atc_matches["drug_name"].head(3).to_list()
                elif "atc_name" in atc_matches.columns:
                    sample_names = atc_matches["atc_name"].head(3).to_list()
                else:
                    sample_names = ["名前情報なし"]
                logger.info(f"サンプル薬剤名: {sample_names}")
        
        # 重複を除去
        rece_codes = list(set(rece_codes))
        drug_mapping[drug_name] = {
            "atc_code": atc_code,
            "rece_codes": rece_codes
        }
        
        logger.info(f"最終的なレセコード: {rece_codes} (計{len(rece_codes)}件)")
    
    return drug_mapping

def verify_codes_in_receipt_data(base_dir: str, drug_mapping: dict, test_files: int = 3):
    """実際の薬剤ファイルでレセコードを検証"""
    logger.info(f"実際の薬剤ファイル（{test_files}ファイル）でレセコードを検証しています...")
    
    drug_dir = os.path.join(base_dir, "receipt_drug")
    if not os.path.exists(drug_dir):
        logger.error(f"薬剤ディレクトリが見つかりません: {drug_dir}")
        return
    
    drug_files = [f for f in os.listdir(drug_dir) if f.endswith(".feather")]
    drug_files = sorted(drug_files)[:test_files]  # 最初のN件のみテスト
    
    verification_results = {}
    
    for drug_name, info in drug_mapping.items():
        verification_results[drug_name] = {
            "atc_code": info["atc_code"],
            "rece_codes": info["rece_codes"],
            "found_in_files": [],
            "total_prescriptions": 0,
            "sample_records": []
        }
    
    for file_name in tqdm(drug_files, desc="薬剤ファイル検証"):
        file_path = os.path.join(drug_dir, file_name)
        
        try:
            df = pl.read_ipc(file_path)
            logger.info(f"\n{file_name}: {len(df)} レコード")
            
            for drug_name, info in drug_mapping.items():
                if info["rece_codes"]:
                    matches = df.filter(pl.col("drug_code").is_in(info["rece_codes"]))
                    
                    if len(matches) > 0:
                        verification_results[drug_name]["found_in_files"].append(file_name)
                        verification_results[drug_name]["total_prescriptions"] += len(matches)
                        
                        # サンプルレコードを取得（最大5件）
                        sample = matches.head(5).select([
                            "kojin_id", "drug_code", "drug_name", "kisoku_tanni"
                        ]).to_dicts()
                        verification_results[drug_name]["sample_records"].extend(sample)
                        
                        logger.info(f"  {drug_name}: {len(matches)} 件の処方")
                        
        except Exception as e:
            logger.warning(f"ファイル {file_name} の読み込みエラー: {e}")
    
    return verification_results

def verify_cyanamide_code(base_dir: str, cyanamide_code: str, test_files: int = 3):
    """シアナミドのレセコードを検証"""
    logger.info(f"シアナミドのレセコード ({cyanamide_code}) を検証しています...")
    
    drug_dir = os.path.join(base_dir, "receipt_drug")
    if not os.path.exists(drug_dir):
        logger.error(f"薬剤ディレクトリが見つかりません: {drug_dir}")
        return None
    
    drug_files = [f for f in os.listdir(drug_dir) if f.endswith(".feather")]
    drug_files = sorted(drug_files)[:test_files]
    
    cyanamide_results = {
        "rece_code": cyanamide_code,
        "found_in_files": [],
        "total_prescriptions": 0,
        "sample_records": []
    }
    
    for file_name in tqdm(drug_files, desc="シアナミド検証"):
        file_path = os.path.join(drug_dir, file_name)
        
        try:
            df = pl.read_ipc(file_path)
            matches = df.filter(pl.col("drug_code") == cyanamide_code)
            
            if len(matches) > 0:
                cyanamide_results["found_in_files"].append(file_name)
                cyanamide_results["total_prescriptions"] += len(matches)
                
                sample = matches.head(5).select([
                    "kojin_id", "drug_code", "drug_name", "kisoku_tanni"
                ]).to_dicts()
                cyanamide_results["sample_records"].extend(sample)
                
                logger.info(f"  {file_name}: {len(matches)} 件の処方")
                
        except Exception as e:
            logger.warning(f"ファイル {file_name} の読み込みエラー: {e}")
    
    return cyanamide_results

def print_summary_report(drug_mapping: dict, verification_results: dict, cyanamide_results: dict):
    """検証結果のサマリーレポート"""
    print("\n" + "="*80)
    print("薬剤コード検証結果サマリー")
    print("="*80)
    
    print("\n1. ATCコードからレセコードへのマッピング結果:")
    print("-" * 50)
    for drug_name, info in drug_mapping.items():
        print(f"\n【{drug_name}】")
        print(f"  ATCコード: {info['atc_code']}")
        print(f"  レセコード: {info['rece_codes']}")
        print(f"  取得コード数: {len(info['rece_codes'])}")
    
    print(f"\n【シアナミド（現在のコード）】")
    print(f"  レセコード: {cyanamide_results['rece_code']}")
    
    print("\n2. 実際の処方データでの検証結果:")
    print("-" * 50)
    for drug_name, result in verification_results.items():
        print(f"\n【{drug_name}】")
        print(f"  検索対象レセコード: {result['rece_codes']}")
        print(f"  処方が見つかったファイル数: {len(result['found_in_files'])}")
        print(f"  総処方件数: {result['total_prescriptions']}")
        
        if result['sample_records']:
            print("  サンプル処方レコード:")
            for i, record in enumerate(result['sample_records'][:3]):
                print(f"    {i+1}. コード:{record['drug_code']}, 薬品名:{record.get('drug_name', 'N/A')}")
    
    print(f"\n【シアナミド】")
    print(f"  検索対象レセコード: {cyanamide_results['rece_code']}")
    print(f"  処方が見つかったファイル数: {len(cyanamide_results['found_in_files'])}")
    print(f"  総処方件数: {cyanamide_results['total_prescriptions']}")
    
    if cyanamide_results['sample_records']:
        print("  サンプル処方レコード:")
        for i, record in enumerate(cyanamide_results['sample_records'][:3]):
            print(f"    {i+1}. コード:{record['drug_code']}, 薬品名:{record.get('drug_name', 'N/A')}")
    
    print("\n3. 推奨されるDRUG_CODES設定:")
    print("-" * 50)
    print("DRUG_CODES = {")
    
    for drug_name, result in verification_results.items():
        if result['rece_codes']:
            if len(result['rece_codes']) == 1:
                print(f'    "{drug_name}": "{result["rece_codes"][0]}",')
            else:
                print(f'    "{drug_name}": {result["rece_codes"]},')
        else:
            print(f'    "{drug_name}": [],  # レセコードが見つかりませんでした')
    
    print(f'    "cyanamide": "{cyanamide_results["rece_code"]}"')
    print("}")
    
    print("\n4. 問題と推奨事項:")
    print("-" * 50)
    
    issues_found = False
    for drug_name, result in verification_results.items():
        if not result['rece_codes']:
            print(f"⚠️  {drug_name}: ATCコードからレセコードが取得できませんでした")
            issues_found = True
        elif result['total_prescriptions'] == 0:
            print(f"⚠️  {drug_name}: レセコードは存在しますが、実際の処方データでは見つかりませんでした")
            issues_found = True
        else:
            print(f"✅ {drug_name}: 正常に検証できました")
    
    if cyanamide_results['total_prescriptions'] == 0:
        print(f"⚠️  シアナミド: 現在のレセコードで処方データが見つかりませんでした")
        issues_found = True
    else:
        print(f"✅ シアナミド: 正常に検証できました")
    
    if not issues_found:
        print("✅ 全ての薬剤コードが正常に検証されました")

def main():
    """メイン処理"""
    logger.info("薬剤コード検証を開始します")
    
    base_dir = DATA_ROOT_DIR
    logger.info(f"データルートディレクトリ: {base_dir}")
    
    # 検証対象のATCコード（現在のconfig）
    target_atc_codes = {
        "nalmefene": "N07BB05",
        "acamprosate": "N07BB03", 
        "disulfiram": "N07BB01"
    }
    
    # シアナミドの現在のレセコード
    cyanamide_code = "3932001S1041"
    
    # 1. マスターファイルの読み込み
    masters = load_drug_masters(base_dir)
    
    # 2. ATCコードからレセコードのマッピング取得
    drug_mapping = get_rece_codes_from_atc(masters, target_atc_codes)
    
    # 3. 実際の薬剤ファイルでの検証
    verification_results = verify_codes_in_receipt_data(base_dir, drug_mapping, test_files=3)
    
    # 4. シアナミドの検証
    cyanamide_results = verify_cyanamide_code(base_dir, cyanamide_code, test_files=3)
    
    # 5. サマリーレポートの出力
    print_summary_report(drug_mapping, verification_results, cyanamide_results)
    
    logger.info("薬剤コード検証が完了しました")

if __name__ == "__main__":
    main()
