#!/usr/bin/env python3
"""
薬剤コード検証スクリプト（修正版）

データ型の問題を修正し、ATCコードからレセコードのマッピングを確認し、
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
        "drug_who_atc": "m_drug_who_atc.feather"
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
    """ATCコードからレセコードを取得（数値型に変換）"""
    logger.info("ATCコードからレセコードのマッピングを取得しています...")
    
    drug_mapping = {}
    
    for drug_name, atc_code in target_atc_codes.items():
        logger.info(f"\n=== {drug_name} (ATC: {atc_code}) ===")
        
        rece_codes = []
        rece_codes_int = []
        
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
                
                # 数値型にも変換
                for code in codes:
                    try:
                        int_code = int(code)
                        rece_codes_int.append(int_code)
                    except (ValueError, TypeError):
                        logger.warning(f"レセコード {code} を数値に変換できませんでした")
                
                # サンプルの薬剤名も表示
                if "atc_name" in atc_matches.columns:
                    sample_names = atc_matches["atc_name"].head(3).to_list()
                else:
                    sample_names = ["名前情報なし"]
                logger.info(f"サンプル薬剤名: {sample_names}")
        
        # 重複を除去
        rece_codes = list(set(rece_codes))
        rece_codes_int = list(set(rece_codes_int))
        
        drug_mapping[drug_name] = {
            "atc_code": atc_code,
            "rece_codes": rece_codes,
            "rece_codes_int": rece_codes_int
        }
        
        logger.info(f"最終的なレセコード: {rece_codes} (計{len(rece_codes)}件)")
        logger.info(f"数値型レセコード: {rece_codes_int}")
    
    return drug_mapping

def check_drug_file_structure(base_dir: str):
    """薬剤ファイルの構造を確認"""
    logger.info("薬剤ファイルの構造を確認しています...")
    
    drug_dir = os.path.join(base_dir, "receipt_drug")
    if not os.path.exists(drug_dir):
        logger.error(f"薬剤ディレクトリが見つかりません: {drug_dir}")
        return None
    
    drug_files = [f for f in os.listdir(drug_dir) if f.endswith(".feather")]
    if not drug_files:
        logger.error("薬剤ファイルが見つかりません")
        return None
    
    # 最初のファイルで構造確認
    sample_file = os.path.join(drug_dir, sorted(drug_files)[0])
    try:
        df = pl.read_ipc(sample_file)
        logger.info(f"サンプルファイル: {sorted(drug_files)[0]}")
        logger.info(f"カラム: {df.columns}")
        logger.info(f"drug_codeの型: {df['drug_code'].dtype}")
        logger.info(f"サンプルdrug_code: {df['drug_code'].head(5).to_list()}")
        return df.schema
    except Exception as e:
        logger.error(f"構造確認エラー: {e}")
        return None

def verify_codes_in_receipt_data(base_dir: str, drug_mapping: dict, test_files: int = 3):
    """実際の薬剤ファイルでレセコードを検証（データ型対応）"""
    logger.info(f"実際の薬剤ファイル（{test_files}ファイル）でレセコードを検証しています...")
    
    drug_dir = os.path.join(base_dir, "receipt_drug")
    if not os.path.exists(drug_dir):
        logger.error(f"薬剤ディレクトリが見つかりません: {drug_dir}")
        return {}
    
    drug_files = [f for f in os.listdir(drug_dir) if f.endswith(".feather")]
    drug_files = sorted(drug_files)[:test_files]  # 最初のN件のみテスト
    
    verification_results = {}
    
    for drug_name, info in drug_mapping.items():
        verification_results[drug_name] = {
            "atc_code": info["atc_code"],
            "rece_codes": info["rece_codes"],
            "rece_codes_int": info["rece_codes_int"],
            "found_in_files": [],
            "total_prescriptions": 0,
            "sample_records": []
        }
    
    for file_name in tqdm(drug_files, desc="薬剤ファイル検証"):
        file_path = os.path.join(drug_dir, file_name)
        
        try:
            df = pl.read_ipc(file_path)
            logger.info(f"\n{file_name}: {len(df)} レコード, drug_code型: {df['drug_code'].dtype}")
            
            for drug_name, info in drug_mapping.items():
                if info["rece_codes_int"]:
                    # 数値型で検索
                    matches = df.filter(pl.col("drug_code").is_in(info["rece_codes_int"]))
                    
                    if len(matches) > 0:
                        verification_results[drug_name]["found_in_files"].append(file_name)
                        verification_results[drug_name]["total_prescriptions"] += len(matches)
                        
                        # サンプルレコードを取得（最大5件）
                        sample_cols = ["kojin_id", "drug_code"]
                        if "drug_name" in df.columns:
                            sample_cols.append("drug_name")
                        if "kisoku_tanni" in df.columns:
                            sample_cols.append("kisoku_tanni")
                        
                        sample = matches.head(5).select(sample_cols).to_dicts()
                        verification_results[drug_name]["sample_records"].extend(sample)
                        
                        logger.info(f"  {drug_name}: {len(matches)} 件の処方")
                        
        except Exception as e:
            logger.warning(f"ファイル {file_name} の読み込みエラー: {e}")
    
    return verification_results

def verify_cyanamide_code(base_dir: str, cyanamide_code: str, test_files: int = 3):
    """シアナミドのレセコードを検証（データ型対応）"""
    logger.info(f"シアナミドのレセコード ({cyanamide_code}) を検証しています...")
    
    drug_dir = os.path.join(base_dir, "receipt_drug")
    if not os.path.exists(drug_dir):
        logger.error(f"薬剤ディレクトリが見つかりません: {drug_dir}")
        return None
    
    drug_files = [f for f in os.listdir(drug_dir) if f.endswith(".feather")]
    drug_files = sorted(drug_files)[:test_files]
    
    # 文字列と数値の両方で試す
    try:
        cyanamide_code_int = int(cyanamide_code)
    except (ValueError, TypeError):
        cyanamide_code_int = None
    
    cyanamide_results = {
        "rece_code": cyanamide_code,
        "rece_code_int": cyanamide_code_int,
        "found_in_files": [],
        "total_prescriptions": 0,
        "sample_records": []
    }
    
    for file_name in tqdm(drug_files, desc="シアナミド検証"):
        file_path = os.path.join(drug_dir, file_name)
        
        try:
            df = pl.read_ipc(file_path)
            
            # データ型に応じて検索方法を変える
            if df['drug_code'].dtype == pl.Int64:
                if cyanamide_code_int is not None:
                    matches = df.filter(pl.col("drug_code") == cyanamide_code_int)
                else:
                    matches = pl.DataFrame()
            else:
                matches = df.filter(pl.col("drug_code") == cyanamide_code)
            
            if len(matches) > 0:
                cyanamide_results["found_in_files"].append(file_name)
                cyanamide_results["total_prescriptions"] += len(matches)
                
                sample_cols = ["kojin_id", "drug_code"]
                if "drug_name" in df.columns:
                    sample_cols.append("drug_name")
                if "kisoku_tanni" in df.columns:
                    sample_cols.append("kisoku_tanni")
                
                sample = matches.head(5).select(sample_cols).to_dicts()
                cyanamide_results["sample_records"].extend(sample)
                
                logger.info(f"  {file_name}: {len(matches)} 件の処方")
                
        except Exception as e:
            logger.warning(f"ファイル {file_name} の読み込みエラー: {e}")
    
    return cyanamide_results

def print_summary_report(drug_mapping: dict, verification_results: dict, cyanamide_results: dict):
    """検証結果のサマリーレポート"""
    print("\n" + "="*80)
    print("薬剤コード検証結果サマリー（修正版）")
    print("="*80)
    
    print("\n1. ATCコードからレセコードへのマッピング結果:")
    print("-" * 50)
    for drug_name, info in drug_mapping.items():
        print(f"\n【{drug_name}】")
        print(f"  ATCコード: {info['atc_code']}")
        print(f"  レセコード（文字列）: {info['rece_codes']}")
        print(f"  レセコード（数値）: {info['rece_codes_int']}")
        print(f"  取得コード数: {len(info['rece_codes'])}")
    
    print(f"\n【シアナミド（現在のコード）】")
    print(f"  レセコード（文字列）: {cyanamide_results['rece_code']}")
    print(f"  レセコード（数値）: {cyanamide_results['rece_code_int']}")
    
    print("\n2. 実際の処方データでの検証結果:")
    print("-" * 50)
    for drug_name, result in verification_results.items():
        print(f"\n【{drug_name}】")
        print(f"  検索対象レセコード: {result['rece_codes_int']}")
        print(f"  処方が見つかったファイル数: {len(result['found_in_files'])}")
        print(f"  総処方件数: {result['total_prescriptions']}")
        
        if result['sample_records']:
            print("  サンプル処方レコード:")
            for i, record in enumerate(result['sample_records'][:3]):
                print(f"    {i+1}. コード:{record['drug_code']}, 薬品名:{record.get('drug_name', 'N/A')}")
    
    print(f"\n【シアナミド】")
    print(f"  検索対象レセコード: {cyanamide_results['rece_code_int']}")
    print(f"  処方が見つかったファイル数: {len(cyanamide_results['found_in_files'])}")
    print(f"  総処方件数: {cyanamide_results['total_prescriptions']}")
    
    if cyanamide_results['sample_records']:
        print("  サンプル処方レコード:")
        for i, record in enumerate(cyanamide_results['sample_records'][:3]):
            print(f"    {i+1}. コード:{record['drug_code']}, 薬品名:{record.get('drug_name', 'N/A')}")
    
    print("\n3. 修正されたDRUG_CODES設定:")
    print("-" * 50)
    print("DRUG_CODES = {")
    
    for drug_name, result in verification_results.items():
        if result['rece_codes_int']:
            if len(result['rece_codes_int']) == 1:
                print(f'    "{drug_name}": {result["rece_codes_int"][0]},  # 数値型')
            else:
                print(f'    "{drug_name}": {result["rece_codes_int"]},  # 数値型')
        else:
            print(f'    "{drug_name}": None,  # レセコードが見つかりませんでした')
    
    if cyanamide_results['rece_code_int'] is not None:
        print(f'    "cyanamide": {cyanamide_results["rece_code_int"]}  # 数値型')
    else:
        print(f'    "cyanamide": "{cyanamide_results["rece_code"]}"  # 文字列型（要確認）')
    print("}")
    
    print("\n4. 問題と推奨事項:")
    print("-" * 50)
    print("🔍 発見された主要な問題:")
    print("   - 元のconfigではATCコードを使用していましたが、実際にはレセコード（数値）が必要")
    print("   - 薬剤ファイルのdrug_codeは数値型（Int64）です")
    
    issues_found = False
    for drug_name, result in verification_results.items():
        if not result['rece_codes_int']:
            print(f"⚠️  {drug_name}: ATCコードからレセコードが取得できませんでした")
            issues_found = True
        elif result['total_prescriptions'] == 0:
            print(f"⚠️  {drug_name}: レセコードは存在しますが、実際の処方データでは見つかりませんでした")
            print(f"      → この期間（2014年4-6月）には処方されていない可能性があります")
            issues_found = True
        else:
            print(f"✅ {drug_name}: 正常に検証できました")
    
    if cyanamide_results['total_prescriptions'] == 0:
        print(f"⚠️  シアナミド: 処方データが見つかりませんでした")
        issues_found = True
    else:
        print(f"✅ シアナミド: 正常に検証できました")
    
    if issues_found:
        print(f"\n💡 推奨事項:")
        print("   1. より新しい期間のデータで再検証してください")
        print("   2. 薬剤の市場導入時期を確認してください")
        print("   3. 治療群分類ロジックの見直しを検討してください")

def main():
    """メイン処理"""
    logger.info("薬剤コード検証（修正版）を開始します")
    
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
    
    # 0. 薬剤ファイルの構造確認
    schema = check_drug_file_structure(base_dir)
    if schema is None:
        logger.error("薬剤ファイルの構造確認に失敗しました")
        return
    
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
    
    logger.info("薬剤コード検証（修正版）が完了しました")

if __name__ == "__main__":
    main()
