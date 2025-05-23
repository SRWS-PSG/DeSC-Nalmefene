#!/usr/bin/env python3
"""
DeSC-Nalmefene 薬剤抽出検証スクリプト

現在のロジックで実際に薬剤が抽出できるかを確認し、
特にシアナミドのレセコード問題を調査します。

作成者: Cline
作成日: 2024-05-24
"""

import os
import sys
# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import logging
import polars as pl
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from tqdm import tqdm
import time
from datetime import datetime, timedelta
from utils.env_loader import DATA_ROOT_DIR as ENV_DATA_ROOT_DIR

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug/drug_extraction_verification.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DrugCodeConfig:
    """現在のロジックで使用している薬剤コード設定"""
    DATA_ROOT_DIR = ENV_DATA_ROOT_DIR
    
    # 現在の薬剤コード設定（修正前）
    DRUG_CODES = {
        "nalmefene": 622607601,      # ナルメフェン (飲酒量低減) - 数値型レセコード
        "acamprosate": 622243701,    # アカンプロサート (断酒) - 数値型レセコード
        "disulfiram": 620008676,     # ジスルフィラム (断酒) - 数値型レセコード
        "cyanamide": 3932001         # シアナミド (断酒) - 数値型レセコード（要確認）
    }
    
    # 追加検証用の候補コード
    ADDITIONAL_SEARCH_PATTERNS = {
        "cyanamide_patterns": [
            "3932001",       # 部分一致用
            "3932001S1041",  # 完全一致用
        ]
    }

def load_drug_master_data() -> pl.DataFrame:
    """薬剤マスターデータの読み込み"""
    logger.info("薬剤マスターデータの読み込みを開始します")
    
    master_file = os.path.join(DrugCodeConfig.DATA_ROOT_DIR, "m_drug_main.feather")
    if not os.path.exists(master_file):
        logger.error(f"薬剤マスターファイルが見つかりません: {master_file}")
        return pl.DataFrame()
    
    drug_master = pl.read_ipc(master_file)
    logger.info(f"薬剤マスターデータ読み込み完了: {len(drug_master)} レコード")
    
    return drug_master

def search_drug_codes_in_master(drug_master: pl.DataFrame) -> Dict[str, List]:
    """薬剤マスターでのコード検索"""
    logger.info("薬剤マスターでのコード検索を開始します")
    
    # カラム名を確認
    logger.info(f"薬剤マスターのカラム: {drug_master.columns}")
    
    results = {}
    
    # 現在の設定コードを検索
    for drug_name, drug_code in DrugCodeConfig.DRUG_CODES.items():
        logger.info(f"\n=== {drug_name.upper()} ({drug_code}) の検索 ===")
        
        # drug_codeで文字列として検索
        drug_code_str_matches = pl.DataFrame()
        try:
            drug_code_str_matches = drug_master.filter(pl.col("drug_code") == str(drug_code))
        except:
            drug_code_str_matches = pl.DataFrame()
        
        # yakka_codeでも検索（レセプトコードの可能性）
        yakka_matches = pl.DataFrame()
        if "yakka_code" in drug_master.columns:
            try:
                yakka_matches = drug_master.filter(pl.col("yakka_code") == str(drug_code))
            except:
                yakka_matches = pl.DataFrame()
        
        # yj_codeでも検索
        yj_matches = pl.DataFrame()
        if "yj_code" in drug_master.columns:
            try:
                yj_matches = drug_master.filter(pl.col("yj_code") == str(drug_code))
            except:
                yj_matches = pl.DataFrame()
        
        # 部分一致検索（すべてのコードカラムで検索）
        partial_matches = pl.DataFrame()
        try:
            partial_drug = drug_master.filter(pl.col("drug_code").str.contains(str(drug_code)))
            partial_yakka = pl.DataFrame()
            partial_yj = pl.DataFrame()
            
            if "yakka_code" in drug_master.columns:
                partial_yakka = drug_master.filter(pl.col("yakka_code").str.contains(str(drug_code)))
            if "yj_code" in drug_master.columns:
                partial_yj = drug_master.filter(pl.col("yj_code").str.contains(str(drug_code)))
            
            # 結果を統合（重複除去）
            all_partials = [partial_drug, partial_yakka, partial_yj]
            non_empty_partials = [df for df in all_partials if not df.is_empty()]
            if non_empty_partials:
                partial_matches = pl.concat(non_empty_partials).unique()
        except:
            partial_matches = pl.DataFrame()
        
        results[drug_name] = {
            "target_code": drug_code,
            "drug_code_matches": len(drug_code_str_matches),
            "yakka_matches": len(yakka_matches),
            "yj_matches": len(yj_matches), 
            "partial_matches": len(partial_matches),
            "drug_code_results": drug_code_str_matches.to_dicts() if len(drug_code_str_matches) > 0 else [],
            "yakka_results": yakka_matches.to_dicts() if len(yakka_matches) > 0 else [],
            "yj_results": yj_matches.to_dicts() if len(yj_matches) > 0 else [],
            "partial_results": partial_matches.to_dicts() if len(partial_matches) > 0 else []
        }
        
        logger.info(f"  drug_code一致: {len(drug_code_str_matches)} 件")
        logger.info(f"  yakka_code一致: {len(yakka_matches)} 件")
        logger.info(f"  yj_code一致: {len(yj_matches)} 件")
        logger.info(f"  部分一致: {len(partial_matches)} 件")
        
        if len(partial_matches) > 0:
            logger.info("  部分一致結果:")
            for match in partial_matches.to_dicts()[:5]:  # 最初の5件表示
                logger.info(f"    - {match}")
    
    # シアナミドの特別検索
    logger.info(f"\n=== CYANAMIDE 特別検索 ===")
    cyanamide_special = {}
    
    for pattern in DrugCodeConfig.ADDITIONAL_SEARCH_PATTERNS["cyanamide_patterns"]:
        logger.info(f"\nパターン検索: {pattern}")
        
        # yakka_codeでの完全一致
        exact_matches = pl.DataFrame()
        if "yakka_code" in drug_master.columns:
            try:
                if pattern.isdigit():
                    exact_matches = drug_master.filter(pl.col("yakka_code") == int(pattern))
                else:
                    exact_matches = drug_master.filter(pl.col("yakka_code").cast(pl.Utf8) == pattern)
            except:
                exact_matches = pl.DataFrame()
        
        # 部分一致
        partial_matches = pl.DataFrame()
        if "yakka_code" in drug_master.columns:
            try:
                partial_matches = drug_master.filter(
                    pl.col("yakka_code").cast(pl.Utf8).str.contains(pattern)
                )
            except:
                partial_matches = pl.DataFrame()
        
        # 薬剤名での検索
        name_matches = drug_master.filter(
            pl.col("drug_name").str.contains("シアナミド|cyanamide|Cyanamide")
        )
        
        cyanamide_special[pattern] = {
            "exact_matches": len(exact_matches),
            "partial_matches": len(partial_matches),
            "name_matches": len(name_matches),
            "exact_results": exact_matches.to_dicts() if len(exact_matches) > 0 else [],
            "partial_results": partial_matches.to_dicts() if len(partial_matches) > 0 else [],
            "name_results": name_matches.to_dicts() if len(name_matches) > 0 else []
        }
        
        logger.info(f"  完全一致: {len(exact_matches)} 件")
        logger.info(f"  部分一致: {len(partial_matches)} 件")
        logger.info(f"  薬剤名検索: {len(name_matches)} 件")
        
        if len(name_matches) > 0:
            logger.info("  薬剤名検索結果:")
            for match in name_matches.to_dicts():
                logger.info(f"    - {match}")
    
    results["cyanamide_special"] = cyanamide_special
    return results

def analyze_recent_drug_receipts(months: int = 6) -> Dict[str, any]:
    """最近の薬剤レセプトデータでの検索"""
    logger.info(f"最近{months}ヶ月の薬剤レセプトデータでの検索を開始します")
    
    drug_dir = os.path.join(DrugCodeConfig.DATA_ROOT_DIR, "receipt_drug")
    if not os.path.exists(drug_dir):
        logger.error(f"薬剤レセプトディレクトリが見つかりません: {drug_dir}")
        return {}
    
    # ファイル一覧を取得して日付順にソート
    drug_files = []
    for f in os.listdir(drug_dir):
        if f.startswith("receipt_drug_") and f.endswith(".feather"):
            try:
                # ファイル名から年月を抽出 (例: receipt_drug_2023_01.feather)
                parts = f.replace("receipt_drug_", "").replace(".feather", "").split("_")
                if len(parts) == 2:
                    year, month = int(parts[0]), int(parts[1])
                    file_date = datetime(year, month, 1)
                    drug_files.append((file_date, os.path.join(drug_dir, f)))
            except ValueError:
                continue
    
    # 最新の6ヶ月分を選択
    drug_files.sort(key=lambda x: x[0], reverse=True)
    recent_files = drug_files[:months]
    
    logger.info(f"検索対象ファイル数: {len(recent_files)}")
    for file_date, file_path in recent_files:
        logger.info(f"  - {file_date.strftime('%Y-%m')}: {os.path.basename(file_path)}")
    
    # 各薬剤コードの検索結果
    drug_search_results = {}
    
    for drug_name, drug_code in DrugCodeConfig.DRUG_CODES.items():
        drug_search_results[drug_name] = {
            "target_code": drug_code,
            "total_records": 0,
            "files_found": 0,
            "monthly_counts": {}
        }
    
    # シアナミド特別検索用
    cyanamide_special_results = {}
    for pattern in DrugCodeConfig.ADDITIONAL_SEARCH_PATTERNS["cyanamide_patterns"]:
        cyanamide_special_results[pattern] = {
            "total_records": 0,
            "files_found": 0,
            "monthly_counts": {}
        }
    
    # ファイルごとに検索
    for file_date, file_path in tqdm(recent_files, desc="薬剤レセプト検索", unit="file"):
        try:
            logger.info(f"\n処理中: {os.path.basename(file_path)}")
            
            # ファイルサイズチェック
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            logger.info(f"ファイルサイズ: {file_size:.2f} MB")
            
            df_drug = pl.read_ipc(file_path)
            total_records = len(df_drug)
            logger.info(f"総レコード数: {total_records:,}")
            
            month_key = file_date.strftime('%Y-%m')
            
            # 現在の薬剤コードで検索
            for drug_name, drug_code in DrugCodeConfig.DRUG_CODES.items():
                # 数値型での検索
                matches = df_drug.filter(pl.col("drug_code") == drug_code)
                match_count = len(matches)
                
                drug_search_results[drug_name]["monthly_counts"][month_key] = match_count
                drug_search_results[drug_name]["total_records"] += match_count
                
                if match_count > 0:
                    drug_search_results[drug_name]["files_found"] += 1
                    logger.info(f"  {drug_name}: {match_count} 件")
                    
                    # サンプルレコードを表示
                    sample_records = matches.head(3).to_dicts()
                    for i, record in enumerate(sample_records):
                        logger.info(f"    サンプル{i+1}: 患者ID={record.get('kojin_id')}, 薬剤コード={record.get('drug_code')}")
            
            # シアナミド特別検索
            logger.info(f"  シアナミド特別検索:")
            
            # 文字列型カラムが存在するかチェック
            has_drug_code_str = "drug_code_str" in df_drug.columns
            
            for pattern in DrugCodeConfig.ADDITIONAL_SEARCH_PATTERNS["cyanamide_patterns"]:
                pattern_matches = 0
                
                if pattern.isdigit():
                    # 数値パターンの場合
                    numeric_matches = df_drug.filter(pl.col("drug_code") == int(pattern))
                    pattern_matches += len(numeric_matches)
                    
                    if has_drug_code_str:
                        # 文字列型でも検索
                        string_exact = df_drug.filter(pl.col("drug_code_str") == pattern)
                        string_partial = df_drug.filter(pl.col("drug_code_str").str.contains(pattern))
                        pattern_matches += len(string_exact) + len(string_partial)
                else:
                    # 文字列パターンの場合
                    if has_drug_code_str:
                        string_matches = df_drug.filter(pl.col("drug_code_str") == pattern)
                        pattern_matches += len(string_matches)
                
                cyanamide_special_results[pattern]["monthly_counts"][month_key] = pattern_matches
                cyanamide_special_results[pattern]["total_records"] += pattern_matches
                
                if pattern_matches > 0:
                    cyanamide_special_results[pattern]["files_found"] += 1
                    logger.info(f"    パターン '{pattern}': {pattern_matches} 件")
            
        except Exception as e:
            logger.error(f"ファイル {file_path} の処理中にエラー: {e}")
            continue
    
    return {
        "standard_drugs": drug_search_results,
        "cyanamide_special": cyanamide_special_results,
        "files_processed": len(recent_files)
    }

def generate_summary_report(master_results: Dict, receipt_results: Dict):
    """検証結果のサマリーレポート生成"""
    logger.info("\n" + "="*80)
    logger.info("薬剤抽出検証結果サマリー")
    logger.info("="*80)
    
    logger.info("\n1. 薬剤マスターでの検索結果:")
    logger.info("-" * 40)
    
    for drug_name, results in master_results.items():
        if drug_name == "cyanamide_special":
            continue
            
        logger.info(f"\n{drug_name.upper()} (コード: {results['target_code']}):")
        logger.info(f"  drug_code一致: {results['drug_code_matches']} 件")
        logger.info(f"  yakka_code一致: {results['yakka_matches']} 件")
        logger.info(f"  yj_code一致: {results['yj_matches']} 件")
        logger.info(f"  部分一致: {results['partial_matches']} 件")
        
        if results['partial_matches'] > 0:
            logger.info("  ⚠️ 部分一致が見つかりました - 設定見直しが必要かもしれません")
    
    logger.info("\n2. シアナミド特別検索結果 (マスター):")
    logger.info("-" * 40)
    
    if "cyanamide_special" in master_results:
        for pattern, results in master_results["cyanamide_special"].items():
            logger.info(f"\nパターン '{pattern}':")
            logger.info(f"  完全一致: {results['exact_matches']} 件")
            logger.info(f"  部分一致: {results['partial_matches']} 件")
            logger.info(f"  薬剤名検索: {results['name_matches']} 件")
    
    logger.info("\n3. 薬剤レセプトでの検索結果:")
    logger.info("-" * 40)
    
    if "standard_drugs" in receipt_results:
        for drug_name, results in receipt_results["standard_drugs"].items():
            logger.info(f"\n{drug_name.upper()} (コード: {results['target_code']}):")
            logger.info(f"  総件数: {results['total_records']:,} 件")
            logger.info(f"  該当ファイル数: {results['files_found']} / {receipt_results['files_processed']}")
            
            if results['total_records'] == 0:
                logger.info("  ❌ レセプトデータで見つかりませんでした")
            else:
                logger.info("  ✅ レセプトデータで見つかりました")
                
                # 月別詳細
                monthly = results['monthly_counts']
                if monthly:
                    logger.info("  月別内訳:")
                    for month, count in sorted(monthly.items()):
                        logger.info(f"    {month}: {count:,} 件")
    
    logger.info("\n4. シアナミド特別検索結果 (レセプト):")
    logger.info("-" * 40)
    
    if "cyanamide_special" in receipt_results:
        for pattern, results in receipt_results["cyanamide_special"].items():
            logger.info(f"\nパターン '{pattern}':")
            logger.info(f"  総件数: {results['total_records']:,} 件")
            logger.info(f"  該当ファイル数: {results['files_found']} / {receipt_results['files_processed']}")
            
            if results['total_records'] > 0:
                monthly = results['monthly_counts']
                if monthly:
                    logger.info("  月別内訳:")
                    for month, count in sorted(monthly.items()):
                        if count > 0:
                            logger.info(f"    {month}: {count:,} 件")
    
    logger.info("\n5. 推奨アクション:")
    logger.info("-" * 40)
    
    # 問題のある薬剤を特定
    problem_drugs = []
    if "standard_drugs" in receipt_results:
        for drug_name, results in receipt_results["standard_drugs"].items():
            if results['total_records'] == 0:
                problem_drugs.append(drug_name)
    
    if problem_drugs:
        logger.info(f"⚠️ 以下の薬剤でレセプトデータが見つかりませんでした: {', '.join(problem_drugs)}")
        logger.info("   - 薬剤コード設定の見直しが必要です")
        logger.info("   - 薬剤マスターでの部分一致結果を参考に正しいコードを特定してください")
    
    # シアナミドの特別な推奨
    cyanamide_standard = receipt_results.get("standard_drugs", {}).get("cyanamide", {}).get("total_records", 0)
    cyanamide_special_total = sum(r.get("total_records", 0) for r in receipt_results.get("cyanamide_special", {}).values())
    
    if cyanamide_standard == 0 and cyanamide_special_total > 0:
        logger.info("\n🔍 シアナミドについて:")
        logger.info("   - 現在の数値型コード (3932001) では見つかりませんでした")
        logger.info("   - 文字列型コード検索では結果が見つかりました")
        logger.info("   - 設定を文字列型コードに変更することを推奨します")

def main():
    """メイン処理"""
    logger.info("薬剤抽出検証を開始します")
    start_time = time.time()
    
    try:
        # 1. 薬剤マスターでの検索
        logger.info("\n=== STEP 1: 薬剤マスターでの検索 ===")
        drug_master = load_drug_master_data()
        
        if drug_master.is_empty():
            logger.error("薬剤マスターデータの読み込みに失敗しました")
            return
        
        master_results = search_drug_codes_in_master(drug_master)
        
        # 2. 薬剤レセプトでの検索
        logger.info("\n=== STEP 2: 薬剤レセプトでの検索 ===")
        receipt_results = analyze_recent_drug_receipts(months=6)
        
        # 3. サマリーレポート生成
        logger.info("\n=== STEP 3: サマリーレポート生成 ===")
        generate_summary_report(master_results, receipt_results)
        
        elapsed_time = time.time() - start_time
        logger.info(f"\n検証完了 (実行時間: {elapsed_time:.2f}秒)")
        
    except Exception as e:
        logger.error(f"検証処理中にエラー: {e}")
        raise

if __name__ == "__main__":
    main()
