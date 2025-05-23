#!/usr/bin/env python3
"""
DeSC-Nalmefene 薬剤用量・剤形確認スクリプト

各薬剤の用量違い・剤形違いを詳細に調査します。

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
from utils.env_loader import DATA_ROOT_DIR as ENV_DATA_ROOT_DIR

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug/drug_dosages_check.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DrugSearch:
    """薬剤検索クラス"""
    DATA_ROOT_DIR = ENV_DATA_ROOT_DIR
    
    # 薬剤名での検索パターン
    DRUG_SEARCH_PATTERNS = {
        "nalmefene": [
            "ナルメフェン", "nalmefene", "Nalmefene", "NALMEFENE",
            "セリンクロ", "Selincro"
        ],
        "acamprosate": [
            "アカンプロサート", "acamprosate", "Acamprosate", "ACAMPROSATE", 
            "レグテクト", "Regtect"
        ],
        "disulfiram": [
            "ジスルフィラム", "disulfiram", "Disulfiram", "DISULFIRAM",
            "ノックビン", "Antabuse"
        ],
        "cyanamide": [
            "シアナミド", "cyanamide", "Cyanamide", "CYANAMIDE",
            "シアナマイド", "カルバミド"
        ]
    }

def load_drug_master_data() -> pl.DataFrame:
    """薬剤マスターデータの読み込み"""
    logger.info("薬剤マスターデータの読み込みを開始します")
    
    master_file = os.path.join(DrugSearch.DATA_ROOT_DIR, "m_drug_main.feather")
    if not os.path.exists(master_file):
        logger.error(f"薬剤マスターファイルが見つかりません: {master_file}")
        return pl.DataFrame()
    
    drug_master = pl.read_ipc(master_file)
    logger.info(f"薬剤マスターデータ読み込み完了: {len(drug_master)} レコード")
    
    return drug_master

def search_all_drug_variants(drug_master: pl.DataFrame) -> Dict[str, List]:
    """各薬剤の全ての用量・剤形を検索"""
    logger.info("薬剤の全用量・剤形検索を開始します")
    
    results = {}
    
    for drug_name, search_patterns in DrugSearch.DRUG_SEARCH_PATTERNS.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"{drug_name.upper()} の詳細検索")
        logger.info(f"{'='*60}")
        
        all_matches = []
        
        # 各検索パターンで検索
        for pattern in search_patterns:
            logger.info(f"\n--- 検索パターン: '{pattern}' ---")
            
            # 薬剤名での検索
            name_matches = drug_master.filter(
                pl.col("drug_name").str.contains(pattern)
            )
            
            # ブランド名での検索
            brand_matches = drug_master.filter(
                pl.col("brand_name").str.contains(pattern)
            )
            
            # 一般名での検索
            ippan_matches = drug_master.filter(
                pl.col("ippan_name").str.contains(pattern)
            )
            
            # 結果をまとめる
            pattern_matches = []
            for matches, source in [(name_matches, "drug_name"), 
                                  (brand_matches, "brand_name"), 
                                  (ippan_matches, "ippan_name")]:
                if not matches.is_empty():
                    for match in matches.to_dicts():
                        match['search_source'] = source
                        match['search_pattern'] = pattern
                        pattern_matches.append(match)
            
            if pattern_matches:
                logger.info(f"  見つかった製品数: {len(pattern_matches)}")
                for match in pattern_matches:
                    logger.info(f"    - {match['drug_name']} (コード: {match['drug_code']})")
                    logger.info(f"      規格: {match['kikaku_tani']}, ブランド: {match['brand_name']}")
            else:
                logger.info(f"  該当なし")
            
            all_matches.extend(pattern_matches)
        
        # 重複除去
        unique_matches = []
        seen_codes = set()
        for match in all_matches:
            if match['drug_code'] not in seen_codes:
                unique_matches.append(match)
                seen_codes.add(match['drug_code'])
        
        results[drug_name] = unique_matches
        
        # サマリー表示
        logger.info(f"\n--- {drug_name.upper()} サマリー ---")
        logger.info(f"総製品数: {len(unique_matches)}")
        
        if unique_matches:
            logger.info("製品一覧:")
            for i, match in enumerate(unique_matches, 1):
                logger.info(f"  {i}. {match['drug_name']}")
                logger.info(f"     コード: {match['drug_code']}")
                logger.info(f"     規格: {match['kikaku_tani']}")
                logger.info(f"     ブランド: {match['brand_name']}")
                logger.info(f"     一般名: {match['ippan_name']}")
                logger.info(f"     YJコード: {match['yj_code']}")
                logger.info(f"     薬価コード: {match['yakka_code']}")
                logger.info("")
    
    return results

def analyze_dosage_patterns(results: Dict[str, List]) -> Dict[str, Dict]:
    """用量パターンの分析"""
    logger.info("\n" + "="*80)
    logger.info("用量・剤形パターン分析")
    logger.info("="*80)
    
    analysis = {}
    
    for drug_name, matches in results.items():
        if not matches:
            continue
            
        logger.info(f"\n{drug_name.upper()} の用量・剤形分析:")
        logger.info("-" * 50)
        
        # 規格による分類
        dosage_groups = {}
        formulation_types = set()
        
        for match in matches:
            kikaku = match['kikaku_tani']
            drug_name_full = match['drug_name']
            
            # 剤形の推定
            if any(x in drug_name_full for x in ['錠', 'タブレット', 'Tab']):
                formulation = '錠剤'
            elif any(x in drug_name_full for x in ['液', 'シロップ', 'Liquid']):
                formulation = '液剤'
            elif any(x in drug_name_full for x in ['原末', '末', 'Powder']):
                formulation = '原末'
            elif any(x in drug_name_full for x in ['カプセル', 'Cap']):
                formulation = 'カプセル'
            else:
                formulation = 'その他'
            
            formulation_types.add(formulation)
            
            if kikaku not in dosage_groups:
                dosage_groups[kikaku] = []
            dosage_groups[kikaku].append({
                'drug_code': match['drug_code'],
                'drug_name': drug_name_full,
                'brand_name': match['brand_name'],
                'formulation': formulation
            })
        
        # 結果表示
        logger.info(f"剤形タイプ: {', '.join(sorted(formulation_types))}")
        logger.info(f"用量バリエーション数: {len(dosage_groups)}")
        
        logger.info("\n用量別詳細:")
        for kikaku, products in sorted(dosage_groups.items()):
            logger.info(f"  規格: {kikaku}")
            for product in products:
                logger.info(f"    - {product['drug_name']} ({product['formulation']})")
                logger.info(f"      コード: {product['drug_code']}, ブランド: {product['brand_name']}")
        
        analysis[drug_name] = {
            'total_products': len(matches),
            'dosage_groups': dosage_groups,
            'formulation_types': list(formulation_types),
            'products': matches
        }
    
    return analysis

def generate_recommendations(analysis: Dict[str, Dict]) -> None:
    """推奨事項の生成"""
    logger.info("\n" + "="*80)
    logger.info("推奨事項・注意点")
    logger.info("="*80)
    
    for drug_name, data in analysis.items():
        if data['total_products'] == 0:
            continue
            
        logger.info(f"\n{drug_name.upper()}:")
        logger.info("-" * 30)
        
        if data['total_products'] == 1:
            logger.info("✅ 単一製品のみ - 設定は適切です")
            product = data['products'][0]
            logger.info(f"   推奨コード: {product['drug_code']}")
        else:
            logger.info(f"⚠️ 複数製品あり ({data['total_products']}製品)")
            logger.info("   以下の点を考慮してください:")
            
            # 剤形の多様性チェック
            if len(data['formulation_types']) > 1:
                logger.info(f"   - 複数剤形: {', '.join(data['formulation_types'])}")
                logger.info("   - 研究目的に応じて剤形を選択してください")
            
            # 用量の多様性チェック
            if len(data['dosage_groups']) > 1:
                logger.info(f"   - 複数用量: {len(data['dosage_groups'])}種類")
                logger.info("   - 主要用量を特定することを推奨します")
            
            # 推奨アクション
            logger.info("   推奨アクション:")
            logger.info("   1. 最も一般的な用量・剤形を特定")
            logger.info("   2. 複数コードでの検索を検討")
            logger.info("   3. または包括的検索パターンの使用")
            
            # 全コードのリスト
            all_codes = [p['drug_code'] for p in data['products']]
            logger.info(f"   全コード: {', '.join(all_codes)}")

def main():
    """メイン処理"""
    logger.info("薬剤用量・剤形確認を開始します")
    
    try:
        # 薬剤マスターデータの読み込み
        drug_master = load_drug_master_data()
        if drug_master.is_empty():
            logger.error("薬剤マスターデータの読み込みに失敗しました")
            return
        
        # 全薬剤バリエーションの検索
        results = search_all_drug_variants(drug_master)
        
        # 用量パターンの分析
        analysis = analyze_dosage_patterns(results)
        
        # 推奨事項の生成
        generate_recommendations(analysis)
        
        logger.info("\n用量・剤形確認が完了しました")
        
    except Exception as e:
        logger.error(f"処理中にエラー: {e}")
        raise

if __name__ == "__main__":
    main()
