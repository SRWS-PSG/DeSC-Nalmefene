#!/bin/bash

set -e

echo "DeSC-Nalmefene 解析パイプラインを開始します"
echo "実行日時: $(date)"

echo "ディレクトリ構造を確認しています..."
mkdir -p data/{raw,interim,processed}
mkdir -p outputs/{logs,reports,figures,tables}
mkdir -p scripts/{preprocessing/{python,r},analysis,helpers,validation}

if [ -f .env ]; then
    echo ".envファイルから環境変数を読み込みます"
    source .env
else
    echo "警告: .envファイルが見つかりません。デフォルト設定を使用します。"
    export DATA_ROOT_DIR="./data/raw"
    export OUTPUT_DIR="./outputs"
fi

echo "Python前処理スクリプトを実行しています..."
if [ -f scripts/preprocessing/python/process_desc_data.py ]; then
    python scripts/preprocessing/python/process_desc_data.py
else
    echo "エラー: Python前処理スクリプトが見つかりません"
    exit 1
fi

echo "R前処理スクリプトを実行しています..."
if [ -f scripts/preprocessing/r/create_analysis_datasets.R ]; then
    Rscript scripts/preprocessing/r/create_analysis_datasets.R
else
    echo "エラー: R前処理スクリプトが見つかりません"
    exit 1
fi

echo "Table 1生成スクリプトを実行しています..."
if [ -f scripts/analysis/table1_generator.R ]; then
    Rscript scripts/analysis/table1_generator.R
else
    echo "エラー: Table 1生成スクリプトが見つかりません"
    exit 1
fi

echo "解析レポートを生成しています..."
if [ -f scripts/analysis/analysis_main.Rmd ]; then
    Rscript -e "rmarkdown::render('scripts/analysis/analysis_main.Rmd', output_dir = 'outputs/reports')"
else
    echo "エラー: 解析レポートRmdファイルが見つかりません"
    exit 1
fi

echo "DeSC-Nalmefene 解析パイプラインが完了しました"
echo "出力ファイルは以下の場所にあります:"
echo "- Table 1: outputs/tables/table1.html, outputs/tables/table1.docx"
echo "- 解析レポート: outputs/reports/analysis_main.html"
echo "終了日時: $(date)"
