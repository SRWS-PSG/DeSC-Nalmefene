# DeSC-Nalmefene 解析コード

## 概要

このリポジトリは、DeSCデータベースを用いたアルコール依存症患者に対する飲酒量低減治療の現況・早期介入としての意義・転帰に関する観察研究のための解析コードを提供します。

## 研究目的

1. 飲酒量低減治療を受けている患者のベースライン特性を明らかにする
2. 飲酒量低減治療が行われる医療機関の特徴を把握する
3. 飲酒量低減治療がアルコール依存症への早期介入に寄与しているかを評価する
4. 治療開始前後の肝機能・生活習慣・飲酒行動等の転帰を3群（飲酒量低減目標治療群、断酒目標治療群、治療目標不明群）で比較する

## ディレクトリ構造

```
DeSC-Nalmefene/
├── data/
│   ├── raw/          # 生データへのリンク（バージョン管理外）
│   ├── interim/      # 中間処理データ
│   └── processed/    # 解析用データセット
├── scripts/
│   ├── preprocessing/
│   │   ├── python/   # Python前処理スクリプト
│   │   └── r/        # R前処理スクリプト
│   ├── analysis/     # 解析スクリプト
│   ├── helpers/      # ユーティリティ関数
│   └── validation/   # データ検証関数
├── outputs/
│   ├── logs/         # 処理ログ
│   ├── reports/      # 生成されたレポート
│   ├── figures/      # 生成された図
│   └── tables/       # 生成された表
└── README.md         # プロジェクト概要
```

## 前提条件

### Python環境

- Python 3.9以上
- 必要なパッケージ:
  - polars
  - pyarrow
  - psutil
  - tqdm

### R環境

- R 4.0.0以上
- 必要なパッケージ:
  - dplyr
  - tidyr
  - arrow
  - gtsummary
  - flextable
  - officer
  - mice
  - epiR

## セットアップ

1. リポジトリのクローン:

```bash
git clone https://github.com/SRWS-PSG/DeSC-Nalmefene.git
cd DeSC-Nalmefene
```

2. 必要なディレクトリの作成:

```bash
mkdir -p data/{raw,interim,processed}
mkdir -p outputs/{logs,reports,figures,tables}
```

3. 環境設定:

`.env`ファイルを作成し、データディレクトリを設定します:

```
DATA_ROOT_DIR=/path/to/desc/data  # 実際のデータディレクトリパスに置き換え
OUTPUT_DIR=/path/to/output        # 実際の出力ディレクトリパスに置き換え
```

## 使用方法

### 1. データ前処理

Python前処理スクリプトを実行して、DeSCデータベースから中間データを作成します:

```bash
python scripts/preprocessing/python/process_desc_data.py
```

次に、R前処理スクリプトを実行して、解析用データセットを作成します:

```bash
Rscript scripts/preprocessing/r/create_analysis_datasets.R
```

### 2. 解析の実行

R Markdownを使用して解析レポートを生成します:

```bash
Rscript -e "rmarkdown::render('scripts/analysis/analysis_main.Rmd', output_dir = 'outputs/reports')"
```

または、個別の解析スクリプトを実行します:

```bash
Rscript scripts/analysis/table1_generator.R
```

## 出力ファイル

- `outputs/tables/table1.html`: ベースライン特性比較表（HTML形式）
- `outputs/tables/table1.docx`: ベースライン特性比較表（Word形式）
- `outputs/reports/analysis_main.html`: 解析レポート

## 注意事項

- 実際のDeSCデータは、セキュリティ上の理由からリポジトリには含まれていません。
- データへのアクセスには適切な権限が必要です。
- 大規模なデータセットを処理するため、十分なメモリとストレージが必要です。

## 開発者

- Devin

## ライセンス

このプロジェクトは非公開です。無断での使用・配布は禁止されています。
