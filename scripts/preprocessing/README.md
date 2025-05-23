# DeSC-Nalmefene データ前処理スクリプト

このディレクトリには、DeSC-Nalmefeneプロジェクトのデータ前処理を行うPythonスクリプトが含まれています。

## スクリプト構成

### 1. 対象者抽出スクリプト
**ファイル**: `python/extract_f10_2_patients.py`

**目的**: DeSCデータベースからF10.2（アルコール依存症）患者を抽出し、インデックス日を設定

**主な機能**:
- ICD10マスターからF10.2に対応するレセプト病名コードを取得
- 疾患ファイルから該当患者を抽出
- 各患者の初回診断日（インデックス日）を特定
- ウォッシュアウト期間（52週、26週、156週）を適用した複数のコホートを生成

**出力ファイル**:
- `f10_2_patients_primary_cohort.feather` - Primary cohort（52週ウォッシュアウト）
- `f10_2_patients_sensitivity_cohort1.feather` - Sensitivity cohort 1（26週ウォッシュアウト）
- `f10_2_patients_sensitivity_cohort2.feather` - Sensitivity cohort 2（156週ウォッシュアウト）
- `f10_2_patients_all.feather` - 全患者（ウォッシュアウト適用前）

### 2. 分析用データセット作成スクリプト
**ファイル**: `python/create_analysis_dataset.py`

**目的**: 抽出された患者に研究計画書で定義された全ての変数を結合し、分析用データセットを作成

**主な機能**:
- **基本情報の結合**（tekiyoテーブル）
  - 年齢、性別、保険者種別、家族情報等
- **健診データの時系列結合**（exam_interview_processedテーブル）
  - 初診前直近、初診後直近、翌年、翌々年の4時点
  - 身体計測、血圧、脂質、肝機能、血糖、尿検査、生活習慣項目
- **治療群の分類**
  - 薬剤処方データから飲酒量低減群、断酒群、治療目標不明群を判定
- **併存疾患の取得**
  - 高血圧、糖尿病、脂質異常症、精神疾患の有無

**出力ファイル**:
- `{cohort_name}_cohort_baseline.feather` - ベースライン時点の全変数
- `{cohort_name}_cohort_longitudinal.feather` - 時系列健診データ

### 3. パイプライン実行スクリプト
**ファイル**: `python/run_preprocessing_pipeline.py`

**目的**: 上記2つのスクリプトを順次実行し、全体の前処理パイプラインを管理

**主な機能**:
- 各スクリプトの順次実行
- エラーハンドリングと進捗管理
- 出力ファイルの存在確認
- 実行サマリーレポートの生成

## 実行方法

### 個別実行
```bash
# 1. F10.2患者抽出
python scripts/preprocessing/python/extract_f10_2_patients.py

# 2. 分析用データセット作成
python scripts/preprocessing/python/create_analysis_dataset.py
```

### パイプライン実行（推奨）
```bash
python scripts/preprocessing/python/run_preprocessing_pipeline.py
```

## 研究計画書との対応

作成されるデータセットは、研究計画書のTable 1〜7で必要な以下の変数群を含みます：

### Table 1: 対象者のベースライン特性
- **人口統計**: 年齢、性別、保険者種別
- **家族情報**: 本人/扶養家族、家族ID/親子ID利用可
- **身体計測**: 身長、体重、BMI、腹囲
- **血圧**: 収縮期血圧、拡張期血圧
- **脂質代謝**: HDL、LDL、中性脂肪
- **肝機能**: GOT、GPT、γ-GT、アルブミン、血小板数
- **血糖代謝**: 空腹時血糖、HbA1c
- **尿検査**: 尿糖、尿蛋白
- **生活習慣・問診**: 飲酒頻度・量、喫煙、睡眠、食習慣、運動習慣

### Table 2: 医療機関の特徴
- 医療機関種別、病床規模等（今後実装予定）

### Table 3: 併存疾患・医療利用度
- 高血圧、糖尿病、脂質異常症、精神疾患の有無

### Table 7: 時系列データでの3群比較
- 初診前直近、初診後直近、翌年、翌々年の4時点での全項目

## 出力ディレクトリ構成

```
outputs/
├── logs/
│   ├── extract_f10_2_patients.log
│   ├── create_analysis_dataset.log
│   └── preprocessing_pipeline.log
├── f10_2_patients_primary_cohort.feather
├── f10_2_patients_sensitivity_cohort1.feather
├── f10_2_patients_sensitivity_cohort2.feather
├── f10_2_patients_all.feather
├── primary_cohort_baseline.feather
├── primary_cohort_longitudinal.feather
├── sensitivity1_cohort_baseline.feather
├── sensitivity2_cohort_baseline.feather
└── all_cohort_baseline.feather
```

## 注意事項

### データ要件
- DeSCデータベースの以下のテーブルが必要です：
  - `tekiyo.feather` - 被保険者台帳
  - `exam_interview_processed.feather` - 健診・問診データ
  - `receipt_diseases/` ディレクトリ - 疾患ファイル群
  - `receipt_drug/` ディレクトリ - 薬剤ファイル群
  - `receipt_drug_santei_ymd/` ディレクトリ - 薬剤処方日ファイル群
  - マスターファイル群（m_icd10.feather等）

### 環境要件
- Python 3.8以上
- Polars
- 十分なメモリ（推奨：16GB以上）
- 十分なストレージ容量

### パフォーマンス
- 大規模データセットの処理のため、実行時間は数時間〜数十時間かかる可能性があります
- システムリソースに応じて自動的にパラメータが最適化されます
- テスト用に一部のファイルのみを処理する設定も含まれています

## 次のステップ

前処理完了後は、Rスクリプトでの記述統計・Table生成に進みます：

```bash
Rscript scripts/analysis/table1_generator.R
```

## トラブルシューティング

### よくある問題
1. **メモリ不足**: chunk_sizeパラメータを小さくする
2. **ファイルが見つからない**: .envファイルのパス設定を確認
3. **権限エラー**: 出力ディレクトリの書き込み権限を確認

### ログの確認
詳細なエラー情報は以下のログファイルを参照してください：
- `outputs/logs/extract_f10_2_patients.log`
- `outputs/logs/create_analysis_dataset.log`
- `outputs/logs/preprocessing_pipeline.log`
