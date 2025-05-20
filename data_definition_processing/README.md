# データ定義書処理プロセス

このディレクトリには、Excelの「data定義書」ファイルをAIが参照しやすいJSON形式に変換するためのスクリプトが含まれています。

## 処理の流れ

1. **excel_to_json_converter.py**
   - Excelファイルを読み込み、シートの形式を判定（被保険者台帳形式または調査定義書形式）
   - 各シートのデータを抽出し、JSONに変換
   - 出力: `../master/database_schema_new.json`

2. **create_ai_friendly_json.py**
   - 生成されたJSONファイルを読み込み、AIが参照しやすい形式に変換
   - テーブル名、物理名、カラム情報（名前、物理名、データ型、長さ、主キーフラグ、コメント）を整理
   - 名前が欠落している場合は物理名から自動生成
   - 出力: `../master/ai_friendly_database_schema.json`

3. **optimize_ai_friendly_json.py**
   - AIが参照しやすいJSONファイルをさらに最適化
   - テーブル間の関連性を`../master/er_figure.md`で定義されたER図に基づいて追加
   - カラム情報をキー・バリュー形式に変換して検索しやすく
   - スキーマ全体の統計情報を追加
   - 出力: `../master/optimized_database_schema.json`

## 統合実行スクリプト

すべての処理を一度に実行するための統合スクリプトが用意されています。

```bash
python process_excel.py [オプション]
```

### オプション

- `--config <ファイルパス>`: 設定ファイルのパス（デフォルト: config.json）
- `--excel <ファイルパス>`: Excelファイルのパス（設定ファイルより優先）
- `--output-dir <ディレクトリパス>`: 出力ディレクトリ（設定ファイルより優先）
- `--skip-steps <ステップ名>`: スキップするステップ（convert, friendly, optimize）

例：
```bash
# すべての処理を実行
python process_excel.py

# 最適化のみスキップ
python process_excel.py --skip-steps optimize

# 別のExcelファイルを処理
python process_excel.py --excel "../other_dir/another_definition.xlsx"
```

## 設定ファイル

`config.json` には、入出力ファイルのパスやログ設定、データ型マッピングなどの設定が含まれています。必要に応じて編集してください。

```json
{
  "input_paths": {
    "excel_file": "../master/data定義書_table定義書.xlsx"
  },
  "output_paths": {
    "database_schema": "../master/database_schema_new.json",
    "ai_friendly_schema": "../master/ai_friendly_database_schema.json",
    "optimized_schema": "../master/optimized_database_schema.json"
  },
  "logging": {
    "level": "INFO",
    "file": "excel_processing.log",
    "console": true
  }
}
```

## 生成されるファイル

- `../master/database_schema_new.json`: 元のExcelファイルの内容をそのままJSONに変換したもの
- `../master/ai_friendly_database_schema.json`: AIが参照しやすい形式に変換したもの
- `../master/optimized_database_schema.json`: テーブル間の関連性を追加し、さらに最適化したもの
- `excel_processing.log`: 処理ログ

## 機能強化

- **エラーハンドリングの強化**: 詳細なエラー情報とスタックトレースを記録
- **データ型の正規化**: 異なる表記のデータ型を標準形式に変換
- **テーブル・カラム情報の補完**: 欠落した情報を自動的に補完
- **関連性検出の強化**: 複数のパターンに基づいてテーブル間の関連性を推測
- **スキーマ統計情報**: テーブル数、カラム数、データ型分布などの統計情報を提供
- **処理時間の計測**: 各処理ステップの実行時間を記録

## 注意点

- 一部のシートは形式が異なるため、処理に失敗する場合があります（参考情報のシートなど）
- テーブル間の関連性は`../master/er_figure.md`で定義されたER図に基づいて設定しています
- 今後、新しい形式のシートが追加された場合は、判定ロジックの追加が必要になる可能性があります
- 処理中に問題が発生した場合は、ログファイル（excel_processing.log）を確認してください
