# Polarsを使ったDeSCデータ前処理

このディレクトリには、DeSCデータベースの処方箋および調剤データに関する前処理スクリプトと関連ファイルが含まれています。この文書では、Polarsライブラリを使用した効率的なデータ処理方法と、各処理ステップのコード例を紹介します。
Polarsと.featherの組み合わせで、ローカルPCでも許容範囲なスピードで処理できます。
各自の環境でどこまでうまくいくかは、実際にやってみながら確認してみてください。

## 目的

この前処理の主な目的は、**処方箋が発行されたが薬局で調剤されなかったケース**を特定することです。このデータは、処方箋コンプライアンスや薬剤アドヒアランスの研究に役立ちます。

## データソース

### マスターデータ

処理には以下のマスターデータ（featherファイル形式）を使用しています：

- `m_med_treat_all.feather`: 医療行為のマスターデータ
- `m_disease.feather`: 疾病コードのマスターデータ
- `m_drug_*.feather`: 薬剤関連のマスターデータ
- その他の`m_*.feather`ファイル

### 処方・調剤コード

処方箋および調剤に関連する医療行為コードは以下のファイルで管理されています：

- `m_med_treat_all_unique.csv`: 処方関連の医療行為コード
- `chozai.csv`: 調剤関連の医療行為コード

## 処理フローとコード例

### 1. Polarsの基本設定と初期データ読み込み

```python
import polars as pl

# 表示設定の最適化（大規模データセット向け）
pl.Config.set_tbl_rows(-1)  # すべての行を表示
pl.Config.set_tbl_cols(-1)  # すべての列を表示
pl.Config.set_fmt_str_lengths(1000)  # 文字列の長さ制限を設定

# マスターデータ読み込み
def read_feather(file):
    print(file)
    df = pl.read_ipc(file)
    print(df.head())
    return df

# 複数のマスターファイルを読み込み
masters = glob.glob('m_*.feather')
for master in masters:
    read_feather(master)
```

### 2. 処方・調剤コードの抽出

```python
# 医療行為マスターから処方関連コードを抽出
df = pl.read_ipc('m_med_treat_all.feather')
print(df.columns)

# 処方を含む行を抽出
df = df.filter(pl.col('medical_practice_name').str.contains('処方', literal=True))

# ユニークな値のみを取得して保存
df_unique = df.select([
    'medical_practice_code',
    'medical_practice_name'
]).unique(
    subset=['medical_practice_name']
)

print(df_unique.shape)
df_unique.write_csv('m_med_treat_all_unique.csv')

# 同様に調剤関連コードも抽出
df = df.filter(pl.col('medical_practice_name').str.contains('調剤', literal=True))
df_unique = df.select([
    'medical_practice_code',
    'medical_practice_name'
]).unique(
    subset=['medical_practice_name']
)
df_unique.write_csv('chozai.csv')
```

### 3. 大規模レセプトデータの効率的処理

```python
import polars as pl
import os
from typing import Set, List, Union
from tqdm import tqdm
import glob
import psutil

def process_receipt_files(
    file_pattern: str,
    receByomei_series: Union[pd.Series, pl.Series],
    output_path: str = "processed_receipts.feather",
    batch_size: int = 20,
    chunk_size: int = 500_000,
    n_threads: int = 8
) -> None:
    # Polarsのグローバル設定を最適化
    pl.Config.set_fmt_str_lengths(50)
    pl.Config.set_tbl_rows(None)
    
    # Pandas SeriesをPolars Seriesに変換
    if isinstance(receByomei_series, pd.Series):
        receByomei_series = pl.Series(receByomei_series.name, receByomei_series.values)
    
    # receByomei_codesのキャッシュを最適化
    receByomei_codes: Set[str] = set(receByomei_series.cast(pl.Utf8).to_list())
    receipt_files: List[str] = [f for f in glob.glob(file_pattern) if os.path.isfile(f)]
    
    if not receipt_files:
        print("No files found matching the pattern")
        return
        
    if os.path.exists(output_path):
        os.remove(output_path)

    # バッチ処理の最適化
    for i in tqdm(range(0, len(receipt_files), batch_size), desc="Processing batches"):
        batch_files = receipt_files[i:i + batch_size]
        process_batch(
            batch_files, 
            receByomei_codes, 
            output_path, 
            chunk_size,
            n_threads
        )
```

### 4. バッチ処理の実装

```python
def process_batch(
    batch_files: List[str],
    receByomei_codes: Set[str],
    output_path: str,
    chunk_size: int,
    n_threads: int
) -> None:
    batch_results = []
    
    for file_path in batch_files:
        try:
            # Lazy実行とメモリマッピングを最適化
            df_lazy = (pl.scan_ipc(
                source=file_path,
                memory_map=True,
                n_rows=chunk_size
            )
            .select([
                "receipt_id",
                "receipt_ym",
                "kojin_id",
                "medical_practice_code"
            ]))
            
            # 並列処理の最適化
            result = (df_lazy
                .with_columns(pl.col("medical_practice_code").cast(pl.Utf8))
                .filter(pl.col("medical_practice_code").is_in(receByomei_codes))
                .group_by("receipt_id")
                .agg([
                    pl.col("receipt_ym").first(),
                    pl.col("kojin_id").first(),
                    pl.col("medical_practice_code").str.concat(",").alias("practice_codes")
                ])
                .collect(streaming=True, n_threads=n_threads))
            
            if not result.is_empty():
                batch_results.append(result)
            
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            continue
    
    if batch_results:
        save_results(batch_results, output_path)
```

### 5. 結果の保存と集約

```python
def save_results(
    batch_results: List[pl.DataFrame],
    output_path: str
) -> None:
    try:
        # 結果の結合
        combined_df = pl.concat(batch_results)
        
        # receipt_idでの重複排除
        final_df = (combined_df
            .unique(subset=["receipt_id"])
            .sort("receipt_id"))
        
        # 既存ファイルがある場合は追加
        if os.path.exists(output_path):
            existing_df = pl.read_ipc(output_path)
            final_df = (pl.concat([existing_df, final_df])
                .unique(subset=["receipt_id"])
                .sort("receipt_id"))
        
        # 圧縮して保存
        final_df.write_ipc(output_path, compression="zstd")
        
    except Exception as e:
        print(f"Error saving results: {str(e)}")
```

### 6. 日付データの結合

```python
def process_medical_data(result_df, base_dir):
    """
    診察日（santei_ymd）情報の処理と結合
    """
    # 一意の日付を取得
    unique_dates = result_df['receipt_ym'].unique().to_list()
    
    # バッチ処理でデータ処理
    batch_size = 10
    for i in range(0, len(unique_dates), batch_size):
        batch_dates = unique_dates[i:i+batch_size]
        processed_dfs = []
        
        for ym in tqdm(batch_dates, desc=f"Processing batch {i//batch_size + 1}"):
            ym_formatted = ym.replace('/', '')
            feather_path = os.path.join(
                base_dir,
                f"receipt_medical_practice_santei_ymd_{ym_formatted}.feather"
            )
            
            if not os.path.exists(feather_path):
                print(f"Warning: File not found - {feather_path}")
                continue
            
            # 日付データ読み込みと結合
            santei_df = pl.read_ipc(feather_path)
            santei_df = santei_df.with_columns(
                pl.col("receipt_id").cast(pl.Int64)
            )
            
            current_month_df = result_df.filter(pl.col("receipt_ym") == ym)
            current_receipt_ids = current_month_df["receipt_id"].unique()
            
            filtered_santei = (
                santei_df
                .filter(pl.col("receipt_id").is_in(current_receipt_ids))
                .select(["receipt_id", "santei_ymd"])
            )
            
            merged_df = current_month_df.join(
                filtered_santei,
                on="receipt_id",
                how="left"
            )
            
            processed_dfs.append(merged_df)
            
            # メモリ解放
            del santei_df, filtered_santei, current_month_df
            gc.collect()
```

### 7. 病名データの構造化処理

```python
def process_month_optimized(df_receipt_month, disease_file_path):
    """病名データの処理と構造化"""
    
    # 病名データの効率的な読み込みと集約
    diseases_df = (
        pl.scan_ipc(disease_file_path)
        .select([
            'receipt_id',
            'diseases_code',
            'sinryo_start_ymd',
            'tenki_kbn_code'
        ])
        .group_by('receipt_id')
        .agg([
            pl.col('diseases_code').alias('diseases_code_list'),
            pl.col('sinryo_start_ymd').alias('sinryo_start_ymd_list'),
            pl.col('tenki_kbn_code').alias('tenki_kbn_code_list')
        ])
        .collect()
    )
    
    # 構造化データの作成（JSONライクな形式）
    result = (
        df_receipt_month
        .join(
            diseases_df,
            on='receipt_id',
            how='left'
        )
        .with_columns([
            pl.struct([
                'diseases_code_list',
                'sinryo_start_ymd_list',
                'tenki_kbn_code_list'
            ]).map_elements(lambda x: {
                'diseases_code': x['diseases_code_list'] if x['diseases_code_list'] is not None else [],
                'sinryo_start_ymd': x['sinryo_start_ymd_list'] if x['sinryo_start_ymd_list'] is not None else [],
                'tenki_kbn_code': x['tenki_kbn_code_list'] if x['tenki_kbn_code_list'] is not None else []
            }).alias('disease_info')
        ])
    )
    
    return result
```

### 8. 薬局調剤データの処理と結合

```python
def process_single_month(df_base: pl.DataFrame, target_ym: str, pharmacy_path: Path) -> pl.DataFrame:
    """処方日と患者IDに基づく薬局調剤データの結合"""
    
    # 処理対象の月のデータをフィルタリング
    df_month = df_base.filter(pl.col("receipt_ym") == target_ym)
    
    # 当月と翌月の薬局データを読み込む（処方から調剤までのタイムラグを考慮）
    target_ym_path = target_ym.replace("/", "")
    next_ym_path = get_next_month(target_ym_path)
    
    pharmacy_dfs = []
    
    # 当月の薬局データ
    current_pharmacy_file = pharmacy_path / f"receipt_drug_santei_ymd_{target_ym_path}.feather"
    if current_pharmacy_file.exists():
        pharmacy_dfs.append(pl.scan_ipc(str(current_pharmacy_file)))
    
    # 翌月の薬局データ
    next_pharmacy_file = pharmacy_path / f"receipt_drug_santei_ymd_{next_ym_path}.feather"
    if next_pharmacy_file.exists():
        pharmacy_dfs.append(pl.scan_ipc(str(next_pharmacy_file)))
    
    if not pharmacy_dfs:
        return df_month  # 薬局データがない場合は元のデータを返す
    
    # 薬局データの結合
    df_pharmacy = (
        pl.concat(pharmacy_dfs)
        .select([
            "kojin_id",
            "shohou_ymd",
            pl.col("dispensing_ymd").alias("pharmacy_dispensing_ymd")
        ])
        .group_by(["kojin_id", "shohou_ymd"])
        .agg([pl.col("pharmacy_dispensing_ymd")])
    )
    
    # 基本データと薬局データの結合
    result = (
        df_month
        .join(
            df_pharmacy,
            left_on=["kojin_id", "santei_ymd"],
            right_on=["kojin_id", "shohou_ymd"],
            how="left"
        )
    )
    
    return result
```

### 9. システムリソースに基づく最適化

```python
def optimize_parameters():
    """システムリソースに基づく最適なパラメータの設定"""
    # CPUコア数の75%を使用
    n_threads = max(1, int(psutil.cpu_count(logical=True) * 0.75))
    
    # 利用可能なメモリの30%をチャンクサイズとして使用（最大500,000）
    available_memory = psutil.virtual_memory().available
    chunk_size = min(500_000, int(available_memory * 0.3 / 1024))
    
    # バッチサイズはCPUコア数の2-3倍に設定
    batch_size = n_threads * 2
    
    return {
        'n_threads': n_threads,
        'chunk_size': chunk_size,
        'batch_size': batch_size
    }
```

## 出力データセット

最終的な出力データセットには以下の主要な列が含まれます：

- `receipt_id`: レセプトID
- `receipt_ym`: レセプト年月
- `kojin_id`: 患者ID
- `santei_ymd`: 診察日
- `medical_practice_codes`: 医療行為コードのリスト
- `disease_info`: 病名情報（疾病コード、診断開始日など）
- `pharmacy_dispensing_ymd`: 薬局調剤日（調剤されなかった場合はNull）

このデータセットを用いて、処方箋が発行されたが調剤されなかったケースを特定し、その要因や傾向を分析することができます。

## 使用方法

以下のように処理を実行できます：

```python
# パラメータの最適化
params = optimize_parameters()

# レセプトデータの処理
process_receipt_files(
    file_pattern=f'{base_path}:/DeSC_v2/*.feather',
    receByomei_series=receByomei,  # PandasまたはPolars Series
    output_path="processed_receipts.feather",
    batch_size=params['batch_size'],
    chunk_size=params['chunk_size'],
    n_threads=params['n_threads']
)

# 日付データの結合
result_df = pl.read_ipc("processed_receipts.feather")
processed_df = process_medical_data(result_df, f'{base_path}:/DeSC_v2/receipt_medical_practice_santei_ymd')

# 病名データと薬局データの処理
# ...（前述のコード例を参照）
```

## 注意事項

- 大規模なデータセットを処理するため、十分なメモリとストレージが必要です
- 処理時間は使用するハードウェアとデータサイズによって異なります
- 中間ファイルは処理終了後に削除するか、適切に管理してください
- メモリ使用量を監視するためのユーティリティ関数を活用してください：

```python
import psutil

def print_memory_usage():
    process = psutil.Process()
    print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")
