import pandas as pd
import json
import os

# Excelファイルのパス
excel_file = "../master/data定義書_table定義書.xlsx"

# Excelファイルを読み込む
xl = pd.ExcelFile(excel_file)

# シート名のリストを取得
sheet_names = xl.sheet_names

# 結果を格納する辞書
results = {"sheet_names": sheet_names, "sheet_samples": {}}

# 各シートの最初の10行を取得
for sheet_name in sheet_names:
    try:
        # シートを読み込む
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)

        # 最初の10行を取得
        sample_data = df.head(10).values.tolist()

        # 結果に追加
        results["sheet_samples"][sheet_name] = sample_data
    except Exception as e:
        results["sheet_samples"][sheet_name] = f"Error: {str(e)}"

# 結果をJSONに変換
output_dir = "."
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

with open(os.path.join(output_dir, "excel_structure.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"シート名のリスト: {sheet_names}")
print("各シートの最初の10行をexcel_structure.jsonに保存しました。")
