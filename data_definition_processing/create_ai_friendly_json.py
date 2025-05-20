import json
import os
import datetime
from typing import Dict, List, Any, Optional

from logger import setup_logger, get_config

# 設定とロガーの取得
config = get_config()
logger = setup_logger()


def load_json(file_path: str) -> Dict[str, Any]:
    """
    JSONファイルを読み込む

    Args:
        file_path: JSONファイルのパス

    Returns:
        JSONデータ
    """
    logger.info(f"JSONファイル '{file_path}' を読み込みます")
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def create_ai_friendly_json(input_json_path: str, output_json_path: str) -> None:
    """
    AIが参照しやすいJSONファイルを作成する

    Args:
        input_json_path: 入力JSONファイルのパス
        output_json_path: 出力JSONファイルのパス
    """
    # JSONファイルを読み込む
    data = load_json(input_json_path)

    # AIが参照しやすい形式に変換
    ai_friendly_data = {
        "tables": {},
        "metadata": {
            "failed_sheets": data.get("failed_sheets", []),
            "skipped_sheets": data.get("skipped_sheets", []),
            "summary": data.get("summary", {}),
            "processed_at": datetime.datetime.now().isoformat(),
        },
    }

    # テーブル情報を抽出
    for sheet_name, sheet_data in data.get("tables", {}).items():
        sheet_type = sheet_data.get("sheet_type")

        if sheet_type == "insured_ledger":
            table_name = sheet_data.get("table_name", sheet_name)
            physical_name = sheet_data.get("physical_name", "")

            if not physical_name:
                physical_name = generate_physical_name(table_name)
                logger.warning(
                    f"テーブル '{table_name}' の物理名が見つからないため、生成しました: {physical_name}"
                )

            # テーブル情報を追加
            table_info = {
                "table_name": table_name,
                "physical_name": physical_name,
                "comment": sheet_data.get("comment", ""),
                "columns": [],
            }

            # カラム情報を追加
            for column in sheet_data.get("columns", []):
                column_info = {
                    "name": column.get("項目名", ""),
                    "physical_name": column.get("物理項目名", ""),
                    "type": column.get("type", ""),
                    "length": column.get("length", ""),
                    "primary_key": True if column.get("PK") == "○" else False,
                    "comment": column.get("備考", ""),
                }

                # 名前が空の場合、物理名から生成
                if not column_info["name"] and column_info["physical_name"]:
                    column_info["name"] = generate_display_name(
                        column_info["physical_name"]
                    )

                # コメントから追加情報を抽出
                if column_info["comment"]:
                    column_info["required"] = is_required_field(column_info["comment"])

                table_info["columns"].append(column_info)

            # テーブル情報を追加
            ai_friendly_data["tables"][physical_name] = enhance_table_info(table_info)

        elif sheet_type == "survey_definition":
            # 調査定義書の情報を追加
            metadata = sheet_data.get("metadata", {})
            ai_friendly_data["metadata"]["survey_definition"] = metadata

        elif sheet_type == "custom_format":
            # カスタム形式の情報を追加
            logger.info(f"カスタム形式のシート '{sheet_name}' の情報を追加します")
            ai_friendly_data["metadata"]["custom_format"] = ai_friendly_data[
                "metadata"
            ].get("custom_format", {})
            ai_friendly_data["metadata"]["custom_format"][sheet_name] = sheet_data

    # メタデータを強化
    ai_friendly_data["metadata"] = enhance_metadata(ai_friendly_data["metadata"])

    # JSONファイルに保存
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(ai_friendly_data, f, ensure_ascii=False, indent=2)

    logger.info(f"AIが参照しやすいJSONファイルを {output_json_path} に保存しました")


def generate_physical_name(display_name: str) -> str:
    """
    表示名から物理名を生成する

    Args:
        display_name: 表示名

    Returns:
        生成された物理名
    """
    # スペースや特殊文字を削除
    physical_name = "".join(c for c in display_name if c.isalnum() or c == " ")

    # スペースをアンダースコアに置換
    physical_name = physical_name.replace(" ", "_")

    # 小文字に変換
    physical_name = physical_name.lower()

    return physical_name


def generate_display_name(physical_name: str) -> str:
    """
    物理名から表示名を生成する

    Args:
        physical_name: 物理名

    Returns:
        生成された表示名
    """
    # アンダースコアをスペースに置換
    display_name = physical_name.replace("_", " ")

    # 単語の先頭を大文字に
    display_name = " ".join(word.capitalize() for word in display_name.split())

    return display_name


def is_required_field(comment: str) -> bool:
    """
    コメントから必須フィールドかどうかを判定する

    Args:
        comment: コメント

    Returns:
        必須フィールドの場合はTrue、そうでない場合はFalse
    """
    required_keywords = ["必須", "required", "not null", "notnull"]
    comment_lower = comment.lower()

    for keyword in required_keywords:
        if keyword in comment_lower:
            return True

    return False


def enhance_table_info(table_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    テーブル情報を強化する

    Args:
        table_info: テーブル情報

    Returns:
        強化されたテーブル情報
    """
    # 主キーの数をカウント
    primary_key_count = sum(1 for col in table_info["columns"] if col["primary_key"])

    # 主キー情報を追加
    table_info["primary_key_count"] = primary_key_count

    # カラム数を追加
    table_info["column_count"] = len(table_info["columns"])

    # データ型の分布を追加
    data_types = {}
    for col in table_info["columns"]:
        data_type = col.get("type", "unknown")
        if data_type not in data_types:
            data_types[data_type] = 0
        data_types[data_type] += 1

    table_info["data_type_distribution"] = data_types

    return table_info


def enhance_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    メタデータを強化する

    Args:
        metadata: メタデータ

    Returns:
        強化されたメタデータ
    """
    # 処理日時の追加
    metadata["processed_at"] = datetime.datetime.now().isoformat()

    # 失敗率の計算
    if "failed_sheets" in metadata:
        metadata["failure_stats"] = {
            "total_failed": len(metadata["failed_sheets"]),
            "failure_reasons": {},
        }

        # 失敗理由の集計
        for sheet in metadata["failed_sheets"]:
            reason = sheet.get("reason", "unknown")
            if reason not in metadata["failure_stats"]["failure_reasons"]:
                metadata["failure_stats"]["failure_reasons"][reason] = 0
            metadata["failure_stats"]["failure_reasons"][reason] += 1

    return metadata


def main():
    # 設定ファイルを読み込む
    config = get_config()

    # 入力ファイルと出力ファイルのパス
    input_json_path = config["input_paths"]["database_schema"]
    output_json_path = config["output_paths"]["ai_friendly_schema"]

    # AIが参照しやすいJSONファイルを作成
    create_ai_friendly_json(input_json_path, output_json_path)


if __name__ == "__main__":
    main()
