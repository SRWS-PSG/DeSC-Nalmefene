import json
import os
import datetime
from typing import Dict, List, Any, Optional, Set, Tuple

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


def optimize_ai_friendly_json(input_json_path: str, output_json_path: str) -> None:
    """
    AIが参照しやすいJSONファイルをさらに最適化する

    Args:
        input_json_path: 入力JSONファイルのパス
        output_json_path: 出力JSONファイルのパス
    """
    # JSONファイルを読み込む
    data = load_json(input_json_path)

    # 最適化されたJSONデータ
    optimized_data = {
        "tables": {},
        "metadata": data.get("metadata", {}),
        "schema_info": {
            "table_count": len(data.get("tables", {})),
            "processed_at": datetime.datetime.now().isoformat(),
        },
    }

    # テーブル情報を抽出
    for physical_name, table_info in data.get("tables", {}).items():
        table_name = table_info.get("table_name", "")

        # テーブル情報を追加
        optimized_data["tables"][physical_name] = {
            "table_name": table_name,
            "physical_name": physical_name,
            "comment": table_info.get("comment", ""),
            "primary_key_count": table_info.get("primary_key_count", 0),
            "column_count": table_info.get("column_count", 0),
            "data_type_distribution": table_info.get("data_type_distribution", {}),
            "columns": {},
        }

        # カラム情報を追加
        for column in table_info.get("columns", []):
            column_physical_name = column.get("physical_name", "")
            if column_physical_name:
                optimized_data["tables"][physical_name]["columns"][
                    column_physical_name
                ] = {
                    "name": column.get("name", ""),
                    "type": column.get("type", ""),
                    "length": column.get("length", ""),
                    "primary_key": column.get("primary_key", False),
                    "required": column.get("required", False),
                    "comment": column.get("comment", ""),
                }

    # テーブル間の関連性をER図に基づいて追加
    add_table_relationships(optimized_data)

    # スキーマ全体を最適化
    optimized_data = optimize_schema(optimized_data)

    # JSONファイルに保存
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(optimized_data, f, ensure_ascii=False, indent=2)

    logger.info(f"最適化されたJSONファイルを {output_json_path} に保存しました")


def add_table_relationships(data: Dict[str, Any]) -> None:
    """
    ER図に基づいてテーブル間の関連性を追加する

    Args:
        data: 最適化するデータ
    """
    # ER図ファイルを読み込む
    er_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "master", "er_figure.md"
    )
    logger.info(f"ER図ファイル '{er_file_path}' を読み込みます")

    try:
        with open(er_file_path, "r", encoding="utf-8") as f:
            er_content = f.read()
    except Exception as e:
        logger.error(f"ER図ファイルの読み込みに失敗しました: {e}")
        logger.warning("ER図からの関連性設定をスキップします")
        return

    # ER図から関連性を抽出
    relationships_map = extract_relationships_from_er(er_content)

    # 各テーブルに関連性を追加
    for table_name, table_info in data["tables"].items():
        # 日本語名からテーブル名を検索
        japanese_name = table_info.get("table_name", "")

        # 関連性を追加
        relationships = []

        # 日本語名がER図に存在する場合
        if japanese_name in relationships_map:
            for rel in relationships_map[japanese_name]:
                # 関連先のテーブルの物理名を検索
                to_table_physical = find_physical_name_by_japanese(
                    data["tables"], rel["to_table"]
                )

                if to_table_physical:
                    # 主キーカラムを取得
                    from_column = get_primary_key_column(table_info)
                    to_column = get_primary_key_column(
                        data["tables"].get(to_table_physical, {})
                    )

                    if from_column and to_column:
                        relationships.append(
                            {
                                "from_column": from_column,
                                "to_table": to_table_physical,
                                "to_column": to_column,
                                "relationship_type": rel["type"],
                                "confidence": "high",
                            }
                        )

        # 関連性が存在する場合のみ追加
        if relationships:
            table_info["relationships"] = relationships


def extract_relationships_from_er(er_content: str) -> Dict[str, List[Dict[str, str]]]:
    """
    ER図から関連性を抽出する

    Args:
        er_content: ER図の内容

    Returns:
        関連性のマップ
    """
    relationships_map = {}

    # 関連性の行を抽出
    relationship_lines = []
    in_relationships_section = False

    for line in er_content.split("\n"):
        line = line.strip()

        # 関連性セクションの開始を検出
        if line == "%% Relationships":
            in_relationships_section = True
            continue

        # 関連性セクション内の行を処理
        if in_relationships_section and line and not line.startswith("```"):
            relationship_lines.append(line)

    # 関連性を解析
    for line in relationship_lines:
        # 矢印表記を検出
        if "-->" in line:
            parts = line.split("-->")
            from_table = parts[0].strip()
            to_table = parts[1].strip()

            if from_table not in relationships_map:
                relationships_map[from_table] = []

            relationships_map[from_table].append(
                {"to_table": to_table, "type": "one_to_many"}
            )

        # コンポジション表記を検出
        elif "*--" in line:
            parts = line.split("*--")
            from_table = parts[0].strip()
            to_table = parts[1].strip()

            if from_table not in relationships_map:
                relationships_map[from_table] = []

            relationships_map[from_table].append(
                {"to_table": to_table, "type": "composition"}
            )

    return relationships_map


def find_physical_name_by_japanese(
    tables: Dict[str, Any], japanese_name: str
) -> Optional[str]:
    """
    日本語名から物理名を検索する

    Args:
        tables: テーブル情報
        japanese_name: 日本語名

    Returns:
        物理名（見つからない場合はNone）
    """
    for physical_name, table_info in tables.items():
        if table_info.get("table_name", "") == japanese_name:
            return physical_name
    return None


def get_primary_key_column(table_info: Dict[str, Any]) -> Optional[str]:
    """
    テーブルの主キーカラムを取得する

    Args:
        table_info: テーブル情報

    Returns:
        主キーカラム名（見つからない場合はNone）
    """
    if not table_info or "columns" not in table_info:
        return None

    for column_name, column_info in table_info["columns"].items():
        if column_info.get("primary_key", False):
            return column_name

    return None


def optimize_schema(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    スキーマ全体を最適化する

    Args:
        data: 最適化するデータ

    Returns:
        最適化されたデータ
    """
    # テーブルの統計情報を追加
    data["schema_info"]["column_counts"] = {}
    data["schema_info"]["primary_key_counts"] = {}
    data["schema_info"]["data_types"] = {}
    data["schema_info"]["relationship_counts"] = {}

    # 統計情報の集計
    for table_name, table_info in data["tables"].items():
        # カラム数
        column_count = len(table_info["columns"])
        data["schema_info"]["column_counts"][table_name] = column_count

        # 主キーの数
        pk_count = sum(
            1
            for col_info in table_info["columns"].values()
            if col_info.get("primary_key")
        )
        data["schema_info"]["primary_key_counts"][table_name] = pk_count

        # データ型の分布
        for col_info in table_info["columns"].values():
            data_type = col_info.get("type", "unknown")
            if data_type not in data["schema_info"]["data_types"]:
                data["schema_info"]["data_types"][data_type] = 0
            data["schema_info"]["data_types"][data_type] += 1

        # 関連性の数
        if "relationships" in table_info:
            data["schema_info"]["relationship_counts"][table_name] = len(
                table_info["relationships"]
            )

    # 全体の統計情報
    data["schema_info"]["total_columns"] = sum(
        data["schema_info"]["column_counts"].values()
    )
    data["schema_info"]["total_primary_keys"] = sum(
        data["schema_info"]["primary_key_counts"].values()
    )
    data["schema_info"]["total_relationships"] = sum(
        data["schema_info"]["relationship_counts"].values()
        if data["schema_info"]["relationship_counts"]
        else [0]
    )

    # テーブル間の関連性グラフを追加
    data["schema_info"]["relationship_graph"] = generate_relationship_graph(
        data["tables"]
    )

    return data


def generate_relationship_graph(tables: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    テーブル間の関連性グラフを生成する

    Args:
        tables: テーブル情報

    Returns:
        関連性グラフ
    """
    graph = {}

    # 各テーブルの関連テーブルを集計
    for table_name, table_info in tables.items():
        graph[table_name] = []

        # 関連性がある場合
        if "relationships" in table_info:
            for rel in table_info["relationships"]:
                related_table = rel["to_table"]
                if related_table not in graph[table_name]:
                    graph[table_name].append(related_table)

    return graph


def main():
    # 設定ファイルを読み込む
    config = get_config()

    # 入力ファイルと出力ファイルのパス
    input_json_path = config["input_paths"]["ai_friendly_schema"]
    output_json_path = config["output_paths"]["optimized_schema"]

    # AIが参照しやすいJSONファイルを最適化
    optimize_ai_friendly_json(input_json_path, output_json_path)


if __name__ == "__main__":
    main()
