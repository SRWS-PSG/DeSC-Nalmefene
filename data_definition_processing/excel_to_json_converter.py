import pandas as pd
import json
import os
import logging
import traceback
from typing import Dict, List, Any, Tuple, Optional, Set

from logger import setup_logger, get_config

# 設定とロガーの取得
config = get_config()
logger = setup_logger()


class ExcelToJsonConverter:
    def __init__(
        self,
        excel_file_path: str,
        output_json_path: str,
        config_path: str = "config.json",
    ):
        """
        Excelファイルを読み込み、JSONに変換するクラスの初期化

        Args:
            excel_file_path: Excelファイルのパス
            output_json_path: 出力するJSONファイルのパス
            config_path: 設定ファイルのパス
        """
        self.excel_file_path = excel_file_path
        self.output_json_path = output_json_path
        self.result_data = {}
        self.failed_sheets = []
        self.processed_sheets = 0
        self.skipped_sheets = []

        # 設定を読み込む
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

    def process(self) -> None:
        """
        Excelファイルを処理してJSONに変換する
        """
        try:
            # Excelファイルを読み込む
            xl = pd.ExcelFile(self.excel_file_path)
            sheet_names = xl.sheet_names

            logger.info(f"処理開始: {len(sheet_names)}シートを処理します")

            # 各シートを処理
            for sheet_name in sheet_names:
                try:
                    logger.info(f"シート '{sheet_name}' の処理を開始します")

                    # 特定のシートをスキップ（例：目次、説明シートなど）
                    if self._should_skip_sheet(sheet_name):
                        logger.info(f"シート '{sheet_name}' はスキップします")
                        self.skipped_sheets.append(sheet_name)
                        continue

                    # シートを読み込む
                    df = pd.read_excel(
                        self.excel_file_path, sheet_name=sheet_name, header=None
                    )

                    # シートの形式を判定して処理
                    self._process_sheet(sheet_name, df)

                except Exception as e:
                    error_details = traceback.format_exc()
                    logger.error(
                        f"シート '{sheet_name}' の処理中にエラーが発生しました: {str(e)}\n{error_details}"
                    )
                    self.failed_sheets.append(
                        {
                            "sheet_name": sheet_name,
                            "reason": str(e),
                            "details": error_details,
                        }
                    )

            # 結果をJSONに保存
            self._save_to_json()

            # 処理結果のサマリーを出力
            self._print_summary()

        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(f"処理中にエラーが発生しました: {str(e)}\n{error_details}")
            raise

    def _should_skip_sheet(self, sheet_name: str) -> bool:
        """
        処理をスキップすべきシートかどうかを判定する

        Args:
            sheet_name: シート名

        Returns:
            スキップすべき場合はTrue、そうでない場合はFalse
        """
        # スキップするシート名のパターン
        skip_patterns = ["目次", "説明", "概要", "使い方", "README", "更新履歴"]

        # パターンに一致するかチェック
        for pattern in skip_patterns:
            if pattern in sheet_name:
                return True

        return False

    def _process_sheet(self, sheet_name: str, df: pd.DataFrame) -> None:
        """
        シートを処理する

        Args:
            sheet_name: シート名
            df: データフレーム
        """
        # シートの形式を判定して処理
        if self._is_survey_definition_format(df):
            logger.info(f"シート '{sheet_name}' は調査定義書形式として処理します")
            self._process_survey_definition_sheet(sheet_name, df)
            self.processed_sheets += 1
        elif self._is_insured_ledger_format(df):
            logger.info(f"シート '{sheet_name}' は被保険者台帳形式として処理します")
            self._process_insured_ledger_sheet(sheet_name, df)
            self.processed_sheets += 1
        elif self._is_custom_format(df):
            logger.info(f"シート '{sheet_name}' はカスタム形式として処理します")
            self._process_custom_format_sheet(sheet_name, df)
            self.processed_sheets += 1
        else:
            # 形式を判別できない場合、詳細な理由を提供
            failed_reason = self._analyze_sheet_format(df)
            logger.warning(
                f"シート '{sheet_name}' の形式を判別できませんでした: {failed_reason}"
            )
            self.failed_sheets.append(
                {
                    "sheet_name": sheet_name,
                    "reason": f"形式を判別できませんでした: {failed_reason}",
                }
            )

    def _is_survey_definition_format(self, df: pd.DataFrame) -> bool:
        """
        調査定義書形式かどうかを判定する

        Args:
            df: データフレーム

        Returns:
            調査定義書形式の場合はTrue、そうでない場合はFalse
        """
        # A列の最初の10行を確認
        first_col_values = df.iloc[:10, 0].dropna().astype(str).tolist()

        # 調査定義書形式の特徴的なキーワード
        keywords = self.config["format_detection"]["survey_definition_keywords"]

        # いずれかのキーワードが含まれているかチェック
        for value in first_col_values:
            for keyword in keywords:
                if keyword in value:
                    return True

        # 【】で囲まれた見出しが複数あるかチェック
        bracket_headers = 0
        for value in first_col_values:
            if value.startswith("【") and value.endswith("】"):
                bracket_headers += 1

        if bracket_headers >= 2:
            return True

        return False

    def _is_insured_ledger_format(self, df: pd.DataFrame) -> bool:
        """
        被保険者台帳形式かどうかを判定する

        Args:
            df: データフレーム

        Returns:
            被保険者台帳形式の場合はTrue、そうでない場合はFalse
        """
        # 最初の数行を確認
        for i in range(min(10, len(df))):
            row = df.iloc[i].astype(str)

            # 設定ファイルから判定キーワードを取得
            headers = self.config["format_detection"]["insured_ledger_headers"]

            # ヘッダーキーワードの出現回数をカウント
            header_count = sum(1 for header in headers if header in row.values)

            # 一定数以上のヘッダーキーワードが含まれていれば被保険者台帳形式と判定
            if header_count >= 2:
                return True

        return False

    def _is_custom_format(self, df: pd.DataFrame) -> bool:
        """
        カスタム形式かどうかを判定する

        Args:
            df: データフレーム

        Returns:
            カスタム形式の場合はTrue、そうでない場合はFalse
        """
        # ここに新しい形式の判定ロジックを追加
        # 例：特定のパターンや構造を持つシートの判定

        # 現時点では未実装のため常にFalseを返す
        return False

    def _analyze_sheet_format(self, df: pd.DataFrame) -> str:
        """
        シート形式を分析し、判別できない理由を返す

        Args:
            df: データフレーム

        Returns:
            判別できない理由の説明
        """
        reasons = []

        # データが少なすぎる場合
        if len(df) < 5:
            reasons.append("データ行が少なすぎます")

        # 空のシートの場合
        if df.isna().all().all():
            reasons.append("シートが空です")

        # ヘッダー行が見つからない場合
        header_found = False
        for i in range(min(10, len(df))):
            row = df.iloc[i].astype(str)
            if "#" in row.values or "項目名" in row.values:
                header_found = True
                break

        if not header_found:
            reasons.append("ヘッダー行が見つかりません")

        # 理由がない場合のデフォルトメッセージ
        if not reasons:
            reasons.append("既知の形式パターンに一致しません")

        return "、".join(reasons)

    def _process_survey_definition_sheet(
        self, sheet_name: str, df: pd.DataFrame
    ) -> None:
        """
        調査定義書形式のシートを処理する

        Args:
            sheet_name: シート名
            df: データフレーム
        """
        # 調査定義書形式のデータを抽出
        data = {
            "sheet_type": "survey_definition",
            "metadata": {},
            "processed_at": pd.Timestamp.now().isoformat(),
        }

        # A列とB列のペアを抽出
        for i in range(len(df)):
            key = df.iloc[i, 0]
            if (
                pd.notna(key)
                and isinstance(key, str)
                and key.startswith("【")
                and key.endswith("】")
            ):
                # キーから【】を削除
                clean_key = key.replace("【", "").replace("】", "")

                # 値を取得（複数行にまたがる可能性があるため、次のキーまでの値を取得）
                values = []
                j = i
                while j < len(df) - 1:
                    j += 1
                    next_key = df.iloc[j, 0]
                    if (
                        pd.notna(next_key)
                        and isinstance(next_key, str)
                        and next_key.startswith("【")
                        and next_key.endswith("】")
                    ):
                        break

                    # B列だけでなく、C列以降も確認
                    row_values = []
                    for col in range(1, min(5, len(df.columns))):
                        value = df.iloc[j, col]
                        if pd.notna(value):
                            row_values.append(str(value))

                    if row_values:
                        values.append(" ".join(row_values))

                data["metadata"][clean_key] = values if values else None

        # 結果に追加
        self.result_data[sheet_name] = data

    def _process_insured_ledger_sheet(self, sheet_name: str, df: pd.DataFrame) -> None:
        """
        被保険者台帳形式のシートを処理する

        Args:
            sheet_name: シート名
            df: データフレーム
        """
        # テーブル名と物理名を取得
        table_name = None
        physical_name = None
        table_comment = None

        # テーブル名と物理名を探す
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            if pd.notna(row[0]) and row[0] == "テーブル名":
                if i + 1 < len(df) and pd.notna(df.iloc[i + 1, 0]):
                    table_name = df.iloc[i + 1, 0]
                if i + 1 < len(df) and pd.notna(df.iloc[i + 1, 2]):
                    physical_name = df.iloc[i + 1, 2]
                # テーブルコメントを取得（存在する場合）
                if (
                    i + 1 < len(df)
                    and len(df.columns) > 3
                    and pd.notna(df.iloc[i + 1, 3])
                ):
                    table_comment = df.iloc[i + 1, 3]
                break

        if not table_name:
            table_name = sheet_name

        if not physical_name:
            # テーブル一覧シートから物理名を探す
            try:
                table_list_df = pd.read_excel(
                    self.excel_file_path, sheet_name="テーブル一覧", header=None
                )
                for i in range(len(table_list_df)):
                    row = table_list_df.iloc[i]
                    if pd.notna(row[4]) and row[4] == table_name:
                        physical_name = row[4]
                        break
            except Exception as e:
                logger.warning(
                    f"テーブル一覧シートからの物理名取得に失敗しました: {str(e)}"
                )

            # 物理名が見つからない場合、テーブル名から生成
            if not physical_name:
                physical_name = self._generate_physical_name(table_name)

        # ヘッダー行のインデックスを特定
        header_row_idx = None
        for i in range(min(15, len(df))):
            row = df.iloc[i].astype(str)
            if (
                "#" in row.values
                and "項目名" in row.values
                and "物理項目名" in row.values
            ):
                header_row_idx = i
                break

        if header_row_idx is None:
            logger.warning(f"シート '{sheet_name}' のヘッダー行が見つかりませんでした")
            self.failed_sheets.append(
                {"sheet_name": sheet_name, "reason": "ヘッダー行が見つかりませんでした"}
            )
            return

        # ヘッダー行を取得
        headers = df.iloc[header_row_idx].tolist()

        # 必要なカラムのインデックスを取得
        col_indices = {
            "#": headers.index("#") if "#" in headers else None,
            "項目名": headers.index("項目名") if "項目名" in headers else None,
            "物理項目名": (
                headers.index("物理項目名") if "物理項目名" in headers else None
            ),
            "type": headers.index("type") if "type" in headers else None,
            "length": headers.index("length") if "length" in headers else None,
            "PK": headers.index("PK") if "PK" in headers else None,
            "備考": headers.index("備考") if "備考" in headers else None,
        }

        # データ行を処理
        columns = []
        for i in range(header_row_idx + 1, len(df)):
            row = df.iloc[i]

            # 空行をスキップ
            if all(pd.isna(row)):
                continue

            # 項目データを抽出
            item = {}
            for key, idx in col_indices.items():
                if idx is not None and idx < len(row):
                    value = row[idx]
                    if pd.notna(value):
                        item[key] = value

            # 必須項目がある場合のみ追加
            if "物理項目名" in item and pd.notna(item.get("物理項目名")):
                # データ型の正規化
                if "type" in item:
                    item["type"] = self._normalize_data_type(item["type"])

                columns.append(item)

        # 結果に追加
        self.result_data[sheet_name] = {
            "sheet_type": "insured_ledger",
            "table_name": table_name,
            "physical_name": physical_name,
            "comment": table_comment,
            "columns": columns,
            "processed_at": pd.Timestamp.now().isoformat(),
        }

    def _process_custom_format_sheet(self, sheet_name: str, df: pd.DataFrame) -> None:
        """
        カスタム形式のシートを処理する

        Args:
            sheet_name: シート名
            df: データフレーム
        """
        # カスタム形式の処理ロジックを実装
        # 現時点では未実装のため、警告を出力
        logger.warning(f"シート '{sheet_name}' のカスタム形式処理は未実装です")
        self.failed_sheets.append(
            {"sheet_name": sheet_name, "reason": "カスタム形式処理は未実装です"}
        )

    def _generate_physical_name(self, table_name: str) -> str:
        """
        テーブル名から物理名を生成する

        Args:
            table_name: テーブル名

        Returns:
            生成された物理名
        """
        # スペースや特殊文字を削除
        physical_name = "".join(c for c in table_name if c.isalnum() or c == " ")

        # スペースをアンダースコアに置換
        physical_name = physical_name.replace(" ", "_")

        # 小文字に変換
        physical_name = physical_name.lower()

        return physical_name

    def _normalize_data_type(self, data_type: Any) -> str:
        """
        データ型を正規化する

        Args:
            data_type: 元のデータ型

        Returns:
            正規化されたデータ型
        """
        if pd.isna(data_type):
            return "unknown"

        data_type_str = str(data_type).lower()

        # 設定ファイルからデータ型マッピングを取得
        type_mapping = self.config["data_types"]["mapping"]

        # マッピングを適用
        for key, normalized_type in type_mapping.items():
            if key in data_type_str:
                return normalized_type

        # マッピングにない場合はそのまま返す

    def _save_to_json(self) -> None:
        """
        結果をJSONファイルに保存する
        """
        # 失敗したシートの情報も含める
        result = {
            "tables": self.result_data,
            "failed_sheets": self.failed_sheets,
            "skipped_sheets": self.skipped_sheets,
            "summary": {
                "total_sheets": len(self.result_data)
                + len(self.failed_sheets)
                + len(self.skipped_sheets),
                "processed_sheets": self.processed_sheets,
                "failed_sheets": len(self.failed_sheets),
                "skipped_sheets": len(self.skipped_sheets),
                "processed_at": pd.Timestamp.now().isoformat(),
            },
        }

        # JSONファイルに保存
        with open(self.output_json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logger.info(f"結果を {self.output_json_path} に保存しました")

    def _print_summary(self) -> None:
        """
        処理結果のサマリーを出力する
        """
        total_sheets = (
            len(self.result_data) + len(self.failed_sheets) + len(self.skipped_sheets)
        )
        success_sheets = len(self.result_data)
        failed_sheets = len(self.failed_sheets)
        skipped_sheets = len(self.skipped_sheets)

        logger.info("=" * 50)
        logger.info(f"処理完了: 合計 {total_sheets} シート")
        logger.info(f"成功: {success_sheets} シート")
        logger.info(f"失敗: {failed_sheets} シート")
        logger.info(f"スキップ: {skipped_sheets} シート")

        if failed_sheets > 0:
            logger.info("失敗したシート:")
            for sheet in self.failed_sheets:
                logger.info(f"  - {sheet['sheet_name']}: {sheet['reason']}")

        if skipped_sheets > 0:
            logger.info("スキップしたシート:")
            for sheet in self.skipped_sheets:
                logger.info(f"  - {sheet}")

        logger.info("=" * 50)


def main():
    # 設定ファイルを読み込む
    config = get_config()

    # 入力ファイルと出力ファイルのパス
    excel_file_path = config["input_paths"]["excel_file"]
    output_json_path = config["output_paths"]["database_schema"]

    # 変換処理を実行
    converter = ExcelToJsonConverter(excel_file_path, output_json_path)
    converter.process()


if __name__ == "__main__":
    main()
