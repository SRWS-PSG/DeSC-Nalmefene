#!/usr/bin/env Rscript
#' DeSC-Nalmefene Table 1 生成
#'
#' @description
#' ベースライン特性の比較表（Table 1）を生成する
#'
#' @author Devin
#' @date 2023-05-20

# ライブラリの読み込み
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(gtsummary)  # 表の生成
  library(flextable)  # 表の整形
  library(officer)    # Word出力
  library(epiR)       # 統計解析
  library(mice)       # 多重代入
})

# 設定
data_dir <- "data/processed"
output_dir <- "outputs/tables"

# ディレクトリ作成
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

#' ベースラインデータの読み込み
#'
#' @param data_dir データディレクトリ
#'
#' @return ベースラインデータフレーム
load_baseline_data <- function(data_dir) {
  cat("ベースラインデータを読み込んでいます\n")
  
  # ファイルパス
  baseline_file <- file.path(data_dir, "baseline.rds")
  
  # ファイルの存在確認
  if (!file.exists(baseline_file)) {
    stop("ベースラインデータファイルが見つかりません: ", baseline_file)
  }
  
  # データの読み込み
  baseline <- readRDS(baseline_file)
  
  # 治療群のラベル付け（既に付いていない場合）
  if (!"treatment_label" %in% names(baseline)) {
    baseline <- baseline %>%
      mutate(
        treatment_label = case_when(
          treatment_group == 1 ~ "飲酒量低減治療",
          treatment_group == 2 ~ "断酒治療",
          treatment_group == 3 ~ "薬物療法なし",
          TRUE ~ NA_character_
        )
      )
  }
  
  return(baseline)
}

#' 多重代入による欠測値の補完
#'
#' @param data 欠測値を含むデータフレーム
#' @param m 代入回数
#' @param seed 乱数シード
#'
#' @return 多重代入されたデータフレーム
impute_missing_values <- function(data, m = 5, seed = 123) {
  cat("多重代入による欠測値の補完を行っています\n")
  
  # 乱数シードの設定
  set.seed(seed)
  
  # 多重代入の実行
  imp <- mice(data, m = m, printFlag = FALSE)
  
  # 代入データの結合
  completed_data <- complete(imp, "long")
  
  return(list(
    imputed_data = completed_data,
    imp_object = imp
  ))
}

#' Table 1の生成
#'
#' @param data ベースラインデータ
#' @param output_dir 出力ディレクトリ
#'
#' @return Table 1オブジェクト
generate_table1 <- function(data, output_dir) {
  cat("Table 1を生成しています\n")
  
  # 変数のラベル付け
  var_labels <- list(
    age = "年齢（平均 ± SD）",
    sex = "性別",
    insurance_type = "保険者種別",
    height = "身長（cm）",
    weight = "体重（kg）",
    bmi = "BMI（kg/m²）",
    waist = "腹囲（cm）",
    sbp = "収縮期血圧（mmHg）",
    dbp = "拡張期血圧（mmHg）",
    hdl = "HDLコレステロール（mg/dl）",
    ldl = "LDLコレステロール（mg/dl）",
    tg = "中性脂肪（mg/dl）",
    t_bil = "総ビリルビン（mg/dl）",
    ast = "GOT(AST)（U/l）",
    alt = "GPT(ALT)（U/l）",
    ggt = "γ-GT（U/l）",
    alb = "アルブミン（g/dl）",
    plt = "血小板数（×10⁴/μl）",
    fbg = "空腹時血糖（mg/dl）",
    hba1c = "HbA1c（%）",
    urine_glucose = "尿糖（%, \\+）",
    urine_protein = "尿蛋白（%, \\+）",
    drinking_freq = "飲酒頻度",
    drinking_amount = "飲酒量",
    smoking = "喫煙有無",
    sleep = "睡眠",
    diet = "食習慣",
    exercise = "運動習慣",
    mental_qol = "メンタルQOL項目"
  )
  
  # 変数の値ラベル付け
  value_labels <- list(
    sex = c("男性" = 1, "女性" = 2),
    insurance_type = c("健保" = 1, "国保" = 2, "後期高齢" = 3),
    urine_glucose = c("陰性" = 0, "陽性" = 1),
    urine_protein = c("陰性" = 0, "陽性" = 1),
    drinking_freq = c("毎日" = 1, "時々" = 2, "ほとんど飲まない" = 3),
    drinking_amount = c("1合未満" = 1, "1-2合" = 2, "2-3合" = 3, "3合以上" = 4),
    smoking = c("なし" = 0, "あり" = 1),
    sleep = c("十分" = 1, "不十分" = 2),
    diet = c("良好" = 1, "普通" = 2, "不良" = 3),
    exercise = c("なし" = 0, "あり" = 1),
    mental_qol = c("高" = 1, "低" = 2)
  )
  
  # 変数のグループ化
  var_groups <- list(
    "人口統計" = c("age", "sex", "insurance_type"),
    "身体計測" = c("height", "weight", "bmi", "waist"),
    "血圧" = c("sbp", "dbp"),
    "脂質代謝" = c("hdl", "ldl", "tg"),
    "肝機能" = c("t_bil", "ast", "alt", "ggt", "alb", "plt"),
    "血糖代謝" = c("fbg", "hba1c"),
    "尿検査" = c("urine_glucose", "urine_protein"),
    "生活習慣・問診" = c("drinking_freq", "drinking_amount", "smoking", "sleep", "diet", "exercise", "mental_qol")
  )
  
  # 変数リストの作成
  all_vars <- unlist(var_groups)
  
  # データの前処理
  data_for_table <- data %>%
    # 因子変数の設定
    mutate(across(c(sex, insurance_type, drinking_freq, drinking_amount, 
                   smoking, sleep, diet, exercise, mental_qol,
                   urine_glucose, urine_protein), as.factor)) %>%
    # 治療群を因子に変換
    mutate(treatment_label = factor(treatment_label, 
                                   levels = c("飲酒量低減治療", "断酒治療", "薬物療法なし")))
  
  # 変数ラベルの適用
  for (var in names(var_labels)) {
    if (var %in% names(data_for_table)) {
      attr(data_for_table[[var]], "label") <- var_labels[[var]]
    }
  }
  
  # 値ラベルの適用
  for (var in names(value_labels)) {
    if (var %in% names(data_for_table)) {
      data_for_table[[var]] <- factor(data_for_table[[var]], 
                                     levels = as.numeric(value_labels[[var]]),
                                     labels = names(value_labels[[var]]))
    }
  }
  
  # Table 1の生成
  table1 <- data_for_table %>%
    select(treatment_label, all_of(all_vars)) %>%
    tbl_summary(
      by = treatment_label,
      missing = "no",  # 欠測値を表示しない
      statistic = list(
        all_continuous() ~ "{mean} ± {sd}",
        all_categorical() ~ "{n} ({p}%)"
      ),
      digits = list(
        all_continuous() ~ 1,
        all_categorical() ~ c(0, 1)
      ),
      label = var_labels
    ) %>%
    add_p() %>%  # p値の追加
    add_overall() %>%  # 全体列の追加
    modify_header(label = "**特性**") %>%  # ヘッダーの変更
    modify_spanning_header(c("stat_1", "stat_2", "stat_3") ~ "**治療群**") %>%
    as_flex_table()
  
  # 出力ファイルパス
  html_file <- file.path(output_dir, "table1.html")
  docx_file <- file.path(output_dir, "table1.docx")
  
  # HTML形式で保存
  save_as_html(table1, path = html_file)
  cat("Table 1をHTML形式で保存しました:", html_file, "\n")
  
  # Word形式で保存
  save_as_docx(table1, path = docx_file)
  cat("Table 1をWord形式で保存しました:", docx_file, "\n")
  
  return(table1)
}

#' メイン処理
main <- function() {
  cat("DeSC-Nalmefene Table 1生成を開始します\n")
  
  # ベースラインデータの読み込み
  baseline <- load_baseline_data(data_dir)
  
  # 多重代入による欠測値の補完
  imputed <- impute_missing_values(baseline)
  
  # Table 1の生成
  table1 <- generate_table1(imputed$imputed_data, output_dir)
  
  cat("Table 1生成が完了しました\n")
}

# スクリプト実行
main()
