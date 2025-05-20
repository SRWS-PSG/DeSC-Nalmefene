#!/usr/bin/env Rscript
#' DeSC-Nalmefene 解析用データセット作成
#'
#' @description
#' Python前処理で作成された中間データから解析用データセットを作成する
#' baseline.rds, followup_long.rds, continuation.rdsを生成する
#'
#' @author Devin
#' @date 2023-05-20

# ライブラリの読み込み
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)  # featherファイル読み込み用
  library(lubridate)
  library(mice)   # 多重代入用
})

# 設定
data_dir <- "data/interim"
output_dir <- "data/processed"

# ディレクトリ作成
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

#' 列名の正規化
#'
#' @param df データフレーム
#' @param column_map 列名のマッピング
#'
#' @return 列名を正規化したデータフレーム
normalize_names <- function(df, column_map) {
  # 列名のマッピングを適用
  current_names <- names(df)
  new_names <- current_names
  
  for (i in seq_along(current_names)) {
    if (current_names[i] %in% names(column_map)) {
      new_names[i] <- column_map[[current_names[i]]]
    }
  }
  
  names(df) <- new_names
  return(df)
}

#' WHO飲酒リスクレベルの計算
#'
#' @param tac 1日あたりの純アルコール摂取量(g)
#' @param sex 性別（1=男性、2=女性）
#'
#' @return WHO飲酒リスクレベル（0-3）
calc_who_risk <- function(tac, sex) {
  # 性別に基づいてリスクレベルを計算
  ifelse(is.na(tac) | is.na(sex), NA,
         ifelse(sex == 1, 
                # 男性の場合
                ifelse(tac < 40, 0,
                       ifelse(tac < 60, 1,
                              ifelse(tac < 100, 2, 3))),
                # 女性の場合
                ifelse(tac < 20, 0,
                       ifelse(tac < 40, 1,
                              ifelse(tac < 60, 2, 3)))))
}

#' アルコール依存症患者データの読み込みと前処理
#'
#' @param data_dir データディレクトリ
#'
#' @return 前処理済みのデータフレーム
load_and_preprocess_patient_data <- function(data_dir) {
  cat("患者データの読み込みを開始します\n")
  
  # 治療群データの読み込み
  treatment_file <- file.path(data_dir, "treatment_groups.feather")
  if (!file.exists(treatment_file)) {
    stop("治療群ファイルが見つかりません: ", treatment_file)
  }
  
  patients <- read_feather(treatment_file) %>%
    as.data.frame()
  
  # 列名のマッピング
  column_map <- list(
    "kojin_id" = "patient_id",
    "index_date" = "index_date",
    "treatment_group" = "treatment_group",
    "first_drug_date" = "first_drug_date"
  )
  
  # 列名の正規化
  patients <- normalize_names(patients, column_map)
  
  # 治療群のラベル付け
  patients <- patients %>%
    mutate(
      treatment_label = case_when(
        treatment_group == 1 ~ "飲酒量低減治療",
        treatment_group == 2 ~ "断酒治療",
        treatment_group == 3 ~ "薬物療法なし",
        TRUE ~ NA_character_
      )
    )
  
  return(patients)
}

#' 患者の健診・問診データの読み込みと前処理
#'
#' @param data_dir データディレクトリ
#' @param patients 患者データフレーム
#'
#' @return 健診・問診データを結合した患者データフレーム
load_and_merge_checkup_data <- function(data_dir, patients) {
  cat("健診・問診データの読み込みを開始します\n")
  
  # TODO: SSDデータの構造に合わせて健診・問診データの読み込み処理を実装
  # 現在は仮のデータを生成
  
  # 患者IDのリスト
  patient_ids <- unique(patients$patient_id)
  n_patients <- length(patient_ids)
  
  # 仮の健診データを生成
  set.seed(123)  # 再現性のために乱数シードを設定
  
  checkup_data <- data.frame(
    patient_id = patient_ids,
    # 人口統計
    age = round(rnorm(n_patients, 50, 10)),
    sex = sample(1:2, n_patients, replace = TRUE),
    insurance_type = sample(1:3, n_patients, replace = TRUE),
    
    # 身体計測
    height = rnorm(n_patients, 165, 10),
    weight = rnorm(n_patients, 65, 12),
    bmi = NA,  # 後で計算
    waist = rnorm(n_patients, 85, 10),
    
    # 血圧
    sbp = rnorm(n_patients, 130, 15),
    dbp = rnorm(n_patients, 80, 10),
    
    # 脂質代謝
    hdl = rnorm(n_patients, 55, 15),
    ldl = rnorm(n_patients, 120, 30),
    tg = rnorm(n_patients, 150, 80),
    
    # 肝機能
    t_bil = rnorm(n_patients, 0.8, 0.3),
    ast = rnorm(n_patients, 30, 15),
    alt = rnorm(n_patients, 25, 15),
    ggt = rnorm(n_patients, 50, 40),
    alb = rnorm(n_patients, 4.2, 0.4),
    plt = rnorm(n_patients, 22, 5),
    
    # 血糖代謝
    fbg = rnorm(n_patients, 100, 20),
    hba1c = rnorm(n_patients, 5.6, 0.7),
    
    # 尿検査
    urine_glucose = sample(0:1, n_patients, replace = TRUE),
    urine_protein = sample(0:1, n_patients, replace = TRUE),
    
    # 生活習慣・問診
    drinking_freq = sample(1:3, n_patients, replace = TRUE),
    drinking_amount = sample(1:4, n_patients, replace = TRUE),
    smoking = sample(0:1, n_patients, replace = TRUE),
    sleep = sample(1:2, n_patients, replace = TRUE),
    diet = sample(1:3, n_patients, replace = TRUE),
    exercise = sample(0:1, n_patients, replace = TRUE),
    mental_qol = sample(1:2, n_patients, replace = TRUE)
  )
  
  # BMIの計算
  checkup_data$bmi <- with(checkup_data, weight / ((height/100)^2))
  
  # 患者データと結合
  patients_with_checkup <- patients %>%
    left_join(checkup_data, by = "patient_id")
  
  return(patients_with_checkup)
}

#' ベースラインデータセットの作成
#'
#' @param patients_with_checkup 健診データ付き患者データフレーム
#' @param output_dir 出力ディレクトリ
#'
#' @return ベースラインデータセット
create_baseline_dataset <- function(patients_with_checkup, output_dir) {
  cat("ベースラインデータセットを作成しています\n")
  
  # WHO飲酒リスクレベルの追加
  baseline <- patients_with_checkup %>%
    mutate(
      who_risk_level = calc_who_risk(
        tac = ifelse(is.na(drinking_amount), NA,
                     # 仮の換算式：飲酒量カテゴリを純アルコール量に変換
                     case_when(
                       drinking_amount == 1 ~ 10,  # 1合未満
                       drinking_amount == 2 ~ 30,  # 1-2合
                       drinking_amount == 3 ~ 50,  # 2-3合
                       drinking_amount == 4 ~ 80,  # 3合以上
                       TRUE ~ NA_real_
                     )),
        sex = sex
      )
    )
  
  # 出力ファイルパス
  output_file <- file.path(output_dir, "baseline.rds")
  
  # 保存
  saveRDS(baseline, output_file)
  cat("ベースラインデータセットを保存しました:", output_file, "\n")
  
  return(baseline)
}

#' フォローアップデータセットの作成（長形式）
#'
#' @param baseline ベースラインデータセット
#' @param output_dir 出力ディレクトリ
#'
#' @return フォローアップデータセット（長形式）
create_followup_dataset <- function(baseline, output_dir) {
  cat("フォローアップデータセットを作成しています\n")
  
  # 患者IDのリスト
  patient_ids <- unique(baseline$patient_id)
  n_patients <- length(patient_ids)
  
  # 観察期間のポイント（週数）
  followup_weeks <- c(0, 4, 12, 24, 36, 48, 60)
  
  # フォローアップデータの作成（仮のデータ）
  followup_rows <- list()
  
  for (patient_id in patient_ids) {
    # ベースラインデータから患者情報を取得
    patient_baseline <- baseline %>%
      filter(patient_id == !!patient_id)
    
    # 各観察ポイントでのデータを生成
    for (week in followup_weeks) {
      # 欠測確率（時間が経つほど欠測が増える）
      missing_prob <- min(0.1 + week/100, 0.5)
      
      # 初期値をベースラインから取得
      initial_tac <- ifelse(is.na(patient_baseline$drinking_amount), NA,
                           case_when(
                             patient_baseline$drinking_amount == 1 ~ 10,
                             patient_baseline$drinking_amount == 2 ~ 30,
                             patient_baseline$drinking_amount == 3 ~ 50,
                             patient_baseline$drinking_amount == 4 ~ 80,
                             TRUE ~ NA_real_
                           ))
      
      # 治療群に応じた改善率
      improvement_rate <- case_when(
        patient_baseline$treatment_group == 1 ~ 0.7,  # 飲酒量低減治療
        patient_baseline$treatment_group == 2 ~ 0.9,  # 断酒治療
        patient_baseline$treatment_group == 3 ~ 0.3,  # 薬物療法なし
        TRUE ~ 0.0
      )
      
      # 時間経過による改善（単純な指数減衰モデル）
      if (!is.na(initial_tac)) {
        current_tac <- initial_tac * exp(-improvement_rate * week / 52)
      } else {
        current_tac <- NA
      }
      
      # 欠測の導入
      if (runif(1) < missing_prob) {
        current_tac <- NA
      }
      
      # 飲酒日数と大量飲酒日数の生成
      if (!is.na(current_tac)) {
        dd <- round(runif(1, max = 28))
        hdd <- round(runif(1, max = dd))
      } else {
        dd <- NA
        hdd <- NA
      }
      
      # WHO飲酒リスクレベルの計算
      who_rl <- calc_who_risk(current_tac, patient_baseline$sex)
      
      # 行の追加
      followup_rows[[length(followup_rows) + 1]] <- data.frame(
        patient_id = patient_id,
        treatment_group = patient_baseline$treatment_group,
        week = week,
        tac = current_tac,
        dd = dd,
        hdd = hdd,
        who_risk_level = who_rl,
        ast = rnorm(1, patient_baseline$ast, 5),
        alt = rnorm(1, patient_baseline$alt, 5),
        ggt = rnorm(1, patient_baseline$ggt, 10)
      )
    }
  }
  
  # データフレームに変換
  followup_long <- do.call(rbind, followup_rows)
  
  # 出力ファイルパス
  output_file <- file.path(output_dir, "followup_long.rds")
  
  # 保存
  saveRDS(followup_long, output_file)
  cat("フォローアップデータセットを保存しました:", output_file, "\n")
  
  return(followup_long)
}

#' 治療継続データセットの作成
#'
#' @param baseline ベースラインデータセット
#' @param output_dir 出力ディレクトリ
#'
#' @return 治療継続データセット
create_continuation_dataset <- function(baseline, output_dir) {
  cat("治療継続データセットを作成しています\n")
  
  # 患者IDのリスト
  patient_ids <- unique(baseline$patient_id)
  n_patients <- length(patient_ids)
  
  # 治療継続データの作成（仮のデータ）
  set.seed(456)  # 再現性のために乱数シードを設定
  
  # 治療群ごとの継続率パラメータ
  continuation_params <- list(
    "1" = list(shape = 1.2, scale = 300),  # 飲酒量低減治療
    "2" = list(shape = 1.5, scale = 350),  # 断酒治療
    "3" = list(shape = 0.8, scale = 200)   # 薬物療法なし
  )
  
  # 継続データの生成
  continuation_rows <- list()
  
  for (i in 1:n_patients) {
    patient_id <- patient_ids[i]
    
    # ベースラインデータから治療群を取得
    treatment_group <- baseline$treatment_group[baseline$patient_id == patient_id]
    
    # 治療群に応じたパラメータを取得
    params <- continuation_params[[as.character(treatment_group)]]
    
    # 最終受診日を生成（ワイブル分布）
    t_last_visit <- round(rweibull(1, shape = params$shape, scale = params$scale))
    
    # 打ち切りフラグ（観察期間終了時に治療継続中ならTRUE）
    max_followup <- 365  # 1年間の観察
    censored <- t_last_visit > max_followup
    
    # 打ち切りの場合、最終受診日を観察期間終了日に設定
    if (censored) {
      t_last_visit <- max_followup
    }
    
    # 脱落フラグ（打ち切りでなければ脱落）
    dropout <- !censored
    
    # 行の追加
    continuation_rows[[i]] <- data.frame(
      patient_id = patient_id,
      treatment_group = treatment_group,
      t_last_visit = t_last_visit,
      censored = censored,
      dropout = dropout
    )
  }
  
  # データフレームに変換
  continuation <- do.call(rbind, continuation_rows)
  
  # 出力ファイルパス
  output_file <- file.path(output_dir, "continuation.rds")
  
  # 保存
  saveRDS(continuation, output_file)
  cat("治療継続データセットを保存しました:", output_file, "\n")
  
  return(continuation)
}

#' メイン処理
main <- function() {
  cat("DeSC-Nalmefene 解析用データセット作成を開始します\n")
  
  # 患者データの読み込みと前処理
  patients <- load_and_preprocess_patient_data(data_dir)
  
  # 健診・問診データの読み込みと結合
  patients_with_checkup <- load_and_merge_checkup_data(data_dir, patients)
  
  # ベースラインデータセットの作成
  baseline <- create_baseline_dataset(patients_with_checkup, output_dir)
  
  # フォローアップデータセットの作成
  followup_long <- create_followup_dataset(baseline, output_dir)
  
  # 治療継続データセットの作成
  continuation <- create_continuation_dataset(baseline, output_dir)
  
  cat("解析用データセット作成が完了しました\n")
}

# スクリプト実行
main()
