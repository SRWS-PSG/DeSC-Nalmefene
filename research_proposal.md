# 研究計画書（ドラフト）

## 1\. タイトルと概要

**研究タイトル（案）：**  
「DeSCデータベースを用いたアルコール依存症患者に対する飲酒量低減治療の現況・早期介入としての意義・転帰に関する観察研究」

**研究期間・場所：**

- データ解析実施拠点：岡山県精神科医療センター  
- データベース期間：2014年04月〜2023年09月  
- 観察期間：20XX年XX月〜20XX年XX月

**概要（抄録）：**  
アルコール依存症に対する飲酒量低減治療（ナルメフェン）は2019年3月に国内保険適用され、断酒以外の治療戦略が広がりつつある。しかし、どのような患者がこの治療を受け、どの医療機関で提供され、早期介入や転帰改善に寄与し得るかは不明である。本研究はDeSCデータベースを用いて、(1)飲酒量低減治療を受ける患者属性、(2)治療が行われる医療機関の特徴、(3)治療がアルコール依存症への早期介入となり得るかの年度別比較、(4)治療後の肝機能・生活習慣等の転帰評価を行う。探索的な記述的研究として、RECORD/RECORD-PE声明に準拠した報告を行う。

## 2\. はじめに（背景と目的）

**背景：**  
アルコール依存症は重大な健康・社会的問題を引き起こす[(World Health Organization 2019\)](https://paperpile.com/c/SAa3wv/G5w5)。近年、日本でナルメフェンが保険適用され[(Miyata et al. 2019\)](https://paperpile.com/c/SAa3wv/gMxx)、飲酒量低減治療マニュアルが関連諸学会から発出されるなど[(日本アルコール・アディクション医学会 et al. 2019\)](https://paperpile.com/c/SAa3wv/xbLj)、断酒以外のアプローチも推奨されるようになった[(Higuchi et al. 2014\)](https://paperpile.com/c/SAa3wv/V0Yu)。しかし、ナルメフェンの保険適用によって、これまでにアルコール依存症治療にアクセスしていなかった患者層に治療が届けられるようになっているのか、アルコール依存症治療を提供する医療機関は増えているのか、より早期の介入につながっているか、飲酒量低減に寄与しているかは明らかではない。

**目的：**

1. 飲酒量低減治療を受けている患者のベースライン特性を明らかにする。  
2. 飲酒量低減治療が行われる医療機関の特徴を把握する。  
3. 飲酒量低減治療がアルコール依存症への早期介入に寄与しているかを評価する。  
4. 治療開始前後の肝機能・生活習慣・飲酒行動等の転帰を3群（飲酒量低減目標治療群、断酒目標治療群、治療目標不明群）で比較する。

## 3\. 方法

### 3.1 研究デザイン

- デザイン：後ろ向き観察的コホート研究  
- 対象：DeSCデータベース収載者のうち、2014年4月〜2023年9月の間にICD-10：**F10.2**（アルコール関連障害）が初めて付与された日をインデックス日とする。  
- 観察期間：2014年4月〜2023年9月。  
- 対象者選定過程はフローチャートで示す。

### 3.2 セッティング

- データソース：DeSCデータベース（約440万人分の匿名加工医療・健診・レセプトデータ）

### 3.3 参加者

- 包含基準：

  観察対象期間中に初めてF10.2が付与された者

- 除外基準：なし

### 3.4 変数

- index date: 観察対象期間中に初めてF10.2が付与された日とする。  
- 治療パターン：F10.2コードが付与されてから12週以内に処方された最初のアルコール依存症治療薬（ナルメフェン\[ATCコード N07BB05\]、アカンプロサート\[ATCコード N07BB03\]、ジスルフィラム\[ATCコード N07BB01\]、シアナミド\[YJコード 3932001S1041 (ATCコードなし)\]）によって分類する。  
  - ナルメフェン：飲酒量低減目標治療群  
  - アカンプロサート、ジスルフィラム、シアナミド：断酒目標治療群  
  - 処方なし：治療目標不明群（非薬物療法群）  
- 評価項目（Table 1参照）：  
  - 人口統計、家族情報、身体計測、血圧、脂質・肝機能・血糖代謝指標、尿検査、生活習慣・問診（飲酒頻度・量、喫煙、睡眠、食習慣、運動習慣、メンタルQOL）  
- 医療機関特性（Table 2）：  
  - 医療機関種別、病床規模、ニコチン依存症管理料算定有無  
  - 精神科単科病院、精神科含む総合病院、大学病院、精神科主体診療所、精神科なし診療所で分類  
  - 専門医療機関フラグ：年間X人以上のアルコール依存症患者診療施設を「専門医療機関」と定義（Xは分布を見て決定する）

## 4\. 統計解析方法

- 解析対象集団  
  - primary cohort: データベース期間の起点からindex dataまでの期間 (washout期間)が52週以上である者で構成されるコホート（本washout期間はopioid use disorderと初めて診断された者を対象としたnew user desingの先行データベース研究をもとに設定した[(Paljarvi et al. 2021\)](https://paperpile.com/c/SAa3wv/KNj9)。  
  - sensitivity cohort1:washout期間が26週以上である者で構成されるコホート  
  - sensitivity cohort2: washout期間が156週以上である者で構成されるコホート  
- 欠損値処理：多重代入（miceパッケージなど）  
- 記述統計：  
  - Table 1〜3で3群比較  
  - 年度別比較（Table 5,6）：年度ごと、ナルメフェン初回処方群vsその他  
  - 転帰評価（Table 7）：初めてF10.2が付与された受診前直近・後直近・翌年・翌々年で3群比較  
- 図示：  
  - 地域別分布ヒートマップ（Figure 2）：人口あたりのナルメフェン処方数ヒートマップ（都道府県別）  
  - swim-lane plot（Figure 3）：個人ごとの時系列治療パターン記述  
  - sankey diagram（Figure 4）：集団レベルでの時系列治療パターン要約  
  - Kaplan-Meier曲線（Figure 5）：治療継続率比較（転院後もF10.2算定続く場合継続とみなす）  
- 感度分析：  
  - primary cohortの欠損補完前のデータセットを用いた分析  
  - sensitivity cohortsの欠損補完前後のデータセットを用いた分析

## 5\. データクリーニング

- データクリーニング：年齢・性別整合性チェック、F10.2初回日確認、異常値対応  
- データシェアリングなし

## 6\. 結果報告計画

Tables

1. **Table 1\.** 対象者のベースライン特性  
2. **Table 2\.** 飲酒量低減治療を提供する医療機関の特徴  
3. **Table 3\.** 併存疾患・医療利用度  
4. **Table 4\.** 初診時併算定された診療報酬コードの分布（頻度の多い10項目を含む）  
5. **Table 5\.** 年度ごとの患者背景比較（ICD10でF10.x初発患者）  
6. **Table 6\.** 年度ごとの患者背景比較（飲酒量低減治療目標群 vs その他）  
7. **Table 7\.** 時系列データでの3群比較（人口統計項目除外）  
   * Table 7-Overall（全体）  
   * Table 7-Male（男性のみ）  
   * Table 7-Female（女性のみ）

Figures

1. **Figure 1\.** 研究対象者選択フローチャート  
2. **Figure 2\.** 地域別分布ヒートマップ  
3. **Figure 3\.** 個人ごとの時系列治療パターン記述 swim-lane plot  
4. **Figure 4\.** 集団レベルでの時系列治療パターン要約 sankey diagram  
5. **Figure 5\.** kaplan-meier曲線（治療継続率）  
   

## 7\. その他情報

- 資金提供者：なし  
- データ提供元の関与：なし  
- 利益相反（COI）：  
  - RS reports personal fees from CureApp, Inc., during the conduct of the study; grants from Osake-no-Kagaku Foundation, The Mental Health Okamoto Memorial Foundation, and Kobayashi Magobe Memorial Medical Foundation; personal fees from Otsuka Pharmaceutical Co., Ltd., Nippon Shinyaku Co., Ltd., Sumitomo Pharma Co., Ltd., and Takeda Pharmaceutical Co., Ltd., outside the submitted work;  In addition, RS has a patent JP2022049590A, US20220084673A1, JP2022178215A, JP2022070086, and JP2023074128A pending.  
- データ・コード共有：行わない

## Supplementary

- Tables案  
- RECORD/RECORD-PE対応表

## Tables案

## Table 1\. 対象者のベースライン特性

| 特性 | 飲酒量低減治療群 (n=XXX) | 断酒治療群 (n=XXX) | 薬物療法なし群 (n=XXX) | p値 |
| :---- | :---- | :---- | :---- | :---- |
| **人口統計** |  |  |  |  |
| 年齢（平均 ± SD） |  |  |  |  |
| 性別（男性, %） |  |  |  |  |
| 保険者種別（%, 健保/国保/後期高齢） |  |  |  |  |
| **家族情報** |  |  |  |  |
| 本人/扶養家族（%, 本人/扶養家族） |  |  |  |  |
| 家族ID利用可（%, 利用可/利用不可） |  |  |  |  |
| 親子ID利用可（%, 利用可/利用不可） |  |  |  |  |
| **身体計測** |  |  |  |  |
| 身長（cm） |  |  |  |  |
| 体重（kg） |  |  |  |  |
| BMI（kg/m²） |  |  |  |  |
| 腹囲（cm） |  |  |  |  |
| **血圧** |  |  |  |  |
| 収縮期血圧（mmHg） |  |  |  |  |
| 拡張期血圧（mmHg） |  |  |  |  |
| **脂質代謝** |  |  |  |  |
| HDLコレステロール（mg/dl） |  |  |  |  |
| LDLコレステロール（mg/dl） |  |  |  |  |
| 中性脂肪（mg/dl） |  |  |  |  |
| **肝機能** |  |  |  |  |
| 総ビリルビン（mg/dl） |  |  |  |  |
| GOT(AST)（U/l） |  |  |  |  |
| GPT(ALT)（U/l） |  |  |  |  |
| γ-GT（U/l） |  |  |  |  |
| アルブミン（g/dl） |  |  |  |  |
| 血小板数（×10⁴/μl） |  |  |  |  |
| **血糖代謝** |  |  |  |  |
| 空腹時血糖（mg/dl） |  |  |  |  |
| HbA1c（%） |  |  |  |  |
| **尿検査** |  |  |  |  |
| 尿糖（%, \+） |  |  |  |  |
| 尿蛋白（%, \+） |  |  |  |  |
| **生活習慣・問診** |  |  |  |  |
| 飲酒頻度（%, 毎日/時々/ほとんど飲まない） |  |  |  |  |
| 飲酒量（%, 1合未満/1-2合/2-3合/3合以上） |  |  |  |  |
| 喫煙有無（%, 有/無） |  |  |  |  |
| 睡眠（%, 十分/不十分） |  |  |  |  |
| 食習慣（%, 良好/普通/不良） |  |  |  |  |
| 運動習慣（%, あり/なし） |  |  |  |  |
| メンタルQOL項目（%, 高/低） |  |  |  |  |

## Table 2\. 飲酒量低減治療を提供する医療機関の特徴

| 医療機関特性 | 飲酒量低減治療あり (n=XXX) | 飲酒量低減治療なし (n=XXX) | 全体 (n=XXX) |
| :---- | :---- | :---- | :---- |
| **医療機関種別** |  |  |  |
| 精神科単科病院 |  |  |  |
| 精神科含む総合病院 |  |  |  |
| 大学病院 |  |  |  |
| 精神科主体診療所 |  |  |  |
| 精神科なし診療所 |  |  |  |
| **病床規模** |  |  |  |
| 20床未満 |  |  |  |
| 20-49床 |  |  |  |
| 50-99床 |  |  |  |
| 100床以上 |  |  |  |
| **ニコチン依存症管理料** |  |  |  |
| 算定実績あり |  |  |  |
| 算定実績なし |  |  |  |

## Table 3\. 併存疾患・医療利用度

| 特性 | 飲酒量低減治療群 (n=XXX) | 断酒治療群 (n=XXX) | 薬物療法なし群 (n=XXX) | p値 |
| :---- | :---- | :---- | :---- | :---- |
| **併存疾患** |  |  |  |  |
| 高血圧（%, 有） |  |  |  |  |
| 糖尿病（%, 有） |  |  |  |  |
| 脂質異常症（%, 有） |  |  |  |  |
| 精神疾患（%, 有） |  |  |  |  |
| その他（%, 有） |  |  |  |  |
| **医療利用度** |  |  |  |  |
| 過去6ヶ月の受診回数 |  |  |  |  |
| 合計点数（平均±SD） |  |  |  |  |

## Table 4\. 初診時併算定された診療報酬コードの分布

| 医療機関種別 | 治療目標 | 通院精神療法 (%) | 依存症集団療法 (%) | 精神科デイケア (%) | 禁煙外来 (%) | 頻度1 (%) | 頻度2 (%) | 頻度3 (%) | 頻度4 (%) | 頻度5 (%) | 頻度6 (%) | 頻度7 (%) | 頻度8 (%) | 頻度9 (%) | 頻度10 (%) | その他 (%) |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| 精神科単科病院 | 飲酒量低減治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 断酒治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 治療目標不明 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 精神科含む総合病院 | 飲酒量低減治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 断酒治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 治療目標不明 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 大学病院 | 飲酒量低減治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 断酒治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 治療目標不明 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 精神科主体診療所 | 飲酒量低減治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 断酒治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 治療目標不明 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 精神科なし診療所 | 飲酒量低減治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 断酒治療 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 治療目標不明 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

## Table 5\. 年度ごとの患者背景比較（ICD10でF10.x初発患者）

| 特性 | 2018年度 (n=XXX) | 2019年度 (n=XXX) | 2020年度 (n=XXX) | 2021年度 (n=XXX) | 2022年度 (n=XXX) | 2023年度 (n=XXX) | p値 |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| 年齢（平均 ± SD） |  |  |  |  |  |  |  |
| 性別（男性, %） |  |  |  |  |  |  |  |
| 保険者種別（%, 健保/国保/後期高齢） |  |  |  |  |  |  |  |
| **家族情報** |  |  |  |  |  |  |  |
| 本人/扶養家族（%, 本人/扶養家族） |  |  |  |  |  |  |  |
| 家族ID利用可（%, 利用可/利用不可） |  |  |  |  |  |  |  |
| 親子ID利用可（%, 利用可/利用不可） |  |  |  |  |  |  |  |
| **身体計測** |  |  |  |  |  |  |  |
| 身長（cm） |  |  |  |  |  |  |  |
| 体重（kg） |  |  |  |  |  |  |  |
| BMI（kg/m²） |  |  |  |  |  |  |  |
| 腹囲（cm） |  |  |  |  |  |  |  |
| \[以下、Table 1と同項目が続く\] |  |  |  |  |  |  |  |

## Table 6\. 年度×治療群比較（ナルメフェン初回 vs その他治療）

| 特性 | 2018年度 ナルメ (n=XXX) | 2018年度 その他 (n=XXX) | 2019年度 ナルメ (n=XXX) | 2019年度 その他 (n=XXX) | ... | p値 |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| 年齢（平均 ± SD） |  |  |  |  |  |  |
| 性別（男性, %） |  |  |  |  |  |  |
| 保険者種別（%, 健保/国保/後期高齢） |  |  |  |  |  |  |
| **家族情報** |  |  |  |  |  |  |
| \[以下、Table 1と同項目が続く\] |  |  |  |  |  |  |

## Table 7\. 時系列データでの3群比較（人口統計項目除外）

### Table 7-Overall（全体）

| 特性 | 初診前直近 飲酒量低減 | 初診前直近 断酒 | 初診前直近 目標不明 | 初診後直近 飲酒量低減 | 初診後直近 断酒 | 初診後直近 目標不明 | 初診後翌年 飲酒量低減 | 初診後翌年 断酒 | 初診後翌年 目標不明 | 初診後翌々年 飲酒量低減 | 初診後翌々年 断酒 | 初診後翌々年 目標不明 | p値 |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| 本人/扶養家族（%, 本人/扶養家族） |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 家族ID利用可（%, 利用可/利用不可） |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 親子ID利用可（%, 利用可/利用不可） |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 身長（cm） |  |  |  |  |  |  |  |  |  |  |  |  |  |
| \[以下、人口統計以外のTable 1全項目\] |  |  |  |  |  |  |  |  |  |  |  |  |  |

### Table 7-Male（男性のみ）

| 特性 | 初診前直近 飲酒量低減(男) | 初診前直近 断酒(男) | ... | p値 |
| :---- | :---- | :---- | :---- | :---- |
| 本人/扶養家族（%, 本人/扶養家族） |  |  |  |  |
| 家族ID利用可（%, 利用可/利用不可） |  |  |  |  |
| \[以下、同様に全項目を男性対象で時系列表示\] |  |  |  |  |

### Table 7-Female（女性のみ）

| 特性 | 初診前直近 飲酒量低減(女) | 初診前直近 断酒(女) | ... | p値 |
| :---- | :---- | :---- | :---- | :---- |
| 本人/扶養家族（%, 本人/扶養家族） |  |  |  |  |
| 家族ID利用可（%, 利用可/利用不可） |  |  |  |  |
| \[以下、同様に全項目を女性対象で時系列表示\] |  |  |  |  |

# 

## 

## 参考文献

[Higuchi, S. et al., 2014\. Acceptance of controlled drinking among treatment specialists of alcohol dependence in Japan. *Alcohol and alcoholism* , 49(4), pp.447–452.](http://paperpile.com/b/SAa3wv/V0Yu)

[Miyata, H. et al., 2019\. Nalmefene in alcohol-dependent patients with a high drinking risk: Randomized controlled trial. *Psychiatry and clinical neurosciences*, 73(11), pp.697–706.](http://paperpile.com/b/SAa3wv/gMxx)

[Paljarvi, T. et al., 2021\. Abuse‐deterrent extended‐release oxycodone and risk of opioid‐related harm. *Addiction*, 116(9), pp.2409–2415.](http://paperpile.com/b/SAa3wv/KNj9)

[World Health Organization, 2019\. *Global Status Report on Alcohol and Health 2018*, World Health Organization.](http://paperpile.com/b/SAa3wv/G5w5)

[日本アルコール・アディクション医学会 et al., 2019\. 飲酒量低減治療マニュアル ポケット版,](http://paperpile.com/b/SAa3wv/xbLj)

