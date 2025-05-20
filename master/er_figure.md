```mermaid
classDiagram
    %% Core Entity
    class 被保険者台帳 {
        PK 仮個人ID
    }
    
    %% Core Related Tables
    class 組織 {
        PK 仮個人ID
        PK 年度
    }
    
    class 所属期間 {
        PK 仮個人ID
        PK 連番号
        +Note: 基本的に適用:所属期間=1:1だが、稀に複数期間在籍する仮個人IDが存在する
    }
    
    class 健診_問診 {
        PK 仮個人ID
        PK 健診実施年月日
    }
    
    %% System Groups
    class kencomテーブル群 {
        <<Package>>
    }
    
    class kencom登録 {
        PK 仮個人ID
    }
    
    class kencom利用機能 {
        PK 仮個人ID
        PK 年月
        PK 計測対象種別コード
    }
    
    class kencom歩活参加 {
        PK 仮個人ID
        PK キャンペーンID
    }
    
    class kencom記事閲覧 {
        PK 仮個人ID
        PK 閲覧日
        PK 記事ID
    }
    
    class ライフログテーブル群 {
        <<Package>>
    }
    
    class ライフログ_歩数 {
        PK 仮個人ID
        PK 年月
        歩数データソース
    }
    
    class ライフログ_血圧 {
        PK 仮個人ID
        PK 年月
        PK 計測時間
        血圧データソース
    }
    
    class ライフログ_体重 {
        PK 仮個人ID
        PK 年月
        体重データソース
    }
    
    class ライフログ_血糖 {
        PK 仮個人ID
        PK 年月
        PK 計測時間
        血糖データソース
    }
    
    class アンケートテーブル群 {
        <<Package>>
    }
    
    class アンケート質問 {
        PK 調査ID
        PK ラベルID
    }
    
    class アンケート選択肢 {
        PK 調査ID
        PK ラベルID
        PK 回答選択肢ID
    }
    
    class アンケート回答 {
        PK 仮個人
        PK 調査ID
        PK ラベルID
    }
    
    class レセプトテーブル群 {
        <<Package>>
    }
    
    class レセプト基本情報 {
        PK 仮レセプトID
        仮個人ID
    }
    
    class レセプト_医療機関 {
        PK 仮レセプトID
        仮医療機関番号
    }
    
    class レセプト_傷病 {
        PK 仮レセプトID
        PK 行番号
        レセプト電算処理システム傷病コード
    }
    
    class レセプト_医薬品 {
        PK 仮レセプトID
        PK 行番号
        レセプト電算処理システム医薬品コード
    }
    
    class レセプト_診療行為 {
        PK 仮レセプトID
        PK 行番号
        レセプト電算処理システム診療行為コード
    }
    
    %% Relationships
    被保険者台帳 --> 組織
    被保険者台帳 --> 所属期間
    被保険者台帳 --> 健診_問診
    
    被保険者台帳 --> kencomテーブル群
    kencomテーブル群 *-- kencom登録
    kencomテーブル群 *-- kencom利用機能
    kencomテーブル群 *-- kencom歩活参加
    kencomテーブル群 *-- kencom記事閲覧
    
    被保険者台帳 --> ライフログテーブル群
    ライフログテーブル群 *-- ライフログ_歩数
    ライフログテーブル群 *-- ライフログ_血圧
    ライフログテーブル群 *-- ライフログ_体重
    ライフログテーブル群 *-- ライフログ_血糖
    
    被保険者台帳 --> アンケートテーブル群
    アンケートテーブル群 *-- アンケート質問
    アンケートテーブル群 *-- アンケート選択肢
    アンケートテーブル群 *-- アンケート回答
    
    被保険者台帳 --> レセプトテーブル群
    レセプトテーブル群 *-- レセプト基本情報
    レセプトテーブル群 *-- レセプト_医療機関
    レセプトテーブル群 *-- レセプト_傷病
    レセプトテーブル群 *-- レセプト_医薬品
    レセプトテーブル群 *-- レセプト_診療行為
    
    レセプト基本情報 --> レセプト_医療機関
    レセプト基本情報 --> レセプト_傷病
    レセプト基本情報 --> レセプト_医薬品
    レセプト基本情報 --> レセプト_診療行為
    ```