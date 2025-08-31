# D2 Process01 - Data Integration Pipeline

## 概要
このプロジェクトは、データベースからデータを抽出し、CSVファイルとしてGCSに保存し、BigQueryに取り込むためのデータ統合パイプラインです。

## 主な機能
- データベースからのデータ抽出（増分・フルロード対応）
- CSVファイルの生成とGCSへのアップロード
- BigQueryへのデータ取り込み
- Systemdate範囲によるデータ取得制御

## Systemdate設定について

### 概要
GCSに配置したコンフィグファイル（`config_d2.json`）で、Systemdateの開始・終了の値を指定し、データの取得範囲を制御できます。

### 設定方法

#### GCS設定ファイル（config_d2.json）
```json
{
  "SYSTEMDATE": {
    "enabled": true,
    "start_date": "relative",
    "end_date": "relative",
    "relative_unit": "days",
    "start_offset": -7,
    "end_offset": -1
  }
}
```

### 設定オプション

- `enabled`: Systemdate設定の有効/無効を制御
- `start_date`: 開始日時（"YYYY-MM-DD HH:MM:SS"形式）
- `end_date`: 終了日時（"YYYY-MM-DD HH:MM:SS"形式）

### 使用例

#### 例1: 特定の期間のデータを取得
```json
{
  "SYSTEMDATE": {
    "enabled": true,
    "start_date": "2024-01-01 00:00:00",
    "end_date": "2024-01-31 23:59:59"
  }
}
```

#### 例2: 開始日時のみ指定（終了日時は現在時刻）
```json
{
  "SYSTEMDATE": {
    "enabled": true,
    "start_date": "2024-01-01 00:00:00"
  }
}
```

#### 例3: 終了日時のみ指定（開始日時はlast_run_time）
```json
{
  "SYSTEMDATE": {
    "enabled": true,
    "end_date": "2024-12-31 23:59:59"
  }
}
```

### 動作仕様
1. 増分クエリ（`is_incremental: true`）の場合のみ適用
2. `:last_run_time`パラメータがある場合は、開始日時で置換
3. WHERE句がない場合は、`WHERE SystemDate BETWEEN '開始日時' AND '終了日時'`を追加
4. WHERE句がある場合は、`AND SystemDate BETWEEN '開始日時' AND '終了日時'`を追加
5. 開始日時が未設定の場合は`last_run_time`を使用
6. 終了日時が未設定の場合は現在時刻を使用

## インストールとセットアップ

### 必要な依存関係
```bash
pip install -r requirements.txt
```

### 環境設定
1. `env.yaml`ファイルで環境変数を設定
2. `config/config.yaml`で基本設定を確認
3. GCSに`config_d2.json`を配置

## 使用方法

### 基本的な実行
```python
from workflows.workflow import Workflow
from managers.config_manager import ConfigManager

# 設定を読み込み
config_manager = ConfigManager()
app_config = config_manager.load_typed_config()

# ワークフローを実行
workflow = Workflow(app_config)
workflow.run()
```

### フルロード実行
```python
# フルロードで実行
workflow.run(force_full_load=True)
```

### カスタムルックバック時間
```python
# 過去30分のデータを取得
workflow.run(lookback_minutes=30)
```

## 設定ファイルの構造

### ローカル設定（config.yaml）
- プロジェクト設定
- データベース接続情報
- BigQuery設定
- GCS設定
- Systemdate設定

### GCS設定（config_d2.json）
- クエリ設定
- バッチサイズ設定
- テーブルフィルタリング設定
- Systemdateオーバーライド設定

## トラブルシューティング

### よくある問題
1. **データベース接続エラー**: 接続情報とファイアウォール設定を確認
2. **GCS権限エラー**: サービスアカウントの権限を確認
3. **BigQuery権限エラー**: データセットへの書き込み権限を確認

### ログの確認
- Cloud Loggingでログレベルを設定
- 環境変数`LOG_LEVEL=DEBUG`で詳細ログを有効化

## 開発者向け情報

### アーキテクチャ
- `managers/`: 設定・データベース・GCS管理
- `processors/`: クエリ・CSV・データ処理
- `workflows/`: ワークフロー実行
- `core/`: データモデル
- `Exceptions/`: カスタム例外クラス

### テスト
```bash
# 単体テストの実行
python -m pytest tests/

# 統合テストの実行
python -m pytest tests/integration/
```

## ライセンス
このプロジェクトは社内利用のためのものです。

## プロジェクトステータス
開発中 - 新機能の追加と改善を継続中
