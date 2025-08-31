import gc
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple, TYPE_CHECKING

from core.models import AppConfig, QueryConfig, ProcessingResult

# 実際のインポート（実行時に必要）
from managers.config_manager import ConfigManager
from managers.database_manager import DatabaseManager
from managers.gcs_manager import GCSManager
from processors.query_processor import QueryProcessor
from processors.csv_processor import CSVProcessor

# 例外クラス
from Exceptions.CsvGenerationException import CsvGenerationException, ConfigurationError

# タイムゾーン設定
JST = timezone(timedelta(hours=9))
# Cloud Logging設定
logger = logging.getLogger(__name__)


class DataProcessor:
    """メインのデータ処理クラス"""

    def __init__(self, config: AppConfig, gcs_manager=None, config_manager=None):
        self.app_config = config
        self.gcs_manager = gcs_manager
        self.data_path = config.storage.data_path
        logger.info(f"Using data path from config: {self.data_path}")

        # ConfigManagerを設定（引数で渡された場合、または新しく作成）
        if config_manager is None and gcs_manager is not None:
            self.config_manager = ConfigManager(gcs_manager=gcs_manager)
        else:
            self.config_manager = config_manager

        # 複数データベース対応のDatabaseManagerを初期化
        self.db_manager = DatabaseManager()
        self.db_manager.set_app_config(config)

        self.csv_processor = CSVProcessor()
        self.query_processor: Optional[QueryProcessor] = None
        self.queries_config: Optional[Dict[str, Any]] = None
        # 除外するカラムの設定
        self.exclude_columns_config = {
            "ResourceStatusHistory": ["TxnDate"]  # ResourceStatusHistoryクエリからTxnDateを除外
        }

    def load_queries_config(self) -> Dict[str, Any]:
        """クエリ設定をローカル設定ファイルから読み込み"""
        try:
            queries = self.config_manager.get_queries_config()
            settings = self.config_manager.get_settings_config()

            config = {
                'queries': queries,
                'settings': settings
            }

            logger.info(f"Successfully loaded queries configuration from local config file")
            logger.info(f"Loaded {len(queries)} queries")

            return config
        except Exception as e:
            error_msg = f"Failed to load queries configuration: {str(e)}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg, original_exception=e)

    def process_query(self, query_config: QueryConfig, last_run_time: str, 
                     batch_size: int, timestamp: str) -> Optional[ProcessingResult]:
        """単一クエリを処理"""
        if not self.query_processor:
            raise RuntimeError("Query processor not initialized")

        query_name = query_config.name
        try:
            load_type = "Full Load" if not query_config.is_incremental else "Incremental"
            logger.info(f"Starting {load_type} processing for query: {query_name}")

            # Systemdate設定を取得して適用
            systemdate_config = self.config_manager.get_systemdate_config()
            if systemdate_config:
                logger.info(f"Applying Systemdate configuration: {systemdate_config}")
                # Systemdate範囲を適用したクエリ設定を作成
                modified_query_config = self.query_processor.process_query_with_systemdate_range(
                    query_config, systemdate_config, last_run_time
                )
                # 修正されたクエリ設定を使用
                query_config = modified_query_config

            # 差分データの件数をカウント
            record_count = self.query_processor.get_record_count(query_config, last_run_time)
            logger.info(f"Query {query_name} has {record_count:,} records to process ({load_type})")

            if record_count == 0:
                logger.info(f"Query {query_name} has no data to process, but creating directory structure")
                # 0件の場合でもディレクトリ構造を作成するためのダミー結果を返す
                return ProcessingResult(
                    query_name=query_name,
                    batch_count=0,
                    row_count=0,
                    total_size_bytes=0,
                    total_size_formatted=self.csv_processor.format_file_size(0)
                )

            # ヘッダー情報を事前取得
            headers = self.query_processor.get_headers(query_config, last_run_time)
            if not headers:
                logger.error(f"Failed to get headers for query {query_name}, skipping")
                return None

            logger.info(f"Query {query_name} has {len(headers)} columns: {', '.join(headers)}")

            # バッチ処理
            return self._process_batches(query_config, headers, last_run_time, batch_size, timestamp, record_count)

        except Exception as query_error:
            logger.error(f"Error processing query {query_name}: {str(query_error)}")
            raise

    def _process_batches(self, query_config: QueryConfig, headers: List[str], 
                        last_run_time: str, batch_size: int, timestamp_dir: str, 
                        record_count: int) -> ProcessingResult:
        """バッチ処理を実行"""
        if not self.query_processor:
            raise RuntimeError("Query processor not initialized")

        query_name = query_config.name
        offset = 0
        batch_number = 0
        processed_rows = 0
        query_total_size = 0
        expected_batches = (record_count + batch_size - 1) // batch_size

        # 除外するカラムを取得
        exclude_columns = self.exclude_columns_config.get(query_name, [])
        if exclude_columns:
            logger.info(f"Excluding columns for {query_name}: {', '.join(exclude_columns)}")

        while True:
            if not self.gcs_manager.check_execution_apply():
                raise CsvGenerationException(f"Executable file not found : {self.gcs_manager.metadata_path}/{self.gcs_manager.execution_apply_file}")
            batch_query = self.query_processor.create_batch_query(query_config, batch_size, offset, headers)

            try:
                logger.debug(f"Executing batch for {query_name}, offset: {offset}")

                rows = self.query_processor.execute_batch(batch_query, last_run_time, query_config)

                if not rows:
                    logger.info(f"No more data to process for {query_name}")
                    break

                batch_number += 1

                # CSVに変換してアップロード（除外カラムを指定）
                csv_data = self.csv_processor.create_csv_data(headers, rows, include_header=True, exclude_columns=exclude_columns)
                batch_filename = f"{query_name}_{batch_number:04d}.csv"
                gcs_path = f"{self.data_path}/{timestamp_dir}/{batch_filename}"

                batch_size_bytes = self.gcs_manager.upload_batch(gcs_path, csv_data, query_name, batch_number)
                query_total_size += batch_size_bytes

                processed_rows += len(rows)

                # メモリクリーンアップ
                del rows, csv_data
                gc.collect()

                # 進捗ログ
                progress_pct = (processed_rows / record_count) * 100
                logger.info(f"Batch {batch_number}/{expected_batches} completed for {query_name} "
                          f"({processed_rows:,}/{record_count:,} rows, {progress_pct:.1f}%)")
                
                offset += batch_size

            except CsvGenerationException as e:
                logger.error(f"{str(e)}")
                raise
            except Exception as batch_error:
                logger.error(f"Batch {batch_number} processing error for {query_name} at offset {offset}: {str(batch_error)}")
                break

        logger.info(f"Successfully processed {query_name}: {processed_rows:,} rows in {batch_number} batches "
                  f"(Total size: {self.csv_processor.format_file_size(query_total_size)})")

        return ProcessingResult(
            query_name=query_name,
            batch_count=batch_number,
            row_count=processed_rows,
            total_size_bytes=query_total_size,
            total_size_formatted=self.csv_processor.format_file_size(query_total_size)
        )

    def process_all_queries(self, force_full_load: bool, lookback_minutes: int, last_run_time: str) -> Tuple[List[str], List[ProcessingResult], datetime]:
        """全クエリを処理"""
        # タイムスタンプを日付と時刻に分離して生成
        current_time = datetime.now(JST)
        date_str = current_time.strftime("%Y%m%d")
        time_str = current_time.strftime("%H%M%S")

        # ディレクトリ構造: data/YYYYMMDD/HHMMSS/
        timestamp_dir = f"{date_str}/{time_str}"
        logger.info(f"Using timestamp directory: {timestamp_dir}")

        # GCS初期化
        self.gcs_manager.initialize()

        # クエリ設定をローカル設定ファイルから読み込み
        self.queries_config = self.load_queries_config()

        # データベース初期化
        self.query_processor = QueryProcessor(self.db_manager)

        # GCSManagerにQueryProcessorを設定（HistoryMainlineテーブルのSystemDate取得用）
        self.gcs_manager.set_query_processor(self.query_processor)

        # パラメータの妥当性チェック
        if not isinstance(last_run_time, str):
            logger.error(f"Invalid last_run_time type: {type(last_run_time)}")
            raise ValueError("Invalid last_run_time parameter")

        if not self.queries_config:
            raise RuntimeError("Queries config not loaded")

        batch_size = self.queries_config['settings']['batch_size']

        # テーブルフィルタリング機能
        enable_table_filtering = self.queries_config['settings'].get('enable_table_filtering', False)
        target_tables = self.queries_config['settings'].get('target_tables', [])

        # 処理対象クエリをフィルタリング
        queries_to_process = self.queries_config['queries']
        
        # テーブルフィルタリングが有効な場合のみフィルタリング
        if enable_table_filtering and target_tables:
            queries_to_process = [q for q in self.queries_config['queries'] if q.target_table in target_tables]
            logger.info(f"Table filtering enabled. Processing {len(queries_to_process)} out of {len(self.queries_config['queries'])} queries")
            logger.info(f"Target tables: {', '.join(target_tables)}")

        uploaded_queries = []
        processing_results = []

        # 処理対象クエリ一覧をログ出力
        logger.info("=== Processing Target Queries ===")
        for i, query_config in enumerate(queries_to_process, 1):
            load_type = "Incremental" if query_config.is_incremental else "Full Load"
            logger.info(f"{i:2d}. {query_config.name} ({query_config.description}) - {load_type}")
        logger.info(f"Total target queries: {len(queries_to_process)}")
        logger.info("===================================")

        # 各クエリを処理
        for query_config in queries_to_process:
            result = self.process_query(query_config, last_run_time, batch_size, timestamp_dir)
            if result:
                uploaded_queries.append(query_config.name)
                processing_results.append(result)
                
                # 0件の場合でもディレクトリ構造を作成
                if result.row_count == 0:
                    gcs_path = f"{self.data_path}/{timestamp_dir}"
                    try:
                        self.gcs_manager.create_directory_structure(gcs_path, query_config.name)
                        logger.info(f"Created directory structure for {query_config.name} with 0 records")
                    except Exception as e:
                        logger.warning(f"Failed to create directory structure for {query_config.name}: {str(e)}")

        
        # 最終実行時刻を更新（HistoryMainlineテーブルのSystemDateの最大値を使用）
        current_run_time = datetime.now(JST).replace(microsecond=0)
        # HistoryMainlineテーブルのSystemDateの最大値を取得してログ出力（main_dbを使用）
        max_system_date = self.query_processor.get_max_system_date("main_db")
        update_run_time = max_system_date
        if max_system_date is not None:
            if max_system_date.tzinfo is None:
                max_system_date = max_system_date.replace(tzinfo=JST)
            else:
                max_system_date = max_system_date.astimezone(JST)
            update_run_time = max_system_date
            logger.info(f"Updated last run time using HistoryMainline max SystemDate: {max_system_date} (JST)")
        else:
            update_run_time = current_run_time
            logger.warning(f"Updated last run time to: {current_run_time} (JST) - using current time (HistoryMainline SystemDate not available)")

        return uploaded_queries, processing_results, update_run_time

    def cleanup(self):
        """リソースをクリーンアップ"""
        self.db_manager.dispose_all()
        gc.collect()
        logger.info("Resources cleaned up")
