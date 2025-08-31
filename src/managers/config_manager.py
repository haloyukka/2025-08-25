import json
import yaml
import logging
from typing import Dict, Any, List, Optional
from core.models import QueryConfig, ProcessingResult, AppConfig, CSVtoSchema, BigQueryConfig, StorageConfig, DatabaseConfig
from Exceptions.CsvToBigQueryException import ConfigError
import os

# Cloud Logging設定
logger = logging.getLogger(__name__)


class ConfigManager:
    """統合された設定ファイル管理クラス（YAMLベース）"""
    
    DEFAULT_CONFIG_PATH = "config/config.yaml"
    
    def __init__(self, config_path: Optional[str] = None, gcs_manager: Optional[Any] = None) -> None:
        """設定ファイルを初期化する
        
        Args:
            config_path: 設定ファイルのパス。Noneの場合はデフォルトパスを使用
            gcs_manager: GCSマネージャー（オプション）
        """
        self.gcs_manager = gcs_manager
        
        if config_path is None:
            config_path = self._find_config_file()
        
        self.config_path = config_path
        self._raw_config = self._load_yaml_config(config_path)
        self._gcs_config = None  # GCSから読み込んだ設定をキャッシュ
        logger.info(f"Configuration loaded from: {config_path}")
    
    def _find_config_file(self) -> str:
        """設定ファイルのパスを検索する
        
        Returns:
            str: 設定ファイルの絶対パス
            
        Raises:
            ConfigError: 設定ファイルが見つからない場合
        """
        # 現在のファイルのディレクトリを基準にパスを構築
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)  # functions-source/
        
        # 検索するパスのリスト
        search_paths = [
            os.path.join(current_dir, '..', 'config', 'config.yaml'),
            os.path.join(project_root, 'config', 'config.yaml'),
            self.DEFAULT_CONFIG_PATH
        ]
        
        for path in search_paths:
            abs_path = os.path.abspath(path)
            if os.path.exists(abs_path):
                logger.info(f"Config file found at: {abs_path}")
                return abs_path
        
        # 設定ファイルが見つからない場合
        error_message = f"Configuration file not found. Searched paths:\n"
        error_message += "\n".join([f"- {os.path.abspath(path)}" for path in search_paths])
        error_message += f"\nCurrent working directory: {os.getcwd()}"
        raise ConfigError(error_message)
    
    def _load_yaml_config(self, config_path: str) -> Dict[str, Any]:
        """YAML設定ファイルを読み込む
        
        Args:
            config_path: 設定ファイルのパス
            
        Returns:
            Dict[str, Any]: 読み込んだ設定データ
            
        Raises:
            ConfigError: ファイルが見つからない、またはYAML解析エラーの場合
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigError(f"Configuration file not found at: {config_path}")
        except yaml.YAMLError as e:
            raise ConfigError("Error parsing YAML config.", original_exception=e)
    
    def _load_gcs_config(self) -> Dict[str, Any]:
        """GCSから設定を読み込む
        
        Returns:
            Dict[str, Any]: GCSから読み込んだ設定データ
            
        Raises:
            ConfigError: GCSマネージャーが設定されていない、または読み込みエラーの場合
        """
        if not self.gcs_manager:
            raise ConfigError("GCS manager not initialized")
        
        try:
            if self._gcs_config is None:
                self._gcs_config = self.gcs_manager.load_config_from_gcs()
            return self._gcs_config
        except Exception as e:
            raise ConfigError(f"Failed to load config from GCS: {str(e)}", original_exception=e)
    
    def get_gcs_config(self) -> Dict[str, Optional[str]]:
        """GCS設定を取得（STORAGEセクションから取得）"""
        storage_config = self._raw_config.get('STORAGE', {})
        result = {
            'bucket_name': storage_config.get('TARGET_BUCKET'),
            'metadata_path': storage_config.get('METADATA_PATH'),
            'metadata_name': storage_config.get('METADATA_NAME'),
            'data_path': storage_config.get('DATA_PATH'),
            'config_path': storage_config.get('CONFIG_PATH'),
            'config_name': storage_config.get('CONFIG_NAME')
        }
        # Noneの値はdictから除外
        return {k: v for k, v in result.items() if v is not None}
    
    def get_database_config(self, database_name: str = "main_db") -> Dict[str, Any]:
        """指定されたデータベースの設定を取得"""
        databases_config = self._raw_config.get('DATABASES', {})
        if database_name not in databases_config:
            raise ValueError(f"Database '{database_name}' not found in configuration")
        
        db_config = databases_config[database_name]
        return {
            'db_user': db_config.get('USER'),
            'db_pass': db_config.get('PASSWORD'),
            'db_name': db_config.get('NAME'),
            'db_host': db_config.get('HOST'),
            'db_port': db_config.get('PORT'),
            'db_type': db_config.get('TYPE', 'mssql')
        }
    
    def get_all_database_names(self) -> List[str]:
        """設定されている全てのデータベース名を取得"""
        databases_config = self._raw_config.get('DATABASES', {})
        return list(databases_config.keys())
    
    def get_settings_config(self) -> Dict[str, Any]:
        """設定項目を取得（GCSから読み込み）"""
        try:
            gcs_config = self._load_gcs_config()
            return gcs_config.get('SETTINGS', {})
        except ConfigError:
            logger.warning("Failed to load SETTINGS from GCS, using empty settings")
            return {}
    
    def get_systemdate_config(self) -> Dict[str, Any]:
        """Systemdate設定を取得（GCSから読み込み）"""
        try:
            gcs_config = self._load_gcs_config()
            systemdate_config = gcs_config.get('SYSTEMDATE', {})
            
            if not systemdate_config.get('enabled', False):
                logger.info("Systemdate configuration is disabled")
                return {}
            
            return systemdate_config
        except ConfigError:
            logger.warning("Failed to load SYSTEMDATE from GCS, using empty settings")
            return {}
        
    def get_queries_config(self) -> List[QueryConfig]:
        """クエリ設定を取得（GCSから読み込み）"""
        try:
            gcs_config = self._load_gcs_config()
            queries_data = gcs_config.get('QUERIES', [])
            return [QueryConfig(**query) for query in queries_data]
        except ConfigError:
            logger.warning("Failed to load QUERIES from GCS, using empty queries")
            return []
    
    
    def load_typed_config(self) -> AppConfig:
        """AppConfigオブジェクトを生成する"""
        try:
            # CSV to Schema設定
            targets_csv_info = [
                CSVtoSchema(
                    table_name=self._raw_config['CONVERSION'][info]['TABLE_NAME'],
                    csv_name=self._raw_config['CONVERSION'][info]['CSV_NAME'],
                    schema_name=self._raw_config['CONVERSION'][info]['SCHEMA_NAME'])
                for info in self._raw_config['CONVERSION']
            ]
            
            # BigQuery設定
            bq_conf = BigQueryConfig(
                project_id=self._raw_config['BIGQUERY']['PROJECT_ID'],
                dataset_id=self._raw_config['BIGQUERY']['DATASET_ID'],
                retry_max_wait=self._raw_config['BIGQUERY']['RETRY_MAX_WAIT'],
                retry_deadline=self._raw_config['BIGQUERY']['RETRY_DEAD_LINE']
            )
            
            # Storage設定
            storage_conf = StorageConfig(
                project_id=self._raw_config['STORAGE']['PROJECT_ID'],
                target_bucket=self._raw_config['STORAGE']['TARGET_BUCKET'],
                data_path=self._raw_config['STORAGE']['DATA_PATH'],
                schema_path=self._raw_config['STORAGE']['SCHEMA_PATH'],
                metadata_path=self._raw_config['STORAGE']['METADATA_PATH'],
                metadata_name=self._raw_config['STORAGE'].get('METADATA_NAME'),
                config_path=self._raw_config['STORAGE']['CONFIG_PATH'],
                config_name=self._raw_config['STORAGE'].get('CONFIG_NAME'),
                mergesql_path=self._raw_config['STORAGE']['MERGESQL_PATH'],
                target_fact_folder=self._raw_config['STORAGE']['FACT_SQL_FOLDER'],
                target_dim_folder=self._raw_config['STORAGE']['DIM_SQL_FOLDER'],
                execution_apply=self._raw_config['STORAGE']['EXECUTE_APPLY'],
                lock_file_path=self._raw_config['STORAGE']['LOCK_FILE_PATH']
            )
            
            # データベース設定
            databases_config = {}
            for db_name, db_config in self._raw_config.get('DATABASES', {}).items():
                databases_config[db_name] = DatabaseConfig(
                    user=db_config['USER'],
                    password=db_config['PASSWORD'],
                    name=db_config['NAME'],
                    host=db_config['HOST'],
                    port=db_config['PORT'],
                    type=db_config.get('TYPE', 'mssql')
                )
            
            return AppConfig(
                targets_csv_info=targets_csv_info,
                bigquery=bq_conf,
                storage=storage_conf,
                databases=databases_config,
                skip_leading_rows=self._raw_config.get('SKIP_LEADING_ROWS', 0),
                check_day_range=self._raw_config.get('CHECK_DAY_RANGE', 2),
                direct_import_flag=self._raw_config.get('DIRECT_IMPORT_FLAG', True)
            )
        except (KeyError, TypeError) as e:
            raise ConfigError("Config is missing required keys.", original_exception=e)
