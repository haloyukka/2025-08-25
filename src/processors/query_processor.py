import sqlalchemy
import logging
import re
from typing import List, Tuple, Any, Optional, TYPE_CHECKING, Dict
from datetime import datetime, timedelta, timezone
from core.models import QueryConfig

# タイムゾーン設定
JST = timezone(timedelta(hours=9))

# Cloud Logging設定
logger = logging.getLogger(__name__)


class QueryProcessor:
    """クエリ処理を管理するクラス"""

    def __init__(self, database_manager):
        self.database_manager = database_manager

    def _get_database_type(self, database_name: str) -> str:
        """データベースタイプを取得"""
        try:
            if not self.database_manager.app_config:
                logger.warning(f"AppConfig not set, defaulting to mssql for {database_name}")
                return 'mssql'
            
            if database_name not in self.database_manager.app_config.databases:
                logger.warning(f"Database '{database_name}' not found in AppConfig, defaulting to mssql")
                return 'mssql'
            
            db_config = self.database_manager.app_config.databases[database_name]
            return db_config.type.lower()
        except Exception as e:
            logger.warning(f"Failed to get database type for {database_name}, defaulting to mssql: {str(e)}")
            return 'mssql'

    def _has_with_clause(self, sql: str) -> bool:
        """SQLにWITH句が含まれているかチェック"""
        with_pattern = r'WITH\s+\w+\s+AS\s*\('
        return bool(re.search(with_pattern, sql, re.IGNORECASE | re.DOTALL))

    def _analyze_with_structure(self, sql: str) -> Dict[str, Any]:
        """WITH句の構造を詳細に解析"""
        analysis = {
            'has_with': False,
            'cte_count': 0,
            'cte_names': [],
            'main_query': '',
            'is_complex': False
        }
        
        # WITH句の存在確認
        with_pattern = r'WITH\s+(\w+)\s+AS\s*\('
        with_matches = list(re.finditer(with_pattern, sql, re.IGNORECASE | re.DOTALL))
        
        if not with_matches:
            return analysis
        
        analysis['has_with'] = True
        analysis['cte_count'] = len(with_matches)
        
        # 複数のCTEがある場合は複雑なWITH句として扱う
        if len(with_matches) > 1:
            logger.info(f"Multiple CTEs detected: {len(with_matches)}")
            analysis['is_complex'] = True
            
            # 複雑なWITH句の解析
            try:
                # 複数のCTEを解析
                cte_pattern = r'WITH\s+(\w+)\s+AS\s*\((.*?)\)'
                cte_matches = list(re.finditer(cte_pattern, sql, re.IGNORECASE | re.DOTALL))
                
                for match in cte_matches:
                    cte_name = match.group(1)
                    analysis['cte_names'].append(cte_name)
                
                # メインクエリを抽出（最後のCTEの後の部分）
                last_cte_end = cte_matches[-1].end()
                analysis['main_query'] = sql[last_cte_end:].strip()
                
            except Exception as e:
                logger.warning(f"Failed to parse complex WITH clause: {str(e)}")
                analysis['is_complex'] = False
        else:
            # 単一のCTEの解析
            try:
                # WITH句の構造を解析
                pattern = r'WITH\s+(\w+)\s+AS\s*\((.*?)\)\s*(SELECT.*)'
                match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
                
                if match:
                    cte_name = match.group(1)
                    cte_definition = match.group(2)
                    main_query = match.group(3)
                    
                    analysis['cte_names'].append(cte_name)
                    analysis['main_query'] = main_query
                    
                    logger.debug(f"Parsed single CTE: {cte_name}")
                else:
                    # より柔軟なパターンマッチング
                    pattern = r'WITH\s+(\w+)\s+AS\s*\((.*?)\)(?:\s*,\s*(\w+)\s+AS\s*\((.*?)\))*\s*(SELECT.*)'
                    match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
                    
                    if match:
                        cte_name = match.group(1)
                        analysis['cte_names'].append(cte_name)
                        
                        # 追加のCTEがある場合
                        if match.group(3):
                            analysis['cte_names'].append(match.group(3))
                            analysis['is_complex'] = True
                        
                        analysis['main_query'] = match.group(5) if match.group(5) else match.group(3)
                    else:
                        logger.warning("Could not parse WITH clause structure")
                        
            except Exception as e:
                logger.warning(f"Failed to parse WITH clause: {str(e)}")
        
        return analysis

    def _create_with_count_query(self, sql: str, database_type: str) -> str:
        """WITH句を含むクエリのCOUNT用クエリを作成"""
        analysis = self._analyze_with_structure(sql)
        
        if not analysis['has_with']:
            return f"SELECT COUNT(*) as record_count FROM ({sql}) as subquery"
        
        # WITH句が含まれている場合は、WITH句を外側に移動し、その後にCOUNT(*)を適用
        # 元のクエリ: WITH Pivoted AS (...) SELECT ... FROM Pivoted WHERE ...
        # 新しい構造: WITH Pivoted AS (...) SELECT COUNT(*) as record_count FROM (SELECT ... FROM Pivoted WHERE ...) as subquery
        
        # WITH句の部分を抽出
        with_pattern = r'(WITH\s+\w+\s+AS\s*\(.*?\))\s*(SELECT.*)'
        match = re.search(with_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            with_clause = match.group(1)  # WITH句の部分
            main_select = match.group(2)  # SELECT文の部分
            
            # 新しい構造を作成
            count_sql = f"""
            {with_clause}
            SELECT COUNT(*) as record_count FROM ({main_select}) as subquery
            """
            
            return count_sql
        else:
            # パースに失敗した場合は元の方法を使用
            logger.warning("Failed to parse WITH clause structure for count query, using fallback method")
            count_sql = re.sub(
                r'SELECT\s+(.*?)\s+FROM\s+',
                'SELECT COUNT(*) as record_count FROM ',
                sql,
                flags=re.IGNORECASE | re.DOTALL
            )
            
            return count_sql

    def _create_with_header_query(self, sql: str, database_type: str, limit_clause: str) -> str:
        """WITH句を含むクエリのヘッダー取得用クエリを作成"""
        analysis = self._analyze_with_structure(sql)
        
        if not analysis['has_with']:
            if database_type == 'mysql':
                return f"SELECT * FROM ({sql}) as subquery {limit_clause}"
            else:
                return f"SELECT {limit_clause} * FROM ({sql}) as subquery"
        
        # WITH句が含まれている場合は、WITH句を外側に移動し、その後にLIMIT/TOPを適用
        # 元のクエリ: WITH Pivoted AS (...) SELECT ... FROM Pivoted WHERE ...
        # 新しい構造: WITH Pivoted AS (...) SELECT {limit_clause} * FROM (SELECT ... FROM Pivoted WHERE ...) as subquery
        
        # WITH句の部分を抽出
        with_pattern = r'(WITH\s+\w+\s+AS\s*\(.*?\))\s*(SELECT.*)'
        match = re.search(with_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            with_clause = match.group(1)  # WITH句の部分
            main_select = match.group(2)  # SELECT文の部分
            
            # 新しい構造を作成
            if database_type == 'mysql':
                # MySQLの場合はLIMITを追加
                header_sql = f"""
                {with_clause}
                SELECT * FROM ({main_select}) as subquery {limit_clause}
                """
            else:
                # SQL Serverの場合はTOPを追加
                header_sql = f"""
                {with_clause}
                SELECT {limit_clause} * FROM ({main_select}) as subquery
                """
            
            return header_sql
        else:
            # パースに失敗した場合は元の方法を使用
            logger.warning("Failed to parse WITH clause structure for header query, using fallback method")
            if database_type == 'mysql':
                # MySQLの場合はLIMITを追加
                header_sql = re.sub(
                    r'SELECT\s+(.*?)\s+FROM\s+',
                    f'SELECT \\1 FROM ',
                    sql,
                    flags=re.IGNORECASE | re.DOTALL
                )
                return f"{header_sql} {limit_clause}"
            else:
                # SQL Serverの場合はTOPを追加
                header_sql = re.sub(
                    r'SELECT\s+(.*?)\s+FROM\s+',
                    f'SELECT {limit_clause} \\1 FROM ',
                    sql,
                    flags=re.IGNORECASE | re.DOTALL
                )
                return header_sql

    def _create_with_batch_query(self, sql: str, database_type: str, select_columns: str, order_column: str, offset: int, batch_size: int) -> str:
        """WITH句を含むクエリのバッチ処理用クエリを作成"""
        analysis = self._analyze_with_structure(sql)
        
        if not analysis['has_with']:
            if database_type == 'mysql':
                return f"""
                SELECT {select_columns} FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY {order_column} ASC) as rn 
                    FROM ({sql}) as subquery
                ) as numbered 
                WHERE rn > {offset} AND rn <= {offset + batch_size}
                """
            else:
                return f"""
                SELECT {select_columns} FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY {order_column} ASC) as rn 
                    FROM ({sql}) as subquery
                ) as numbered 
                WHERE rn > {offset} AND rn <= {offset + batch_size}
                """
        
        # WITH句が含まれている場合は、WITH句を外側に移動し、その後にROW_NUMBERを適用
        # 元のクエリ: WITH Pivoted AS (...) SELECT ... FROM Pivoted WHERE ...
        # 新しい構造: WITH Pivoted AS (...) SELECT {select_columns} FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY {order_column} ASC) as rn FROM (SELECT ... FROM Pivoted WHERE ...) as subquery) as numbered WHERE rn > {offset} AND rn <= {offset + batch_size}
        
        # WITH句の部分を抽出
        with_pattern = r'(WITH\s+\w+\s+AS\s*\(.*?\))\s*(SELECT.*)'
        match = re.search(with_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            with_clause = match.group(1)  # WITH句の部分
            main_select = match.group(2)  # SELECT文の部分
            
            # 新しい構造を作成
            batch_sql = f"""
            {with_clause}
            SELECT {select_columns} FROM (
                SELECT *, ROW_NUMBER() OVER (ORDER BY {order_column} ASC) as rn 
                FROM ({main_select}) as subquery
            ) as numbered 
            WHERE rn > {offset} AND rn <= {offset + batch_size}
            """
            
            return batch_sql
        else:
            # パースに失敗した場合は元の方法を使用
            logger.warning("Failed to parse WITH clause structure, using fallback method")
            batch_sql = re.sub(
                r'SELECT\s+(.*?)\s+FROM\s+',
                f'SELECT \\1, ROW_NUMBER() OVER (ORDER BY {order_column} ASC) as rn FROM ',
                sql,
                flags=re.IGNORECASE | re.DOTALL
            )
            
            return f"""
            SELECT {select_columns} FROM ({batch_sql}) as numbered 
            WHERE rn > {offset} AND rn <= {offset + batch_size}
            """

    def _get_limit_clause(self, database_type: str) -> str:
        """データベースタイプに応じたLIMIT句を取得"""
        if database_type == 'mysql':
            return 'LIMIT 1'
        else:
            return 'TOP 1'

    def _adjust_parameter_placeholder(self, sql: str, database_type: str) -> str:
        """データベースタイプに応じてパラメータプレースホルダーを調整"""
        # :last_run_timeの形式をそのまま使用
        return sql

    def _get_column_quote(self, database_type: str) -> str:
        """データベースタイプに応じたカラム名の引用符を取得"""
        if database_type == 'mysql':
            return '`'  # MySQL用のバッククォート
        else:
            return '['  # SQL Server用の角括弧

    def _get_column_quote_close(self, database_type: str) -> str:
        """データベースタイプに応じたカラム名の閉じ引用符を取得"""
        if database_type == 'mysql':
            return '`'  # MySQL用のバッククォート
        else:
            return ']'  # SQL Server用の角括弧

    def get_record_count(self, query_config: QueryConfig, last_run_time: str) -> int:
        """差分データのレコード数を取得"""
        try:
            engine = self.database_manager.get_engine(query_config.database_name)
            database_type = self._get_database_type(query_config.database_name)

            with engine.connect() as connection:
                base_sql = query_config.sql
                adjusted_sql = self._adjust_parameter_placeholder(base_sql, database_type)

                # WITH句の解析
                analysis = self._analyze_with_structure(query_config.sql)
                if analysis['has_with']:
                    logger.info(f"Query {query_config.name} contains WITH clause with {analysis['cte_count']} CTE(s)")
                    if analysis['is_complex']:
                        logger.info(f"Complex WITH clause detected with CTEs: {', '.join(analysis['cte_names'])}")
                
                # WITH句を含むクエリの場合は特別な処理
                if analysis['has_with']:
                    count_sql = self._create_with_count_query(adjusted_sql, database_type)
                    logger.debug(f"WITH clause count SQL for {query_config.name}: {count_sql}")
                else:
                    # 通常のクエリの場合はサブクエリとして使用
                    count_sql = f"SELECT COUNT(*) as record_count FROM ({adjusted_sql}) as subquery"
                    logger.debug(f"Standard count SQL for {query_config.name}: {count_sql}")

                if query_config.is_incremental and query_config.timestamp_column is not None:
                    result = connection.execute(sqlalchemy.text(count_sql), {'last_run_time': last_run_time})
                else:
                    result = connection.execute(sqlalchemy.text(count_sql))

                record_count = result.scalar()
                result.close()
                return int(record_count) if record_count is not None else 0
        except Exception as e:
            logger.error(f"Failed to get record count for query {query_config.name}: {str(e)}")
            raise

    def _retry_record_count(self, query_config: QueryConfig, last_run_time: str) -> int:
        """レコード数取得のリトライ処理"""
        try:
            logger.info(f"Retrying record count with direct query for {query_config.name}")
            engine = self.database_manager.get_engine(query_config.database_name)
            database_type = self._get_database_type(query_config.database_name)
            limit_clause = self._get_limit_clause(database_type)

            with engine.connect() as connection:
                base_sql = query_config.sql
                adjusted_sql = self._adjust_parameter_placeholder(base_sql, database_type)

                # WITH句の解析
                analysis = self._analyze_with_structure(query_config.sql)
                
                if analysis['has_with']:
                    test_sql = self._create_with_header_query(adjusted_sql, database_type, limit_clause)
                else:
                    if database_type == 'mysql':
                        test_sql = f"SELECT * FROM ({adjusted_sql}) as subquery {limit_clause}"
                    else:
                        test_sql = f"SELECT {limit_clause} * FROM ({adjusted_sql}) as subquery"

                logger.debug(f"Test SQL for {query_config.name}: {test_sql}")

                if query_config.is_incremental and query_config.timestamp_column is not None:
                    test_result = connection.execute(sqlalchemy.text(test_sql), {'last_run_time': last_run_time})
                else:
                    test_result = connection.execute(sqlalchemy.text(test_sql))

                has_records = test_result.fetchone() is not None
                test_result.close()

                if has_records:
                    # WITH句を含むクエリの場合は特別な処理
                    if analysis['has_with']:
                        count_sql = self._create_with_count_query(adjusted_sql, database_type)
                    else:
                        count_sql = f"SELECT COUNT(*) as record_count FROM ({adjusted_sql}) as subquery"
                    
                    if query_config.is_incremental and query_config.timestamp_column is not None:
                        result = connection.execute(sqlalchemy.text(count_sql), {'last_run_time': last_run_time})
                    else:
                        result = connection.execute(sqlalchemy.text(count_sql))
                    record_count = result.scalar()
                    result.close()
                    logger.info(f"Successfully got record count for {query_config.name}: {record_count}")
                    return int(record_count) if record_count is not None else 0
                else:
                    logger.info(f"No records found for {query_config.name}")
                    return 0
        except Exception as retry_error:
            logger.error(f"Retry also failed for query {query_config.name}: {str(retry_error)}")
            return 0

    def get_headers(self, query_config: QueryConfig, last_run_time: str) -> List[str]:
        """クエリのヘッダー情報を取得"""
        try:
            engine = self.database_manager.get_engine(query_config.database_name)
            database_type = self._get_database_type(query_config.database_name)
            limit_clause = self._get_limit_clause(database_type)

            with engine.connect() as connection:
                base_sql = query_config.sql
                adjusted_sql = self._adjust_parameter_placeholder(base_sql, database_type)

                # WITH句の解析
                analysis = self._analyze_with_structure(query_config.sql)
                
                if analysis['has_with']:
                    header_sql = self._create_with_header_query(adjusted_sql, database_type, limit_clause)
                else:
                    if database_type == 'mysql':
                        header_sql = f"SELECT * FROM ({adjusted_sql}) as subquery {limit_clause}"
                    else:
                        header_sql = f"SELECT {limit_clause} * FROM ({adjusted_sql}) as subquery"

                logger.debug(f"Header SQL for {query_config.name}: {header_sql}")

                if query_config.is_incremental and query_config.timestamp_column is not None:
                    header_result = connection.execute(sqlalchemy.text(header_sql), {'last_run_time': last_run_time})
                else:
                    header_result = connection.execute(sqlalchemy.text(header_sql))

                headers = list(header_result.keys())
                header_result.close()
                return headers
        except Exception as e:
            logger.error(f"Failed to get headers for query {query_config.name}: {str(e)}")
            logger.error(f"Query SQL: {query_config.sql}")
            if query_config.is_incremental and query_config.timestamp_column is not None:
                logger.error(f"Last run time parameter: {last_run_time}")
            raise e

    def create_batch_query(self, query_config: QueryConfig, batch_size: int, offset: int, headers: List[str]) -> sqlalchemy.TextClause:
        """バッチ処理用のクエリを作成"""
        base_sql = query_config.sql
        database_type = self._get_database_type(query_config.database_name)
        adjusted_sql = self._adjust_parameter_placeholder(base_sql, database_type)

        # `rn` カラムを除いた、必要なカラムのみを選択する
        select_columns = ", ".join([f'{self._get_column_quote(database_type)}{h}{self._get_column_quote_close(database_type)}' for h in headers])

        # すべてのケースでorder_by_columnを使用
        order_column = query_config.order_by_column

        # WITH句の解析
        analysis = self._analyze_with_structure(query_config.sql)
        
        if analysis['has_with']:
            batch_sql = self._create_with_batch_query(adjusted_sql, database_type, select_columns, order_column, offset, batch_size)
        else:
            if database_type == 'mysql':
                # MySQL用のバッチクエリ
                batch_sql = f"""
                SELECT {select_columns} FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY {order_column} ASC) as rn 
                    FROM ({adjusted_sql}) as subquery
                ) as numbered 
                WHERE rn > {offset} AND rn <= {offset + batch_size}
                """
            else:
                # SQL Server用のバッチクエリ
                batch_sql = f"""
                SELECT {select_columns} FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY {order_column} ASC) as rn 
                    FROM ({adjusted_sql}) as subquery
                ) as numbered 
                WHERE rn > {offset} AND rn <= {offset + batch_size}
                """

        return sqlalchemy.text(batch_sql)

    def execute_batch(self, batch_query: sqlalchemy.TextClause, last_run_time: str, query_config: QueryConfig) -> List[Tuple[Any, ...]]:
        """バッチクエリを実行"""
        engine = self.database_manager.get_engine(query_config.database_name)
        with engine.connect() as connection:
            params = {'last_run_time': last_run_time} if query_config.is_incremental and query_config.timestamp_column is not None else None
            if params:
                result = connection.execute(batch_query, params)
            else:
                result = connection.execute(batch_query)
            rows = result.fetchall()
            result.close()
            return [tuple(row) for row in rows]

    def _get_record_count_with_fallback(self, query_config: QueryConfig, last_run_time: str) -> int:
        """WITH句を含むクエリのレコード数取得のフォールバック処理"""
        try:
            engine = self.database_manager.get_engine(query_config.database_name)
            database_type = self._get_database_type(query_config.database_name)
            limit_clause = self._get_limit_clause(database_type)

            with engine.connect() as connection:
                base_sql = query_config.sql
                adjusted_sql = self._adjust_parameter_placeholder(base_sql, database_type)

                # WITH句の解析
                analysis = self._analyze_with_structure(query_config.sql)
                
                # まず1件だけ取得してデータが存在するかチェック
                if analysis['has_with']:
                    test_sql = self._create_with_header_query(adjusted_sql, database_type, limit_clause)
                else:
                    if database_type == 'mysql':
                        test_sql = f"SELECT * FROM ({adjusted_sql}) as subquery {limit_clause}"
                    else:
                        test_sql = f"SELECT {limit_clause} * FROM ({adjusted_sql}) as subquery"

                logger.debug(f"WITH clause fallback test SQL for {query_config.name}: {test_sql}")

                if query_config.is_incremental and query_config.timestamp_column is not None:
                    test_result = connection.execute(sqlalchemy.text(test_sql), {'last_run_time': last_run_time})
                else:
                    test_result = connection.execute(sqlalchemy.text(test_sql))

                has_records = test_result.fetchone() is not None
                test_result.close()

                if has_records:
                    # データが存在する場合は、より安全な方法でカウント
                    logger.info(f"Data exists for WITH clause query {query_config.name}, attempting safe count")
                    
                    # 段階的にカウントを試行
                    try:
                        # 方法1: WITH句用のCOUNT
                        count_sql = self._create_with_count_query(adjusted_sql, database_type)
                        if query_config.is_incremental and query_config.timestamp_column is not None:
                            result = connection.execute(sqlalchemy.text(count_sql), {'last_run_time': last_run_time})
                        else:
                            result = connection.execute(sqlalchemy.text(count_sql))
                        record_count = result.scalar()
                        result.close()
                        
                        if record_count is not None:
                            logger.info(f"Successfully counted records for WITH clause query {query_config.name}: {record_count}")
                            return int(record_count)
                    except Exception as count_error:
                        logger.warning(f"WITH clause count failed for query {query_config.name}: {str(count_error)}")
                    
                    # 方法2: より安全なカウント（段階的アプローチ）
                    try:
                        # まず小さなサンプルでテスト
                        if analysis['has_with']:
                            test_count_sql = self._create_with_header_query(adjusted_sql, database_type, 'TOP 1000')
                        else:
                            test_count_sql = f"SELECT TOP 1000 * FROM ({adjusted_sql}) as subquery"
                            
                        if query_config.is_incremental and query_config.timestamp_column is not None:
                            test_result = connection.execute(sqlalchemy.text(test_count_sql), {'last_run_time': last_run_time})
                        else:
                            test_result = connection.execute(sqlalchemy.text(test_count_sql))
                        
                        test_rows = test_result.fetchall()
                        test_result.close()
                        
                        if test_rows:
                            # データが存在する場合は、より大きなサンプルでカウント
                            logger.info(f"Data exists for {query_config.name}, attempting full count")
                            count_sql = self._create_with_count_query(adjusted_sql, database_type)
                            
                            if query_config.is_incremental and query_config.timestamp_column is not None:
                                result = connection.execute(sqlalchemy.text(count_sql), {'last_run_time': last_run_time})
                            else:
                                result = connection.execute(sqlalchemy.text(count_sql))
                            record_count = result.scalar()
                            result.close()
                            
                            if record_count is not None:
                                logger.info(f"Successfully counted records with safe method for {query_config.name}: {record_count}")
                                return int(record_count)
                    except Exception as safe_count_error:
                        logger.warning(f"Safe count also failed for WITH clause query {query_config.name}: {str(safe_count_error)}")
                    
                    # データは存在するが正確なカウントができない場合
                    logger.warning(f"Data exists for {query_config.name} but cannot get exact count, returning estimated count")
                    return 1  # 少なくとも1件は存在することを示す
                else:
                    logger.info(f"No records found for WITH clause query {query_config.name}")
                    return 0
                    
        except Exception as fallback_error:
            logger.error(f"Fallback record count failed for WITH clause query {query_config.name}: {str(fallback_error)}")
            return 0

    def get_max_system_date(self, database_name) -> Optional[datetime]:
        """HistoryMainlineテーブルのSystemDateの最大値を取得"""
        try:
            engine = self.database_manager.get_engine(database_name)
            with engine.connect() as connection:
                result = connection.execute(sqlalchemy.text("SELECT MAX(SystemDate) FROM HistoryMainline"))
                max_date = result.scalar()
                result.close()
                if max_date is not None:
                    # 文字列ならdatetimeに変換
                    if isinstance(max_date, str):
                        max_date = datetime.fromisoformat(max_date)
                    return max_date
                else:
                    logger.warning("No SystemDate found in HistoryMainline")
                    return None
        except Exception as e:
            logger.error(f"Failed to get max SystemDate from HistoryMainline: {str(e)}")
            return None

    def apply_systemdate_range(self, sql: str, systemdate_config: Dict[str, Any], last_run_time: str = None) -> str:
        """SQLクエリにSystemdateの範囲を適用する
        
        Args:
            sql: 元のSQLクエリ
            systemdate_config: Systemdate設定
            last_run_time: 前回実行時刻
            
        Returns:
            str: Systemdate範囲が適用されたSQLクエリ
        """
        try:
            if not systemdate_config.get('enabled', False):
                logger.info("Systemdate configuration is disabled, returning original SQL")
                return sql
            
            # 設定値の取得
            start_date = systemdate_config.get('start_date')
            end_date = systemdate_config.get('end_date')
            
            # 開始日時の決定
            if start_date:
                start_date_str = start_date
            else:
                # last_run_timeを使用
                start_date_str = last_run_time if last_run_time else "1900-01-01 00:00:00"
            
            # 終了日時の決定
            if end_date:
                end_date_str = end_date
            else:
                # 現在時刻を使用
                end_date_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
            
            logger.info(f"Applying Systemdate range: {start_date_str} to {end_date_str}")
            
            # SQLクエリにSystemdate範囲を適用
            if 'WHERE' in sql.upper():
                if ':last_run_time' in sql:
                    # last_run_timeパラメータを置換
                    sql = sql.replace(':last_run_time', f"'{start_date_str}'")
                else:
                    # WHERE句の後にANDで追加
                    sql = re.sub(r'WHERE\s+(.*)', rf'WHERE \1 AND SystemDate BETWEEN \'{start_date_str}\' AND \'{end_date_str}\'', sql, flags=re.IGNORECASE)
            else:
                # WHERE句がない場合は追加
                sql = f"{sql} WHERE SystemDate BETWEEN '{start_date_str}' AND '{end_date_str}'"
            
            logger.info(f"SQL with Systemdate range applied: {sql}")
            return sql
            
        except Exception as e:
            logger.error(f"Failed to apply Systemdate range to SQL: {str(e)}")
            return sql

    def process_query_with_systemdate_range(self, query_config: QueryConfig, systemdate_config: Dict[str, Any], last_run_time: str = None) -> QueryConfig:
        """QueryConfigにSystemdate範囲を適用する
        
        Args:
            query_config: 元のQueryConfig
            systemdate_config: Systemdate設定
            last_run_time: 前回実行時刻
            
        Returns:
            QueryConfig: Systemdate範囲が適用されたQueryConfig
        """
        try:
            # 新しいQueryConfigオブジェクトを作成
            modified_config = QueryConfig(
                name=query_config.name,
                description=query_config.description,
                sql=query_config.sql,
                timestamp_column=query_config.timestamp_column,
                primary_key=query_config.primary_key,
                target_table=query_config.target_table,
                is_incremental=query_config.is_incremental,
                order_by_column=query_config.order_by_column
            )
            
            # Systemdate範囲を適用
            if query_config.is_incremental and query_config.timestamp_column:
                modified_config.sql = self.apply_systemdate_range(
                    query_config.sql, 
                    systemdate_config, 
                    last_run_time
                )
                logger.info(f"Applied Systemdate range to query: {query_config.name}")
            
            return modified_config
            
        except Exception as e:
            logger.error(f"Failed to process query with Systemdate range: {str(e)}")
            return query_config
