from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, DecimalType
from datetime import datetime, timedelta

class RdbSender():
    def __init__(self, conn_info):
        self.conn_info = conn_info
    
    def query_from_df(self, sparkSession: SparkSession, query_tbl_nm: str, sensor_df: DataFrame, sensor_col_lst, target_col_lst, select_col_lst):
        '''
        sensor_df를 이용하여 타겟 DB 데이터 조회하여 데이터 프레임으로 RETURN
        - query_tab_nm: 조회하고자 하는 테이블명(ex: 스키마명.테이블명)
        - sensor_df: 조회 이용할 데이터 프레임
        - sensor_col_lst: 조회 조건에 사용할 sensor_df 컬럼
        - target_col_lst: sensor_col_lst에 대응된는 타겟 테이블 컬럼(sensor_col_lst 순서대로 매핑)
        - select_col_lst: 추출된 컬럼
        '''
        sub_select_str = '' # subquery 변수
        schema_dict = {col.name: col.dataType for col in sensor_df.schema}
        result_df = None
        
        ### row별 union 쿼리문 생성
        for row in sensor_df.collect():
            sub_select_str_per_row = 'SELECT '
            for sensor_col in sensor_col_lst:
                value = row[sensor_col]
                if isinstance(schema_dict[sensor_col], DecimalType) or isinstance(schema_dict[sensor_col], LongType): # NUMBER 타입
                    sub_select_str_per_row += f'{value} {sensor_col},'
                elif isinstance(schema_dict[sensor_col], TimestampType):  # DATA 타입
                    sub_select_str_per_row += f"TO_DATE('{value}', 'YYYY-MM-DD HH24:MI:SS') {sensor_col},"
                else:
                    sub_select_str_per_row += f"'{value}' {sensor_col},"  # STRING 타입
            
            sub_select_str_per_row = sub_select_str_per_row[:-1] + ' FROM DUAL UNION ALL ' 
            sub_select_str += sub_select_str_per_row
            
        ### Oracle Database의 쿼리 길이 제한 100,000이기에 99,000 초과시 나눠 실행
        if len(sub_select_str) > 99000:
            print(f'rdb_sender: len(sub_select_str) 초과: {len(sub_select_str)}')
            print(f'rdb_sender: database call will be separated.')
            sub_select_str = sub_select_str[:-11] # 맨 끝의 UNION ALL 제거
            sub_result_df = self._send_query(sparkSession, query_tab_nm, sub_select_str, sensor_col_lst, target_col_lst, select_col_lst)
            if result_df is None:
                result_df = sub_result_df
            else:
                result_df = result_df.unoin(sub_result_df)
            
            sub_select_str = '' # 초기화
        
        sub_select_str = sub_select_str[:-11] # 맨 끝의 UNION ALL 제거
        print(f'rdb_sender: len(sub_select_str): {len(sub_select_str)}')
        if len(sub_select_str) > 0:
            sub_result_df = self._send_query(sparkSession, query_tbl_nm, sub_select_str, sensor_col_lst, target_col_lst, select_col_lst)
            if result_df is None:
                result_df = sub_result_df
            else:
                result_df = result_df.unoin(sub_result_df)
        
        ### sensor_df 빈 데이터프레임인 경우
        if result_df is None:
            result_df = sparkSession.createDataFrame([], sensor_df.schema)
            
        return result_df
    
    
    def _send_query(self, sparkSession, query_tbl_nm, sub_select_str, sensor_col_lst, target_col_lst, select_col_lst):
        on_clause = ''
        for i in range(len(sensor_col_lst)):
            on_clause += f'E.{target_col_lst[i]} = S.{sensor_col_lst[i]} AND '
        
        on_clause = on_clause[:-4] # 맨 끝의 AND 제거
        select_col_str = ','.join(select_col_lst)
        
        query = f'''
            SELECT {select_col_str}
              FROM {query_tbl_nm} E
             WHERE EXISTS
                   (
                    SELECT 1
                      FROM ({sub_select_str}) S
                     WHERE {on_clause}
                   )
        '''
        
        return_df = sparkSession.read \
                    .format('jdbc') \
                    .options(**self.conn_info) \
                    .option('query', query) \
                    .load()
        
        print('rdb_sender.query_from_df: Response is Successful')
        
        return return_df    
    
    
    def query_sql_with_df(self, sparkSession: SparkSession, sql: str, df1: DataFrame, df2: DataFrame=None, df3: DataFrame=None):
        '''
        쿼리를 WITH VIEW를 생성하여 RETURN
        - df1: 필수 데이터프레임
        - WITH 문 생성하여 JOIN
        '''
        with_str = f'WITH df1 AS ({self._make_view(df1)})\n'
        if df2:
            with_str += f', df2 AS ({self._make_view(df2)})\n'
            
        if 3:
            with_str += f', df3 AS ({self._make_view(df3)})\n'
        
        query = with_str + sql
        return_df = sparkSession.read \
                    .format('jdbc') \
                    .options(**self.conn_info) \
                    .option('query', query) \
                    .load()
        
        return return_df
    
    
    def _make_view(self, df: DataFrame):
        schema_dict = {col.name: col.dataType for col in df.schema}
        with_sql = ''
        for row in df.collect():
            select_per_row = 'SELECT '
            for col_name, col_type in schema_dict.items():
                value = row[col_name]
                if isinstance(col_type, DecimalType) or isinstance(col_type, LongType):
                    select_per_row += f'{value} {col_name}, '
                elif isinstance(col_type, TimestampType):
                    select_per_row += f"TO_DATE('{value}', 'YYYY-MM-DD HH24:MI:SS') {col_name}, "
                else:
                    select_per_row += f"'{value}' {col_name}, "
            
            select_per_row = select_per_row[:-2] + ' FROM DUAL UNION ALL\n' # 콤마제거 후 줄 넘김
            with_sql += select_per_row
        
        with_sql = with_sql[:-10] # 맨 끝의 UNION ALL 제거
        print(f'rdb_sender: len(with_sql): {len(with_sql)}')
        
        if len(with_sql) == 0: # 빈 데이터프레임인 경우
            with_sql = 'SELECT '
            for col_name, col_type in schema_dict.items():
                if isinstance(col_type, DecimalType) or isinstance(col_type, LongType): # NUMBER 타입
                    with_sql += f'CAST(NULL AS NUMERIC) {col_name}, '
                elif isinstance(col_type, TimestampType): # DATE 타입
                    with_sql += f'CAST(NULL AS DATE) {col_name}, '
                else:
                    with_sql += f"NULL AS {col_name}, " # STRING 타입
            
            with_sql = with_sql[:-2] + ' FROM DUAL'
            
        return with_sql