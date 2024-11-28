from src.common.base_spark_class import BaseSparkClass
from src.common.kafka_sender import KafkaSender
from src.common.rdb_sender import RdbSender
from src.common.conn_info import ORACLE_BIZDB_INFO, ORACLE_EDWDB_INFO
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, get_json_object, trim, lit, date_format, date_add, date_sub, current_date, current_timestamp
from pyspark.sql.functions import regexp_replace, when, min, max, udf, expr, collect_list, to_date, broadcast
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, DecimalType
from datetime import datetime, timedelta

class BaseAppCommon(BaseSparkClass):
    def __init__(self, *args):
        super().__init__(*args)
        
        ### KAFKA 변수
        #self.STATING_OFFSETS = 'earliest' # earliest / latest
        self.storage_level = StorageLevel.MEMORY_AND_DISK
        #self.log_type = 'DEBUG'
        self.MAX_OFFSETS_PER_TRIGGER = '10000' # 튜닝결과에 따라 변경 가능
        self.KAFKA_PARTITION_CNT = ##partition_cnt##
        
        ### DB 객체 변수
        self.biz_rdb_sender = RdbSender(conn_info=ORACLE_BIZDB_INFO)
        self.ewd_rdb_sender = RdbSender(conn_info=ORACLE_EDWDB_INFO)
    
        ### REIDS 객체 변수
        self.redis_in  = self.get_redis_client(cluster='internal') # internal / outbound
        self.redis_out = self.get_redis_client(cluster='outbound') # internal / outbound
    
        if '##stage##' == 'PROD': 
            self.SPARK_EXECUTOR_CORES = '3'
            self.SPARK_EXECUTOR_MEMORY = '2g'
            self.SPARK_EXECUTOR_INSTANCES = '2'
            self.SPARK_SQL_SHUFFLE_PARTITIONS = '6'
            self.SPARK_DRIVER_MEMORYOVERHEAD = '2g'
            self.SPARK_EXECUTOR_MEMORYOVERHEAD = '1g'
            self.KAFKA_PARTITION_CNT = '6'
            self.STATING_OFFSETS = 'latest'
            self.log_type = 'INFO'
        elif '##stage##' == 'DEV':
            self.SPARK_EXECUTOR_CORES = '1'
            self.SPARK_EXECUTOR_MEMORY = '1g'
            self.SPARK_EXECUTOR_INSTANCES = '1'
            self.SPARK_SQL_SHUFFLE_PARTITIONS = '1'
            self.SPARK_DRIVER_MEMORYOVERHEAD = '1g'
            self.SPARK_EXECUTOR_MEMORYOVERHEAD = '1g'
            self.KAFKA_PARTITION_CNT = '1'
            self.STATING_OFFSETS = 'earliest'
            self.log_type = 'DEBUG'
    
    
    def merge_init_stream_by_redis(self, spark, stream_df, init_tbl_df, ptt_cnt, pk_col_lst, first_start, redis_conn, **kwargs):
        storage_level = kwargs.get('storage_level')
        table_nm = kwargs.get('table_nm')
        
        return self.init_integerator.merge_initial_by_stram_by_redis(spark, stream_df, init_tbl_df, ptt_cnt, pk_col_lst,
                                                                     first_start, redis_conn, storage_level, self.app_name, table_nm)
    
    
    def query_from_df(self, spark, query_tbl_nm, sensor_df, sensor_col_lst, target_col_lst, select_col_lst):
        return self.ewd_rdb_sender.query_from_df(spark, query_tbl_nm, sensor_df, sensor_col_lst, target_col_lst, select_col_lst)
    
    
    ### 초기 적재시 사용
    def reload_function(self, spark, epoch_id, sql, reload_df, pk_col_lst, storage_level):
        new_reload_df = self.edwdbs_execute_query(spark, sql).repartition(self.KAFKA_PARTITION_CNT, pk_col_lst)
        new_reload_df_cnt = new_reload_df.count()
        
        if new_reload_df_cnt == 0:
            self.write_log('INFO', '테이블 reload 데이터 없음', epoch_id)
            return        

        if self.log_type == 'DEBUG': self.write_log('INFO', f'최종 추출 건수: {new_reload_df_cnt}')
        if self.log_type == 'DEBUG': new_reload_df.orderBy(*self.pk_col_lst).show(truncate=False)
        
        reload_df.unpersist()
        reload_df = new_reload_df
        reload_df.persist()
        
        return reload_df
    
    
    ### 타겟 테이블에 데이터 적재 시 사용
    def tgt_tbl_init_load(self, spark, tgt_tbl_nm, filt_cd, init_sql: str=None):
        if init_sql == None: return
        
        ### LOAD 데이터
        tgt_init_df = self.edwdbs_execute_query(spark, init_sql)
        tgt_init_df.persist()

        if self.log_type == 'DEBUG': self.write_log('INFO', f'추출 건수: {tgt_init_df.count()}')
        if self.log_type == 'DEBUG': tgt_init_df.show(truncate=False)
        
        ### 타겟 데이터 삭제 쿼리
        tgt_del_sql = f'''
            DELETE FROM {tgt_tbl_nm} WHERE 컬럼 IN ('{filt_cd}')
        '''
        
        ### 타겟 데이터 삭제 후 확인쿼리
        tgt_cnt_sql = f'''
            SELECT COUNT(1) AS CNT
             FROM {tgt_tbl_nm} 
            WHERE 1 = 1
              AND 컬럼 IN ('{filt_cd}')
        '''
        
        ### 타겟테이블 적재 전 데이터 삭제
        del_cnt = spark.read \
                 .format('jdbc') \
                 .options(**ORACLE_EDWDB_INFO) \
                 .options('sessionInitStatement', tgt_del_sql) \
                 .option('query', tgt_cnt_sql) \
                 .load() \
                 .select(col('CNT').cast(LongType())) \
                 .first()[0]
        
        if self.log_type == 'DEBUG': self.write_log('INFO', f'타겟 테이블 삭제 후 건수: {del_cnt}')
        
        ### 타겟테이블 데이터 적재
        tgt_init_df.write \
                   .format('jdbc') \
                   .mode('append') \
                   .options(**ORACLE_EDWDB_INFO) \
                   .option('dbtable', tgt_tbl_nm) \
                   .save()
        
        self.write_log('INFO', f'타겟 {tgt_tbl_nm} 테이블 {filt_cd} 데이터 적재완료')
        
        tgt_init_df.uppersist()
        