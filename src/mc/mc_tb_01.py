########################################################################################################
# - 개요:
# - 소스 테이블:
# - 매핑 테이블:
# - 타겟 테이블:
# - 타겟 토픽:
# - 설명
#   . 감지기준: INSERT/UPDATE
#   . 조건:
#   . 분류코드:
# - 최초 작성일자: 2024.11.11
# - 수정이력
#   . 수정일자 / 수정자 / 수정내용
#   . 
########################################################################################################

from src.common.base_spark_class import BaseSparkClass
from src.common.kafka_sender import KafkaSender
from src.common.rdb_sender import RdbSender
from src.common.conn_info import ORACLE_BIZDB_INFO
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, get_json_object, trim, lit, date_format, date_add, date_sub, current_date, current_timestamp
from pyspark.sql.functions import regexp_replace, when, min, max, udf, expr, collect_list, to_date, broadcast
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, DecimalType
from datetime import datetime, timedelta

class MC_TB_01(BaseSparkClass):
    def __init__(self, app_name):
        super().__init__(app_name)
        
        ### HADOOP 경로설정        
        self.chk_hdfs_dir = self.base_chk_hdfs_dir + '/' + self.app_name
        self.init_hdfs_dir = self.base_init_hdfs_dir + '/' + self.app_name
        
        ### KAFKA 변수
        self.KAFKA_SOURCE_TOPICS = '##topic-prefix-oggbd_xxxdb##.스키마명.테이블명'
        self.MAX_OFFSETS_PER_TRIGGER = '10000' # 튜닝결과에 따라 변경 가능
        self.write_topic_nm = '##stage##.SPARK.TB_TABLE_SPAKR'
        self.kafka_sender = KafkaSender(write_topic_nm=self.write_topic_nm)
        self.kafka_key_col_lst = ['','']
        self.kafka_value_col_lst = ['','','','']
        
        ### REDIS 변수
        self.redis = self.get_redis_client(cluster='internal')
        self.RDS_COL_LST = ['','','','']
        self.RDS_VALUE_COL_LST = ['','','','']
        self.RDS_SCHEMA = ' STRING, '.join(self.RDS_COL_LST) + ' STRING'
        
        ### DB 변수
        self.rdb_sender = RdbSender(conn_info=ORACLE_BIZDB_INFO)
        
        ## 테이블 변수
        self.TB_PK_COL_LST = ['','']
        self.TB_VALUE_COL_LST = ['','','','']

        if '##stage##' == 'PROD':
            self.SPARK_DRIVER_CORES = '2'   # 소스단에 설정된 값으로 실제 반영되지 않고 참고용. spark-submit 시 설정필요
            self.SPARK_DRIVER_MEMORY = '2g' # 소스단에 설정된 값으로 실제 반영되지 않고 참고용. spark-submit 시 설정필요 
            self.SPARK_EXECUTOR_CORES = '3'
            self.SPARK_EXECUTOR_MEMORY = '2g'
            self.SPARK_EXECUTOR_INSTANCES = '2'
            self.SPARK_SQL_SHUFFLE_PARTITIONS = '6'
            self.SPARK_DRIVER_MEMORYOVERHEAD = '2g'
            self.SPARK_EXECUTOR_MEMORYOVERHEAD = '1g'
            self.KAFKA_PARTITION_CNT = '6'
            self.STATING_OFFSETS = 'latest'
            self.storage_level = StorageLevel.MEMORY_AND_DISK
            self.log_type = 'INFO'
        elif '##stage##' == 'DEV':
            self.SPARK_DRIVER_CORES = '1'   # 소스단에 설정된 값으로 실제 반영되지 않고 참고용. spark-submit 시 설정필요
            self.SPARK_DRIVER_MEMORY = '1g' # 소스단에 설정된 값으로 실제 반영되지 않고 참고용. spark-submit 시 설정필요 
            self.SPARK_EXECUTOR_CORES = '1'
            self.SPARK_EXECUTOR_MEMORY = '1g'
            self.SPARK_EXECUTOR_INSTANCES = '1'
            self.SPARK_SQL_SHUFFLE_PARTITIONS = '1'
            self.SPARK_DRIVER_MEMORYOVERHEAD = '1g'
            self.SPARK_EXECUTOR_MEMORYOVERHEAD = '1g'
            self.KAFKA_PARTITION_CNT = '1'
            self.STATING_OFFSETS = 'earliest'
            self.storage_level = StorageLevel.MEMORY_AND_DISK
            self.log_type = 'DEBUG'
        
        
    def _main(self):
        global db_schma
        
        spark = self.get_spark_session(SparkSession)
        sc = spark.sparkContext
        sc.setCheckpointDir(self.init_hdfs_dir)
        self.logger.logger.setLevel(self.log_type)
        
        self.first_start = True
        
        # VIEW SQL
        self.view_df_sql = f'''
            SELECT /*+ BRAODCAST(A) */
                   A.PK
                 , B.PK
              FROM 테이블명 A
              JOIN 테이블명 B
                ON A.PK = B.PK
             WHERE 1 = 1
               AND A = '01'
        '''
        
        # REDIS 스키마 확인용
        db_sql = f'''
            SELECT {','.join(self.TB_PK_COL_LST + self.TB_VALUE_COL_LST)}
              FROM 스키마.테이블명 
             WHERE ROWNUM = 1
        '''
        schema_df = self.edwdbs_execute_query(spark, db_sql)
        if self.log_type == 'DEBUG': schema_df.show(truncate=False)
        
        # JSON 스키마 선언
        self.df_json_schma = StructType([
            StructField('KKK1', StringType(), True),
            StructField('KKK2', StringType(), False),
            StructField('__OP_TYPE', StringType(), False),
            StructField('KFK_PTT', DecimalType(2,0), False), 
            StructField('KFK_OFFSET', DecimalType(20,0), False) 
        ])
        
        # 기타변수
        self.last_ym = ''
        self.map_df_lst_01 = []
        self.map_df_lst_02 = list()
        self.map_sql_lst = [
            '''SELECT A
                    , MAX(B) AS B
                 FROM 스키마.테이블명
                WHERE 1 = 1
                  AND A IS NOT NULL
                GROUP BY A
            ''',
            '''SELECT A
                    , MAX(B) AS B
                 FROM 스키마.테이블명
                WHERE 1 = 1
                  AND A IS NOT NULL
                GROUP BY A
            '''
        ]
        
        # 테이블 초기화
        self.map_df_lst_01.append(spark.createDataFram(data=[], schema='AA STRING, BB STRING'))
        
        # Streaming Set
        kafka_input_df = spark.readStream \
                         .format('kafka') \
                         .option('kafka.bootstrap.servers', self.KAFKA_BOOTSTRAP_SERVERS) \
                         .option('subscribe', self.KAFKA_SOURCE_TOPICS) \
                         .option('startingOffsets', self.STATING_OFFSETS) \
                         .option('failOnDataLoss', 'false') \
                         .option('maxOffsetsPerTrigger', self.MAX_OFFSETS_PER_TRIGGER) \
                         .load() \
                         .selectExpr(
                              'CAST(key AS STRING) AS KEY',
                              'CAST(value AS STRING) AS VALUE',
                              'topic',
                              'partition',
                              'offset') \
                         .writeStream \
                         .foreachBatch(lambda df, epoch_id: self.foreach_batch_process(df, epoch_id, spark)) \
                         .option('checkpointLocation', self.chk_hdfs_dir) \
                         .start()
        
        kafka_input_df.awaitTermination()
    
    
    def foreach_batch_process(self, df: DataFrame, epoch_id, spark: SparkSession):
        self.write_log('INFO', '시작', epoch_id)
        
        stream_df = self.null_transformer.convert_value_null_to_op_type(df)
        #stream_df = self.null_transformer.set_null_to_default(stream_df, schema_df.schema)
        
        stream_df = stream_df \
                    .select(
                            trim(get_json_object(col('KEY'), '$.payload.PK_01')).alias('PK_01'),
                            trim(get_json_object(col('KEY'), '$.payload.PK_02')).alias('PK_02'),
                            trim(get_json_object(col('VALUE'), '$.payload.VAL_01')).alias('VAL_01'),
                            trim(get_json_object(col('VALUE'), '$.payload.VAL_02')).alias('VAL_02'),
                            trim(get_json_object(col('VALUE'), '$.payload.VAL_03')).alias('VAL_03'),
                            trim(get_json_object(col('VALUE'), '$.payload.__OP_TYPE')).alias('__OP_TYPE'),
                            col('partition').alias('KFK_PTT'),
                            col('offset').alias('KFK_OFFSET')
                    )
        ############################################# 공통로직 완료 #############################################

        ############################################# 개별로직 완료 #############################################
        ### 개별로직 코드생성
        self.run_play(spark, epoch_id, stream_df)
        ############################################# 개별로직 종료 #############################################
        
        self.writ_log('INFO', '종료', epoch_id)
        self.first_start = False
    
    ### 개별로직 코드생성
    def run_play(self, spark: SparkSession, epoch_id, stream_df: DataFrame):
        ### Topic 타겟 변수
        tgt_tbl_nm = 'SCMDB.TABLE_NAME'
        write_topic_nm = '##topic-prefix-spark-testdb##' + '.' + tgt_tbl_nm
        
        ### KAFKA 변수
        kafka_sender = KafkaSender(write_topic_nm=write_topic_nm)
        self.kafka_pk_col_lst = ['A', 'B']
        self.kafka_value_col_lst = ['AA', 'BB', 'CC']
        
        ### 기타변수
        self.F_DC = '000001'
        
        if self.log_type == 'DEBUG' self.write_log('INFO', f'stream_df 인입 건수: {stream_df.count()}')
        if self.log_type == 'DEBUG' stream_df.orderBy(col('KFK_OFFSET').desc(), *self.TB_PK_COL_LST).show(truncate=False)
        
        stream_df = stream_df.filter((col('A') == '01' & 
                                      (col('B').isNotNull() | col('C').isNotNull() | col('D').isNotNull())) |
                                      (col('A') == '01' & col('__OP_TYPE') == 'I'))
        
        stream_df.persist(self.storage_level)
        stream_df_cnt = stream_df.count()
        if self.log_type == 'DEBUG': self.write_log('INFO', f'stream 필터된 건수: {stream_df_cnt}')
        if self.log_type == 'DEBUG': stream_df.orderBy(*self.TB_PK_COL_LST, col('KFK_OFFSET').desc()).show(truncate=False)
        
        if stream_df_cnt == 0:
            self.write_log('INFO', f'stream 처리 대상 없음', epoch_id)
            stream_df.unpersist()
            return
                
        ### stream 된 PK 값 저장
        start_pk_df = self.get_pk_df(spark, stream_df)
        start_pk_df.persist(self.storage_level).first()
        if self.log_type == 'DEBUG': self.write_log('INFO', f'stream PK 건수: {start_pk_df.count()}')
        if self.log_type == 'DEBUG': start_pk_df.orderBy(*self.TB_PK_COL_LST).show(truncate=False)

        ### TB 테이블 조회
        view_dg_df = self.edw_rdb_sender.query_sql_with_df(
            SparkSession = spark,
            sql          = self.view_df_sql,
            df1          = stream_df.select(*self.TB_PK_COL_LST).dropDuplicates()            
        )
        view_dg_df.persist(self.storage_level)
        if self.log_type == 'DEBUG': self.write_log('INFO', f'TB 테이블 건수: {view_dg_df.count()}')
        if self.log_type == 'DEBUG': view_dg_df.orderBy(col('A').desc()).show(truncate=False)

        ### TB2 테이블 조회
        view_dg_df2 = self.edw_rdb_sender.query_from_df(
            SparkSession   = spark,
            query_tbl_nm   = '스크마명.TB2',
            sensor_df      = stream_df.select('A').distinct(),
            sensor_col_lst = ['A'],
            target_col_lst = ['A'],
            select_col_lst = self.TB_VALUE_COL_LST
        )
        view_dg_df2.persist(self.storage_level)
        if self.log_type == 'DEBUG': self.write_log('INFO', f'TB2 테이블 건수: {view_dg_df2.count()}')
        if self.log_type == 'DEBUG': view_dg_df2.orderBy(col('A').desc()).show(truncate=False)

        ### REDIS TB 데이터 가져오기                   
        redis_mc_df = self.redis.send_hget(
            df           = stream_df,
            redis_tbl_nm = 'TB',
            pk_col_lst   = self.TB_PK_COL_LST,
            schema       = schema_df.schema,
            spark        = spark,
            chk_offset   = False
        )
        redis_mc_df.persist(self.storage_level)
        if self.log_type == 'DEBUG': self.write_log('INFO', f'REDIS TB 테이블 건수: {redis_mc_df.count()}')
        if self.log_type == 'DEBUG': redis_mc_df.orderBy(col('A').desc()).show(truncate=False)              

        ### REDIS TB2 데이터 가져오기                   
        redis_mc_df2 = self.redis.send_hget(
            df           = stream_df,
            redis_tbl_nm = 'TB',
            pk_col_lst   = self.TB_PK_COL_LST,
            schema       = self.RDS_SCHEMA,
            spark        = spark,
            chk_offset   = False
        )
        redis_mc_df2.persist(self.storage_level)
        if self.log_type == 'DEBUG': self.write_log('INFO', f'REDIS TB2 테이블 건수: {redis_mc_df2.count()}')
        if self.log_type == 'DEBUG': redis_mc_df2.orderBy(col('A').desc()).show(truncate=False)                    

        ### 결과 데이터 추출
        result_df = self.get_filt_df(spark, stream_df, view_dg_df, redis_mc_df)
        result_df.persist(self.storage_level)
        result_df_cnt = result_df.count()
        if self.log_type == 'DEBUG': self.write_log('INFO', f'최종 추출 건수: {result_df_cnt}')
        if self.log_type == 'DEBUG': result_df.orderBy(*self.TB_PK_COL_LST).show(truncate=False)
        
        ### 결과 데이터와 초기 스트림된  데이터 비교하여 삭제여부 판단
        get_del_df = self.get_del_df(start_pk_df, result_df)
        get_del_df.persist(self.storage_level).first()
        
        ### 삭제 데이터 KAFKA SEND
        del_cnt = get_del_df.count()
        if del_cnt > 0:
            self.write_log('INFO', f'KAFAK 삭제 데이터 건수: {del_cnt}', epoch_id)
            if self.log_type == 'DEBUG': get_del_df.orderBy(*self.kafka_pk_col_lst).show(truncate=False)
            
            ### KAFKA SEND
            kafka_sender.send_kafka_schema_payload(
                df            = get_del_df,
                pk_col_lst    = self.kafka_pk_col_lst,
                value_col_lst = []
            )
        else:
            self.write_log('INFO', f'KAFKA 삭제 건수 없음', epoch_id)
            
        #### 추출된 데이터 KAFKA SEND
        if result_df_cnt > 0:
            self.write_log('INFO', f'KAFKA 전송 데이터 건수: {result_df_cnt}', epoch_id)
            if self.log_type == 'DEBUG': result_df.orderBy(*self.kafka_pk_col_lst).show(truncate=False)
            
            ### KAFKA SEND
            kafka_sender.send_kafka_schema_payload(
                df            = result_df,
                pk_col_lst    = self.kafka_pk_col_lst,
                value_col_lst = self.kafka_value_col_lst
            )
        else:
            self.write_log('INFO', f'KAFKA 삭제 건수 없음', epoch_id)   

        spark.catalog.dropGlobalTempView('stream_view')
        if stream_df.is_cached: stream_df.unpersist()
        if start_pk_df.is_cached: start_pk_df.unpersist()
        if view_dg_df.is_cached: view_dg_df.unpersist()
        if view_dg_df.is_cached: view_dg_df2.unpersist()
        if redis_mc_df.is_cached: redis_mc_df.unpersist()
        if redis_mc_df2.is_cached: redis_mc_df2.unpersist()
        if result_df.is_cached: result_df.unpersist()
        if get_del_df.is_cached: get_del_df.unpersist()
        
        
    ### stream된 PK 데이터 저장    
    def get_pk_df(self, spark: SparkSession, stream_df: DataFrame):
        ### INSERT 스트림 데이터 한번 더 필터
        stream_df = stream_df.filter(((col('A').isNotNull()) & (col('B') == 'Y') & (col('__OP_TYPE') == 'I')) |
                                     (col('__OP_TYPE') == 'U'))
        
        ### REDIS에서 데이터 조회
        redis_df = self.redis.send_hget(
            df         = stream_df,
            tbl_nm     = 'TB_TABLE',
            pk_col_lst = self.TB_PK_COL_LST,
            schema     = schema_df.schema,
            spark      = spark
        )
        
        result_df = redis_df \
            .select(
                col('A').alias('A_PK'),
                expr("CASE WHEN A LIKE 'J%  THEN '02' ELSE '01' END AS B_BK"),
                lit('00000').alias('CODE')
            ) \
            .dropDuplicates()
        
        return result_df
    
    
    ### 삭제될 데이터 추출
    def get_del_df(self, start_pk_df: DataFrame, result_df: DataFrame):
        result_df = start_pk_df \
            .join(result_df.hint('broadcast'), self.kafka_pk_col_lst, 'left_anti') \
            .select(
                col('A'),
                col('B'),
                col('C')
            ) \
            .dropDuplicates()
        
        return result_df
    
    
    ### 최종 데이터 추출
    def get_filt_df(self, spark: SparkSession, stream_df: DataFrame, view_df: DataFrame, redis_df: DataFrame):
        stream_df.createOrReplaceGlobalTempView('stream_view')
        view_df.createOrReplaceTempView('TB_A')
        redis_df.createOrReplaceTempView('TB_B')
        
        pk_str = ','.join(self.TB_PK_COL_LST)
        result_df = spark.sql(f'''
            SELECT /* BROADCAST(A) */
                   A.PK AS PK_A
                ,  DATA_FORMAT(A_DT, 'yyyyMMddHHmmss') AS A_DT
                 , CURRENT_TIMESTAMP() AS L_DT  
              FROM (
                    SELECT ROW_NUMBER() OVER(PARTITION BY {pk_str} ORDER BY KFK_OFFSET DESC NULLS LAST) AS RNK_NO
                         , X.*
                      FROM global_temp.stream_view X
                     WHERE 1= 1
                   ) A
              JOIN (
                    SELECT *
                      FROM TB_A
                     WHERE 1 = 1
                   ) B
                ON A.PK = B.PK
        ''')
    
        return result_df
    
        
if __name__ == '__main__':
    mc_tb_01 = MC_TB_01(app_name='MC_TB_01')
    mc_tb_01.main()
    