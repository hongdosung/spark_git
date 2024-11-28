from pyspark.sql import SparkSession
import argparse
from src.db_sync.db_sync_bass_class import DBSyncBaseClass
from src.common.conn_info import conn_info
#from src.common.postgres_sender import get_conn
import os

class INIT_LOAD_TO_ORACLE(DBSyncBaseClass):
    def __init__(self, app_name):
        super().__init__(self, app_name)
        
        ### HADOOP 경로설정        
        self.chk_hdfs_dir = self.base_chk_hdfs_dir + '/' + self.app_name
        self.init_hdfs_dir = self.base_init_hdfs_dir + '/' + self.app_name

        self.get_args()

        self.SPARK_DRIVER_CORES = '2'   # 소스단에 설정된 값으로 실제 반영되지 않고 참고용. spark-submit 시 설정필요
        self.SPARK_DRIVER_MEMORY = '2g' # 소스단에 설정된 값으로 실제 반영되지 않고 참고용. spark-submit 시 설정필요 
        self.SPARK_EXECUTOR_CORES = '3'
        self.SPARK_EXECUTOR_MEMORY = '4g'
        self.SPARK_EXECUTOR_INSTANCES = '2'
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '6'
        self.SPARK_DRIVER_MEMORYOVERHEAD = '2g'
        self.SPARK_EXECUTOR_MEMORYOVERHEAD = '1g'
        
        self.SPARK_KEYOSERIALIZER_BUFFER_MAX = '2000m'
        self.PIECE_SIZE = 10000
        self.PTT_CNT = 6
        self.log_type = 'INFO'
    
    
    def get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-sc', '--source_db_type', required=True, help='초기 적재할 소스 데이터타입 입력 (ex: ORACLE)')
        parser.add_argument('-st', '--source_table_name', required=True, help='초기 적재할 소스 테이블명 입력 (ex: 스키마명.테이블명)')
        parser.add_argument('-tc', '--target_db_type', required=True, help='초기 적재할 타겟 데이터타입 입력 (ex: POSTGRESQL)')
        parser.add_argument('-tt', '--target_table_name', required=True, help='초기 적재할 타겟 테이블명 입력 (ex: 스키마명.테이블명)')
        parser.add_argument('-c', '--column_names', required=True, help='초기 적재할 컬럼명 콤마로 연결하여 지정, 미지정시 *로 수행 (ex: A_COL, B_COL, C_COL)')
		
        args = parser.parse_args()
        self.src_db_type = args.source_db_type
        self.src_tbl_nm = args.source_conn_name
        self.tat_db_type = args.target_db_type
        self.tat_tbl_nm = args.target_table_name
        self.value_col_lst = args.column_names
        
        self.write_log('INFO', f'src_conn_nm: {self.src_conn_nm}')
        self.write_log('INFO', f'value_col_lst: {self.value_col_lst}')
		
        self.src_tbl_nm = args.table_name
        self.action_nm = 'COUNT' if args.action_name == None else args.action_name
	
    def execute_query(self, spark, db_name, sql):
        fuc_nm = f'{db_name.lower()}_execute_query'
        method = getattr(self, fuc_nm, None)
        
        if method:
            return method(spark, sql).repartition(self.PTT_CNT)
        else:
            raise ValueError(f'Method {fuc_nm} not found')
        
 
    def main(self):
        spark = self.get_spark_session(SparkSession)
        sc = spark.sparkContext
        sc.setCheckpointDir(self.init_hdfs_dir)
        
        init_sql = f'''
            SELECT {self.value_col_lst}
              FROM {self.src_tbl_nm}
             WHERE 1 = 1
        '''
        
        self.write_log('INFO', f'초기 적재 SQL: {init_sql}')
        init_df = self.execute_query(spark, self.src_tbl_nm, init_sql)
        init_df.persist()        
        init_cnt = init_df.count()
        self.write_log('INFO', f'조회완료 테이블명: {self.src_tbl_nm} 건수: {init_cnt}')
        init_df.show(truncate=False)
        
        tgt_conn_name = f'{self.tat_tbl_nmx.upper()}_CONN_INFO'
        tgt_config = getattr(conn_info, tgt_conn_name, None)
        print(f'{**tgt_config}')
        
        self.write_log('INFO', f'조회완료 테이블명: {self.tat_tbl_nm} 건수: {init_cnt}')
        init_df.show(truncate=False)
        
        init_df.write \
            .format('jdbc') \
            .options(**tgt_config) \
            .option('dbtable', self.tgt_tbl_nm) \
            .mode('append') \
            .save()
        
        self.write_log('INFO', f'적재완료 테이블명: {self.tat_tbl_nm} 건수: {init_cnt}')
        init_df.unpersist()
        

if __name__ == '__main__':
    init_load_to_oracle = INIT_LOAD_TO_ORACLE(app_name='INIT_LOAD_TO_ORACLE')
    init_load_to_oracle.main()