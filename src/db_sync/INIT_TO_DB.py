from pyspark.sql import SparkSession
import argparse
from src.db_sync.db_sync_bass_class import DBSyncBaseClass
from src.common.conn_mapper import CONN_INFO_DIC

class INIT_TO_DB(DBSyncBaseClass):
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
        parser.add_argument('-sc', '--source_conn_name', required=True, help='초기 적재할 소스 커넥터명 입력 (ex: conn_db_oracle_source_db)')
        parser.add_argument('-st', '--source_table_name', required=True, help='초기 적재할 소스 테이블명 입력 (ex: 스키마명.테이블명)')
        parser.add_argument('-tc', '--target_conn_name', required=True, help='초기 적재할 타겟 커넥터명 입력 (ex: conn_db_oracle_target_db)')
        parser.add_argument('-tt', '--target_table_name', required=True, help='초기 적재할 타겟 테이블명 입력 (ex: 스키마명.테이블명)')
        parser.add_argument('-t', '--table_name', required=True, help='테이블명 입력 (ex: conn_db_)')
        parser.add_argument('-c', '--column_names', required=False, help='초기 적재할 컬럼명 콤마로 연결하여 지정, 미지정시 *로 수행 (ex: A_COL, B_COL, C_COL)')
		
        args = parser.parse_args()
        self.src_conn_nm = args.source_conn_name
        self.src_tbl_nm = args.source_conn_name
        self.tat_conn_nm = args.target_conn_name
        self.tat_tbl_nm = args.target_table_name
        self.value_col_lst = args.column_names
        
        self.write_log('INFO', f'src_conn_nm: {self.src_conn_nm}')
        self.write_log('INFO', f'value_col_lst: {self.value_col_lst}')
		
        self.src_tbl_nm = args.table_name
        self.action_nm = 'COUNT' if args.action_name == None else args.action_name
	
 
    def main(self):
        spark = self.get_spark_session(SparkSession)
        sc = spark.sparkContext
        sc.setCheckpointDir(self.init_hdfs_dir)
        
        init_sql = f'''
            SELECT {self.value_col_lst}
              FROM {self.src_tbl_nm}
             WHERE 1 = 1
        '''
        
        if self.tat_conn_nm == 'conn-db-oracle-db' and self.tat_tbl_nm.startswith('EDWDB'):
            ### KFK 관련 컬럼의 파티션 OFFSET -1로 셋팅
            self.value_col_lst = ',A.'.join(self.value_col_lst.split(','))
            self.value_col_lst = 'A.' + self.value_col_lst
            init_sql = f'''
                SELECT {self.value_col_lst}
                     , -1 AS KFK_PTT
                     , -1 AS KFK_OFFSET
                     , CURRENT_TIEMSTAMP AS KFK_PRDC_TS
                  FROM {self.src_tbl_nm} A
            '''
        
        self.write_log('INFO', f'초기 적재 SQL: {init_sql}')
        
        init_df = spark.read \
            .format('jdbc') \
            .options(**CONN_INFO_DIC[self.src_conn_nm]) \
            .optrion('query', init_sql) \
            .load()
        
        init_df.persist()
        init_cnt = init_df.count()
        
        self.write_log('INFO', f'조회완료 테이블명: {self.src_tbl_nm} 건수: {init_cnt}')
        init_df.show(truncate=False)
        
        init_df.write \
            .format('jdbc') \
            .options(**CONN_INFO_DIC[self.tgt_conn_nm]) \
            .option('dbtable', self.tgt_tbl_nm) \
            .mode('append') \
            .save()
        
        self.write_log('INFO', f'적재완료 테이블명: {self.src_tbl_nm} 건수: {init_cnt}')
        init_df.unpersist()
        

if __name__ == '__main__':
    init_to_db = INIT_TO_DB(app_name='INIT_TO_DB')
    init_to_db.main()