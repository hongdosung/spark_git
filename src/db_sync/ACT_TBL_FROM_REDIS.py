from pyspark.sql import SparkSession
import argparse
from src.db_sync.db_sync_bass_class import DBSyncBaseClass
from src.common.conn_info import conn_info
#from src.common.postgres_sender import get_conn
import os

class ACT_TBL_FROM_REDIS(DBSyncBaseClass):
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
        self.PIECE_SIZE = 100000
        self.PTT_CNT = 6
        self.log_type = 'INFO'
    
    
    def get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--table_name', required=True, help='REDIS 초기적재 테이블명 입력 (ex: 테이블명)')
        parser.add_argument('-s', '--pseudo_columns', required=False, help='REDIS에 키 변경할 컬럼 지정 (ex: COL_A, COL_B)')
        parser.add_argument('-a', '--action_name', required=False, help='REDIS에 적재된 테이블에 수행할 action 지정, 미지정 시 COUNT (ex: SELECT, COUNT, DELETE)')
        parser.add_argument('-p', '--piece_size', required=False, help='수행 단위 PIECE_SIZE 입력, 미지정 시 100000 (ex: 10000)')
        parser.add_argument('-k', '--key_val', required=False, help='SELECT 수행 시 KEY 지정, 미지정시 *로 수행 (ex: A_COL, B_COL, C_COL)')
		
        args = parser.parse_args()
        self.src_tbl_nm = args.table_name
        self.pseudo_col_str = args.pseudo_columns
        self.action_nm = 'COUNT' if args.action_name == None else args.action_name
        if self.action_nm == 'SELECT': self.PIECE_SIZE = 10
        if args.piece_size: self.PIECE_SIZE = int(args.piece_size)
        self.tat_tbl_nm = args.piece_size
        self.key_val = '*' if args.key_val == None else args.key_val
        
        self.write_log('INFO', f'table_name: {self.src_tbl_nm}')
        self.write_log('INFO', f'pseudo_columns: {self.pseudo_col_str}')
        self.write_log('INFO', f'action_name: {self.action_name}')
        self.write_log('INFO', f'piece_size: {self.PIECE_SIZE}')
        self.write_log('INFO', f'key_val: {self.key_val}')

 
    def main(self):
        if not self.action_nm in ('COUNT', 'SELECT', 'DELETE'):
            self.write_log('INFO', 'ACTION명 존재하지 않습니다.')
            return
        elif self.action_nm == 'SELECT':
            spark = self.get_spark_session(SparkSession)
        
        if self.pseudo_col_str:
            pseudo_col_lst = self.pseudo_col_str.split(',')
        else:
            pseudo_col_lst = []
            
        if pseudo_col_lst:
            tmp_key_ptn = f"{self.src_tbl_nm}:{':'.join(pseudo_col_lst)}:"
        else:
            tmp_key_ptn = f"{self.src_tbl_nm}:"
    
        key_ptn = tmp_key_ptn + self.key_val
        self.write_log('INFO', f'key_ptn : {key_ptn}')
        
        rslt_lst = []
        final_lst = []
        scan_cnt = 0
        if self.action_nm == 'SELECT' and self.key_val != '*':
            scan_cnt += 1
            return_dict = self.get_value_with_key(key_ptn, tmp_key_ptn)
            
            rslt_lst.append(return_dict)
            final_lst = list(return_dict.keys())
            
            str_schema = ' STRING, '.join(final_lst) + ' STRING'
            return_df = spark.createDataFrame(rslt_lst, str_schema)
            return_df.show(truncate=False)
        else:
            for key in self.redis.redis_client.scan_iter(math=key_ptn, count=self.PIECE_SIZE):
                scan_cnt += 1
                if self.action_nm == 'DELETE':
                    self.redis.redis_client.delete(key)
                elif self.action_nm == 'SELECT':
                    return_dict = self.get_value_with_key(key, tmp_key_ptn)
                    rslt_lst.append(return_dict)
                    if scan_cnt == 1:
                        final_lst = list(return_dict.keys())
                    else:
                        final_lst = final_lst + list(set(list(return_dict.keys())) - set(final_lst))
                
                if scan_cnt%self.PIECE_SIZE == 0:
                    if self.action_nm == 'SELECT':
                        str_schema = ' STRING, '.join(final_lst) + ' STRING'
                        return_df = spark.createDataFrame(rslt_lst, str_schema)
                        return_df.show(truncate=False)
                        break
                    else:
                        self.write_log('INFO', f'{self.src_tbl_nm} 테이블 REDIS {self.action_nm} ({scan_cnt}건) 완료. {scan_cnt} 번째 key: {key}')
                    
        self.write_log('INFO', f'{self.src_tbl_nm} 테이블 REDIS {self.action_nm} (총 {scan_cnt}건) 완료.')
        
        
    def get_value_with_key(self, key, tmp_key_ptn):
        return_dict = {}
        pk_lst = key[len(tmp_key_ptn):].split(':')
        for i in range(len(pk_lst)):
            return_dict.update({f'PK{i}' : pk_lst[i]})
        
        result_dict = self.redis.redis_client.hgetall(key)
        result_dict = {k: v for k, v in result_dict.items()}
        
        return_dict.update(result_dict)
        
        return result_dict
        

if __name__ == '__main__':
    act_tbl_from_redis = ACT_TBL_FROM_REDIS(app_name='ACT_TBL_FROM_REDIS')
    act_tbl_from_redis.main()