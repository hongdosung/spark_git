import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from src.common.kafka_sender import KafkaSender
from src.common.postgres_sender import PostgresSender

class Logger:
    def __init__(self, app_name):
        logging.basicConfig(level=logging.INFO, 
                            format=f'[{app_name}][%(levelname)s][%(asctime)s.%(msecs)03d]%(message)s',
                            datefmt='%Y-%m-%d %H:%M%S')
        self.logger = logging.getLogger(app_name)
        self.app_name = app_name
        self.step_num = 0
        self.last_epoch_id = 0
        self.spark_log_tbnm = 'spark_app_log'
                
        '''
        self.postgres_sender = PostgresSender(
            host = '##spark_log_dbhost##'
            port = '##spark_log_dbport##'
            database_name =  '##spark_log_dbname##'
            user = '##spark_log_dbuser##'
            passwd = '##spark_log_dbpasswd##'
        )
        self.postgres_sender.cursor = self.postgres_sender.conn.cursor()     
        '''
        
    def write_log_epoch(self, log_type: str, msg: str, epoch_id=None):
        log_type_lst = ['debug', 'info', 'warning', 'error', 'critical']
        if log_type.lower() not in log_type_lst: return
        
        if self.last_epoch_id == epoch_id:
            self.step_num += 1
        else:
            self.step_num = 1
            self.last_epoch_id = epoch_id
        
        if epoch_id == None:
            getattr(self.logger, log_type.lower())(f'[STEP:{self.step_num}] {msg}')
        else:
            getattr(self.logger, log_type.lower())(f'[EPOCH:{epoch_id}][STEP:{self.step_num}] {msg}')
        
        """
        insert_msg = f'''INSERT INTO {self.spark_log_tbnm} VALUES("{self.app_name}", {datetime.now(timezone('Asia/Seoul'))}, "{msg}", "{epoch_id}") ''''
        try:
            self.postgres_sender.cursor.execute(insert_msg)
        except:
            self.postgres_sender = PostgresSender(
                host = '##spark_log_dbhost##'
        # The `wirte_log` method in the `baseSparkClass` class is a helper method used to write logs
        # using the `Logger` instance associated with the class. It takes three parameters:
        # 1. `log_type`: The type of log message being written (e.g., INFO, ERROR, DEBUG).
        # 2. `msg`: The actual log message to be written.
        # 3. `epoch_id` (optional): An optional parameter that can be used to specify an epoch ID
        # associated with the log message.
                port = '##spark_log_dbport##'
                database_name =  '##spark_log_dbname##'
                user = '##spark_log_dbuser##'
                passwd = '##spark_log_dbpasswd##'
           )
           self.postgres_sender.cursor = self.postgres_sender.conn.cursor() 
        """
    
    def set_perf_meta(self, your_self, case_nm=None):
        '''
        your_self: 해당 함수를 실행하는 클래스의 self 변수
        case_nm: max_offsets_per_trigger나 executor 정보는 자동 셋팅되면 직접 추가하고 싶은 comment가 있을 경우 작성
        
        ex)
        def _main(self):
            self.logger.set_perf_meta(self)
            ...
        
        def foreach_batch_process(self, df, epoch_id, spark):
            -------------- 로직 시작 ----------------------
            # 구간 1
            self.write_log_epoch('INFO', 'start', epoch_id)
            self.logger.append_stat(epoch_id, msg)
            # 구간 2
            self.write_log_epoch('INFO', 'stream_df.count()', epoch_id)
            self.logger.append_stat(epoch_id, 'stream_df.count()')
            # 구간 3
            self.write_log_epoch('INFO', 'process_df.count()', epoch_id)
            self.logger.append_stat(epoch_id, 'process_df.count()')
            # 전송
            self.logger.send_perf_stat(spark)
        '''
        
        self.perf_stat_lst = []
        self.app_name = getattr(your_self, 'app_name')
        self.executor_instances = getattr(your_self, 'SPARK_EXECUTOR_INSTANCES')
        self.executor_cores = getattr(your_self, 'SPARK_EXECUTOR_CORES')
        self.executor_memory = getattr(your_self, 'SPARK_EXECUTOR_MEMORY')
        self.max_offsets_per_trigger = getattr(your_self, 'MAX_OFFSETS_PER_TRIGGER')
        self.case_nm = f'max_offsets_per_trigger: {self.max_offsets_per_trigger}, exeutor_instances: {self.executor_instances}, executor_cores: {self.executor_cores}, executor_memory: {self.executor_memory}'
        
        if case_nm: # case_nm을 직접 주고 싶은 경우, self.case_nm 변수에 append
            self.case_nm += f', {case_nm}'
        
        self.kafka_sender = KafkaSender(write_topic_nm='##topic-prefix-spark-db##.스키마명.테이블명')      
    
    
    def append_stat(self, epoch_id, msg):
        evn_ts = datetime.now()
        self.perf_stat_lst.append({
             'APP_NAME': self.app_name,
             'CASE_NM': self.case_nm,
             'EPOCH_ID': epoch_id,
             'STEM_NUM': self.step_num,
             'ENT_TS': evn_ts,
             'MSG': msg
        })
        
    
    def send_perf_stat(self, sparkSession: SparkSession):
        pert_stat_df = sparkSession.createDataFrame(self.perf_stat_lst)
        self.kafka_sender.send_kafka_schema_payload(
            df = pert_stat_df,
            pk_col_lst = ['APP_NAME','CASE_NM','EPOCH_ID'],
            value_col_lst = ['APP_NAME','CASE_NM','EPOCH_ID','STEM_NUM','ENT_TS','MSG']
        )
        
        self.perf_stat_lst = [] # KAFKA 전송 후 초기화