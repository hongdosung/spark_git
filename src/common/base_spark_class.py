from src.common.conn_info import ORACLE_EDWDB_INFO, ORACLE_BIZDB_INFO
from src.common.null_transformer import NullTransformer
from src.common.redis_sender import RedisSender
from src.common.logger import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from src.common.init_integrator import InitIntegrator

class BaseSparkClass():
    def __init__(self, app_name):
        self.app_name = app_name
        self.base_chk_hdfs_dir = 'hdfs:///home/spark/chk_kafka_offset'
        self.base_init_hdfs_dir = 'hdfs:///home/spark/chk_init_dir'
        self.null_transformer = NullTransformer()
        self.init_integrator = InitIntegrator()
        self.logger = Logger(self.app_name)
        
        ## spark config 변수들
        self.KFAKA_BOOTSTRAP_SERVERS = '##bootstrap-servers##'
        self.SPARK_DRIVER_CORES = '##spark-driver-cores##'   # spark-submit shell 배포 실행시 필요: spark-submit --deploy-mode cluster --driver-cores 2 --driver-memory 2G ./소스경로
        self.SPARK_DRIVER_MEMORY = '##spark-driver-memory##' # spark-submit shell 배포 실행시 필요: spark-submit --deploy-mode cluster --driver-cores 2 --driver-memory 2G ./소스경로
        self.SPARK_EXECUTOR_CORE = '##spark-executor-core##'
        self.SPARK_EXECUTOR_MEMORY = '##spark-executor-memory##'
        self.SPARK_EXECUTOR_INSTANCES = '##spark-executor-instances##'
        self.SPARK_EXECUTOR_MEMORYOVERHEAD = '1g'
        self.SPARK_EXECUTOR_HEARTBEATINTERVAL = '10s'
        self.SPARK_SQL_SHUFFLE_PARTITIONS = '##spark-sql_shuffle_partitions##'
        self.SPARK_SQL_AUTOBROADCASTJOINTRESHOLD = '100m'
        self.SPARK_SQL_BROADCASTTIMEOUT = '3600' # 06분
        self.SPARK_SERIALIZER = 'org.apache.spark.serializer.KryoSerializer'
        self.SPARK_KEYOSERIALIZER_BUFFER_MAX = '256m'
        self.SPARK_NETWORK_TIMEOUT = '60s'
        

    def main(self):
        '''
        공통작업 내용
        '''
        self._main()
    
    
    def _main(self):
        '''
        본 클래스 상속받은 자식 클래스에서 재 정의할 것
        '''
        pass
    
    
    def write_log(self, log_tpye, msg, epoch_id=None):
        self.logger.write_log_epoch(log_tpye, msg, epoch_id)
        
    
    def get_spark_session(self, SparkSession: SparkSession):
        return SparkSession \
            .builder \
            .appName(self.app_name) \
            .config('spark.driver.cores', self.SPARK_DRIVER_CORES) \
            .config('spark.driver.memory', self.SPARK_DRIVER_MEMORY) \
            .config('spark.executor.cores', self.SPARK_EXECUTOR_CORE) \
            .config('spark.executor.memory', self.SPARK_EXECUTOR_MEMORY) \
            .config('spark.executor.instances', self.SPARK_EXECUTOR_INSTANCES) \
            .config('spark.executor.memoryOverhead', self.SPARK_EXECUTOR_MEMORYOVERHEAD) \
            .config('spark.executor.heartBeatInterval', self.SPARK_EXECUTOR_HEARTBEATINTERVAL) \
            .config('spark.sql.shuffle.partitions', self.SPARK_SQL_SHUFFLE_PARTITIONS) \
            .config('spark.sql.autoBroadcastJoinTreshold', self.SPARK_SQL_AUTOBROADCASTJOINTRESHOLD) \
            .config('spark.sql.broadcastTimeout', self.SPARK_SQL_BROADCASTTIMEOUT) \
            .config('spark.sql.broadcastTimeout', self.SPARK_SQL_BROADCASTTIMEOUT) \
            .config('spark.serializer', self.SPARK_SERIALIZER) \
            .config('spark.keyoserializer.buffer.max', self.SPARK_KEYOSERIALIZER_BUFFER_MAX) \
            .config('spark.network.timeout', self.NETWORK_TIMEOUT) \
            .getOrCreate()
            
            ##.config('spark.driver.cores', self.SPARK_DRIVER_CORES)   --> pyspark에서 설정불가, spark-submit 할 때 --driver-cores 옵션으로 지정할 것
            ##.config('spark.driver.memory', self.SPARK_DRIVER_MEMORY) --> pyspark에서 설정불가, spark-submit 할 때 --driver-memory 옵션으로 지정할 것
    
    
    def edwdbs_execute_query(self, SparkSession: SparkSession, query):
        return_df = SparkSession.read \
                    .format('jdbc') \
                    .options(**ORACLE_EDWDB_INFO) \
                    .options('query', query) \
                    .options('fetchsize', '100000') \
                    .load()
    
        return return_df
    
    
    def bizdbs_execute_query(self, SparkSession: SparkSession, query):
        return_df = SparkSession.read \
                    .format('jdbc') \
                    .options(**ORACLE_BIZDB_INFO) \
                    .options('query', query) \
                    .options('fetchsize', '100000') \
                    .load()
    
        return return_df
    
    
    def get_redis_client(self, cluster='outbound'):
        return RedisSender(cluster=cluster)
    
    
    def merge_initial_by_streaming(self, SparkSession: SparkSession, streaming_df, init_tbl_df, ptt_cnt, pk_col_lst, **kwargs):
        first_start = kwargs.get('first_start')
        storage_level = kwargs.get('storage_level')
        dataset_nm = kwargs.get('dataset_nm')
        
        return self.init_integrator.merge_initial_by_streaming(SparkSession, streaming_df, init_tbl_df, ptt_cnt, pk_col_lst, first_start, storage_level, self.app_name, dataset_nm)
    
    
    def checkpoint_dataframe(self, df: DataFrame, dataset_nm: str, spark: SparkSession):
        bf_ckpt_rdd_id = df.rdd.id()
        save_df = df.checkpoint()
        af_ckpt_rdd_id = save_df.rdd.id()
        
        if dataset_nm is not None:
            ckpt_df = spark.createDataFrame(data=([bf_ckpt_rdd_id, af_ckpt_rdd_id],), schema='BEFORE_RDD_ID LONG, AFTER_RDD_ID LONG')
            ckpt_df.write \
                   .format('csv') \
                   .mode('overwrite') \
                   .option('header', False) \
                   .save(f'/home/spark/last_rdd_info/{self.app_name}/{dataset_nm}')
            
            print(f'last_rdd_info 저장완료 => 데이터 프레임: {dataset_nm}, 시작범위:{bf_ckpt_rdd_id}, 끝범위:{af_ckpt_rdd_id}')
         
        return save_df