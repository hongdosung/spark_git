from src.db_sync.db_sync_bass_class import DBSyncBaseClass
from pyspark.sql import SparkSession

class TB_TO_REDIS(DBSyncBaseClass):
    def __init__(self, app_name):
        super().__init__(app_name)
        
        ### HADOOP 경로설정        
        self.chk_hdfs_dir = self.base_chk_hdfs_dir + '/' + self.app_name
        self.init_hdfs_dir = self.base_init_hdfs_dir + '/' + self.app_name
        
        #### 테이블 변수
        self.TBL_NM = 'TB_DB'
        self.PK_COL_LST = ['PK_A,','PK_B']
        self.VALUE_COL_LST = ['COL_A,','COL_B','COL_C']
        
        #### KFK 변수
        self.KFAKA_SOURCE_TOPICS = ['##topic-prefix-oggbd-스키마명.테이블명##']
        self.MAX_OFFSETS_PER_TRIGGER = '10000'
        
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
        spark = self.get_spark_session(SparkSession)
        sc = spark.sparkContext
        #sc.setCheckpointDir(self.init_hdfs_dir)
        self.logger.logger.setLevel(self.log_type)
    
        # Start Streaming
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
                         .foreachBatch(lambda df, epoch_id: self.foreach_batch_process(df, epoch_id, self.PK_COL_LST, self.VALUE_COL_LST, spark)) \
                         .option('checkpointLocation', self.chk_hdfs_dir) \
                         .start()
        
        kafka_input_df.awaitTermination()

        
if __name__ == '__main__':
    tb_to_redis = TB_TO_REDIS(app_name='TB_TO_REDIS')
    tb_to_redis.main()
    