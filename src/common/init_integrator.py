from datetime import datetime, date, time
from pyspark.sql.functions import col, get_json_object, trim, lit, broadcast
from src.common.null_transformer import NullTransformer
from src.common.redis_sender import RedisSender
from pyspark.storagelevel import StorageLevel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

class InitIntegrator():
    def __init__(self):
        pass
    
    
    def checkpoint_dataframe(self, init_tbl_df: DataFrame, spark: SparkSession, app_name, dataset_nm):
        bf_ckpt_rdd_id = init_tbl_df.rdd.id()
        new_init_tbl_df = init_tbl_df.checkpoint()
        af_ckpt_rdd_id = new_init_tbl_df.rdd.id()
        
        if dataset_nm is not None:
            ckpt_df = spark.createDataFrame(data=([bf_ckpt_rdd_id, af_ckpt_rdd_id],), schema='BEFORE_RDD_ID LONG, AFTER_RDD_ID LONG')
            ckpt_df.write \
                   .format('csv') \
                   .mode('overwrite') \
                   .option('header', False) \
                   .save(f'/home/spark/last_rdd_info/{app_name}/{dataset_nm}')
            
            print(f'last_rdd_info 저장완료 => 데이터 프레임: {dataset_nm}, 시작범위:{bf_ckpt_rdd_id}, 끝범위:{af_ckpt_rdd_id}')
         
        return new_init_tbl_df
    
    def merge_init_by_streaming(
        self,
        sparkSession: SparkSession,
        streaming_df: DataFrame,
        init_tbl_df: DataFrame,
        ptt_cnt,
        pk_col_lst,
        first_start,
        storage_level=None,
        app_name=None,
        dataset_nm=None
    ):
        if storage_level is None:
            _storage_level = StorageLevel.MEMORY_AND_DISK
        else:
            _storage_level = storage_level
        
        
        # streaming_df가 update TX로 발생한 row 중 필요한 컬럼이 모두 null로 존재한다면 해당 row는 merge에서 제외
        to_col_lst = streaming_df.columns
        val_col_lst = [col_nm for col_nm in to_col_lst if col_nm not in pk_col_lst + ['__OP_TYPE','KFK_PTT','KFK_OFFSET']]
        updt_tgt_df = streaming_df.filter(col('__OP_TYPE') == 'U').na.drop(subset=val_col_lst, how='all')
        ins_del_tgt_df = streaming_df.filter(col('__OP_TYPE').isin('I', 'D'))
        streaming_df = updt_tgt_dfx.union(ins_del_tgt_df)
        streaming_df.persist(_storage_level)
        
        print(f'straming 건수: {streaming_df.count()}, {datetime.now()}')
        
        # init 테이블 파티션별 마지막 offset 값 확인
        # application 시작 후 첫 micro batch 시에만 MAX_KFK_OFFSET 비교 후 merge 대상을 확인
        # 두번째 micro batch 부터는 무조건 merge하면 되므로 skip
        if first_start is None or first_start:
            print('first run 대상: {first_start}')
            max_offset_per_ptt_df = init_tbl_df \
                .filter(init_tbl_df['KFK_PTT'].isNotNull()) \
                .groupBy('KFK_PTT') \
                .max('KFK_OFFSET') \
                .sort('KFK_PTT', ascending=True) \
                .withColumnRenamed('max(KFK_OFFSET)', 'MAX_KFK_OFFSET')
            max_offset_per_ptt_df.persist(_storage_level)
            max_offset_per_ptt_df.first() # action 함수 실행
            
            # apply_tgt_df: streaming_df에서 merge할 전체 대상
            apply_tgt_df = streaming_df.join(
                max_offset_per_ptt_df.hint(broadcast),
                'KFK_PTT',
                'leftouter'
            ).na.fill({'MAX_KFK_OFFSET':-1})
            
            apply_tgt_df = apply_tgt_df.filter(
                apply_tgt_df['KFK_OFFSET'] > apply_tgt_df['MAX_KFK_OFFSET'] 
            ).drop('MAX_KFK_OFFSET').select(*streaming_df.columns)
            
            if apply_tgt_df.count() == 0:
                max_offset_per_ptt_df.unpersist()
                streaming_df.unpersist()
                return init_tbl_df
            else:
                apply_tgt_df.persist(_storage_level).first()
            
        else:
            apply_tgt_df = streaming_df
            
        
        ################### 일괄 DELETE 처리 단계 ###################
        # apply_tgt_df 내 PK별 마지막 OP가 D인 PK들은 무조건 삭제
        pk_str = ','.join(pk_col_lst)
        apply_tgt_df.createOrReplaceGlobaTempView('apply_tgt_df')
        del_target_df = sparkSession.sql(f'''
            SELECT A.*
              FROM (
                    SELECT ROW_NUMBER() OVER(PARTITION BY {pk_str} ORDER BY NVL(KFK_OFFSET, -1) DESC) AS EDN_NUM
                         ,  X.*
                      FROM global_temp.apply_tgt_df X
                   )
             WHERE 1 = 1
               AND EDN_NUM = 1
               AND __OP_TYPE = 'D'
                                         
        ''').drop('EDN_NUM')
        
        del_cnt = del_target_df.count()
        
        