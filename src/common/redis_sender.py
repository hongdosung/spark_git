import redis
import time
from redis.sentinel import Sentinel
from rediscluter import RedisCluster
from rediscluster.exceptions import ClusterError, ClusterDownError
from src.commom.null_transformer import NullTransformer
from pyspark.sql.functions import col, from_unixtime, to_timestamp
from pyspark.sql.types import DecimalType, TimestampType, LongType
from pyspark.dataframe import DataFrame
from pyspark.sql import SparkSession

def reconnect_on_failure(func):
    def wrapper(self, *args, **kwargs):
        retry_cnt = 0
        while True:
            try:
                return func(self, *args, **kwargs)
            except (ClusterError, ClusterDownError):
                retry_cnt += 1
                print(f'######## Redis Cluster connection error occurred. Reconnection retry_cnt: {retry_cnt} ########')
                time.sleep(1)
                self._get_redis_conn()
                if retry_cnt >= 11:
                    raise Exception('Redis Cluster connection retry count({retry_cnt}) has been exceeded. Program exit')
                else:
                    continue

    return wrapper


class RedisSender():
    def __init__(self, cluster='outbound'):
        self.cluster = cluster # internal / outbound
        self.profile = '##stage##'
        self._get_redis_conn()


    def _get_redis_conn(self):
        if self.profile == 'PROD': # 운영서버
            if self.cluster == 'outbound': # 외부용
                self.redis_client = RedisCluster(
                    startup_nodes = [
                        {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'7379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'7379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'7379'}
                    ],
                    decode_responses = True
                )
            elif self.cluster == 'internal': # 내부용
                self.redis_client = RedisCluster(
                    startup_nodes = [
                        {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'7379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'7379'},
                        {'host':'xxx.xxx.xxx.xxx', 'port':'7379'}
                    ],
                    decode_responses = True
                )
        else: # 개발서버
            self.redis_client = RedisCluster(
                startup_nodes = [
                    {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                    {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                    {'host':'xxx.xxx.xxx.xxx', 'port':'6379'},
                    {'host':'xxx.xxx.xxx.xxx', 'port':'7379'},
                    {'host':'xxx.xxx.xxx.xxx', 'port':'7379'},
                    {'host':'xxx.xxx.xxx.xxx', 'port':'7379'}
                ],
                decode_responses = True
            )


    @reconnect_on_failure
    def send_hset(self, df: DataFrame, tbl_nm, pk_col_lst, value_to_field_lst, value_col_lst, ignore_null=None):
        '''
        pk_col_lst: redis의 key가 될 컬럼(필수)
        value_to_field_lst: dataFrame의 값으로 Redis hset의 Field로 올려야 할 경우
        value_col_lst: redis hset에서 기본 field 및 value로 지정한 컬럼, 각 컬럼은 1건씩 전송(필수)
        '''

        row_lst = df.collect()
        for row in row_lst:
            pk_val_lst = [tbl_nm] # 첫 요소로 테이블명 고정

            for pk_col in pk_col_lst:
                pk_val_lst.append(str(row[pk_col])) # 컬럼 값을 입력

            pk_str = ':'.join(pk_val_lst)
            mapping_dict = {}
            field_str = []
            if value_to_field_lst:
                for v2f_col in value_to_field_lst:
                    field_str += str(row[v2f_col]) + ':'

            for value_col in value_col_lst:
                value = row[value_col]

                if ignore_null:
                    if value is None:
                        continue
                    else:
                        value = NullTransformer.set_default_to_null_return_value(df.schema.json(), value_col, value)

                value_append_field_str = field_str
                value_append_field_str += value_col
                value_str = str(value)

                mapping_dict[value_append_field_str] = value_str

            self.redis_client.hset(pk_str, mapping=mapping_dict)


    @reconnect_on_failure
    def send_hset_by_json(self, json_payload, tbl_nm, pk_col_lst, value_col_lst):
        '''
        null_transformer.set_null_to_default 함수가 적용되지 않은 수수 json 구저의 kafka palyload를 받아 전송하는 함수
        '''

        pk_val_lst = [tbl_nm] # 첫 요소로 테이블명 고정
        for pk_col in pk_col_lst:
            pk_val_lst.append(str(json_payload[pk_col]))

        pk_str = ':'.join(pk_val_lst)
        mapping_dict = {}
        value_append_field_str = ''

        for value_col in value_col_lst:
            try:
                value = json_payload[value_col] # json payload 구조이므로 update의 경우 컬럼값이 없을 수도 있음, 이 경우 continue로 지정
            except:
                continue
            else:
                mapping_dict[value_col] = str(value)

        if len(mapping_dict) == 0: # 처리할 데이터가 없는 경우
            return

        self.redis_client.hset(pk_str, mapping=mapping_dict)


    @reconnect_on_failure
    def send_hset_by_json_chk_key(self, json_payload, tbl_nm, pseudo_key_lst, org_key_lst, value_col_lst):
        '''
        null_transformer.set_null_to_default 함수가 적용되지 않은 수수 json 구저의 kafka palyload를 받아 전송하는 함수
        원래의 PK가 아닌 일반컬럼을 PK로 지정하여 Redis에 동기화하는 경우 사용되는 함수

        - pseudo_key_lst: Redis PK로 지정하고자 하는 일반컬럼(ex: A_COL)
        - org_key_lst: 원래 Pk 컬럼 (ex: PK_01, PK_02, PK_03)
        - value_col_lst: 일반컬럼, Redis filed명 끝으로 지정됨(ex: ETC_COL)

        Redis 저장시 아래와 같이 저장
        ---------------------------------------------------------------------------------------------
            key                    field               field         value                field
        ---------------------------------------------------------------------------------------------
         tbl_nm:A_COL:ccccc     aaaa:bbbb:dddd:ETC_COL    60    aaaa:bbbb:gggg:ETC_COL       30
        ---------------------------------------------------------------------------------------------

        aaaa: PK_01
        bbbb: PK_02
        dddd, gggg: PK_03
        '''

        pk_val_lst = [tbl_nm] + pseudo_key_lst # 첫 요소로 고정
        for pk_col in pseudo_key_lst:
            pk_val_lst.append(str(json_payload.get(pk_col))) # pk_col 값이 없는 경우 string 'None'으로 저장

        pk_str = ':'.join(pk_val_lst)
        mapping_dict = {}
        #field_str = ''
        org_key_lst = [col for col in org_key_lst if col not in pseudo_key_lst] # pseudo_key_list에 있는 컬럼 제외
        for value_col in value_col_lst:
            try:
                value = json_payload[value_col] # json payload 구조이므로 update의 경우 컬럼값이 없을 수도 있음, 이 경우 continue로 지정
            except:
                continue
            else:
                mapping_dict[value_col] = str(value)

        if len(mapping_dict) == 0: # 처리할 데이터가 없는 경우
            return

        self.redis_client.hset(pk_str, mapping=mapping_dict)


    @reconnect_on_failure
    def send_hdel(self, df: DataFrame, tbl_nm, pk_col_lst, value_to_field_lst, value_col_lst):
        '''
        pk_col_lst: redis의 key가 될 컬럼(필수)
        value_to_field_lst: dataFrame의 값으로 Redis hdel의 Field로 올려야 할 경우
        value_col_lst: redis hdel에서 기본 field 및 value로 지정한 컬럼, 각 컬럼은 1건씩 전송(필수)
        '''

        row_lst = df.collect()
        for row in row_lst:
            pk_val_lst = [tbl_nm] # 첫 요소로 테이블명 고정

            for pk_col in pk_col_lst:
                pk_val_lst.append(str(row[pk_col])) # 컬럼 값을 입력

            pk_str = ':'.join(pk_val_lst)
            field_str = ''
            if value_to_field_lst:
                for v2f_col in value_to_field_lst:
                    field_str += str(row[v2f_col]) + ':'

            for value_col in value_col_lst:
                value_append_field_str = field_str
                value_append_field_str += value_col

                #print('hdel', pk_str, value_append_field_str)
                self.redis_client.hdel(pk_str, value_append_field_str)


    @reconnect_on_failure
    def send_hdel_by_json(self, json_payload, tbl_nm, pk_col_lst, value_to_field_lst, value_col_lst):
        pk_val_lst = [tbl_nm] # 첫 요소로 테이블명 고정
        for pk_col in pk_col_lst:
            pk_val_lst.append(str(json_payload.get(pk_col)))

        pk_str = ':'.join(pk_val_lst)
        v2f_str = ''
        if value_to_field_lst:
            for v2f_col in value_to_field_lst:
                v2f_str += str(json_payload.get(v2f_col)) + ':'

        for value_col in value_col_lst:
            self.redis_client.hdel(pk_str, v2f_str + value_col)


    @reconnect_on_failure
    def send_del(self, df: DataFrame, tbl_nm, pk_col_lst):
        # key 자체로 삭제 할 경우
        row_lst = df.collect()
        for row in row_lst:
            pk_val_lst = [tbl_nm] # 첫 요소로 테이블명 고정

            for pk_col in pk_col_lst:
                pk_val_lst.append(str(row[pk_col])) # 컬럼 값을 입력

            pk_str = ':'.join(pk_val_lst)
            self.redis_client.delete(pk_str)


    @reconnect_on_failure
    def send_del_by_json(self, json_payload, tbl_nm, pk_key_lst):
        # key 자체로 삭제 할 경우
        pk_val_lst = [tbl_nm] # 첫 요소로 테이블명 고정

        for row in pk_key_lst:
            pk_val_lst.append(str(json_payload.get(pk_col)))

        pk_str = ':'.join(pk_val_lst)
        self.redis_client.delete(pk_str)


    @reconnect_on_failure
    def send_hget(self, df: DataFrame, redis_tbl_nm, pk_col_lst, schema, spark: SparkSession, chk_offset: bool=True):
        '''
        데이터프레임에서 pk_col_lst에 해당하는 값에 대해서 redis 조회 후  데이터프레임에 구성하여 return하는 함수
        - df: streaming에서 인입된 데이터 프레임, redis를 검색하기 위한 값을 가지고 있는 데이터프레
        - redis_tbl_nm: redis에서 검색할 테이블명
        - pk_col_lst: redis_tbl_nm의 PK 컬럼, redis 조회시 redis_tbl_nm:pk01값:pk02값 으로 조회
        - schema: redis 조회 시 전체 field를 가져오므로  필요한 컬럼만 리턴할 수 있도록 schema 명시
                  redis 조회 후 dataframe으로 변환 시 우선 string으로 생성한 후 return_df를 타입에 맞게 가옹
        '''

        if chk_offset:
            df.createOrReplaceGlobalTempView('df')
            dedup_df = spark.sql(f'''
                SELECT *
                  FROM (
                        SELECT ROW_NUMBER() OVER(PARTITION BY {','.join(pk_col_lst)} ORDER BY KFK_OFFSET DESC) AS NUM
                             , A.*
                          FROM global_temp.df A
                       )
                 WHERE NUM = 1
            '''
            )
        else:
            dedup_df = df.select(pk_col_lst).distinct()


        if isinstance(schema, str):
            str_schema = schema
            decimal_cols = {}
            ts_cols = {}
        else:
            str_schema = ' STRING,'.join(col.name for col in schema) + ' STRING'
            decimal_cols = {col.name: col.dataType for col in schema if isinstance(col.dataType, DecimalType)}
            ts_cols = {col.name: col.dataType for col in schema if isinstance(col.dataType, TimestampType)}

        pk_val_lst = ''
        rslt_lst = []

        for row in dedup_df.collect():
            pk_val_lst = [redis_tbl_nm] # 첫 요소로 테이블명 고정

            for pk_col in pk_col_lst:
                pk_val_lst.append(str(row[pk_col])) # 컬럼 값을 입력

            pk_str = ':'.join(pk_val_lst)
            retry_cnt = 1200

            if chk_offset:
                try:
                    op_type = row['__OP_TYPE']
                except ValueError:
                    raise Exception("Dataframe passed send_hget doesn't has '__OP_TYPE' field, Please include '__OP_TYPE' field in dataframe (Table: {redis_tbl_nm})")
            else:
                op_type = None # chk_offset 하지 않는 프로그램은 None으로 간주

            while True:
                return_dict = self.redis_client.hgetall(pk_str)
                if op_type == 'I':
                    if not return_dict and retry_cnt > 0:
                        retry_cnt = retry_cnt - 1
                        print(f'Insert TX: Redis send_hget returns empty data, remain retry count: {retry_cnt}, key: {pk_str}')
                        time.sleep(0.5)
                        continue
                    elif not return_dict and retry_cnt == 0:
                        print(f'Insert TX: Redis send_hget returns empty data even after {retry_cnt} retries (key: {pk_str})')
                        print(f'key({pk_str}) may be deleted, Empty dataframe will be returned')
                        return_dict = {}
                        break
                        # raise Exception(f'Redis send_hget returns empty data, even after {retry_cnt} retries (key: {pk_str})')
                    else:
                        return_dict = {k: v for k, v in return_dict.items() if v != 'None'} # None으로 빠진 컬럼은 creatDataFrame 수행시 null로 잡힘
                        break

                elif op_type == 'U':
                    # 만약 Update 트랜잭션 임에도 Redis 조회하여 빈 딕셔너리가 온다면 Update 직후 Delete 되었음을 의미, 이 경우 빈 데이터프레임 return
                    if len(return_dict) == 0 and retry_cnt > 0:
                        retry_cnt = retry_cnt - 1
                        print(f'Update TX: Redis send_hget returns empty data, remain retry count: {retry_cnt}, key: {pk_str}')
                        time.sleep(0.5)
                        continue
                    elif len(return_dict) == 0 and retry_cnt == 0:
                        print(f'Update TX: Redis send_hget returns empty data, even after {retry_cnt} retries (key: {pk_str})')
                        print(f'key({pk_str}) may be deleted, Empty dataframe will be returned')
                        return_dict = {}
                        break

                    # Redis 조회 결과가 존재한다면 kafka OFFSET 비교
                    df_kfk_offset = int(row['KFK_OFFSET'])
                    try:
                        rds_kfk_offset = int(return_dict['KFK_OFFSET'])
                    except:
                        if retry_cnt > 0:
                            retry_cnt = retry_cnt - 1
                            print(f"Redis doesn't has 'KFK_OFFSET' field, Please include 'KFK_OFFSET' field in TO_REDIS SPARK JOB (Table: {redis_tbl_nm} KEY: {pk_str})")
                            print(f'Remain retry count: {retry_cnt}')
                            time.sleep(0.5)
                            continue
                        else:
                            print(f'Remain retry count: {retry_cnt}')
                            raise Exception(f"Redis doesn't has 'KFK_OFFSET' field, Please include 'KFK_OFFSET' field in TO_REDIS SPARK JOB (Table: {redis_tbl_nm} KEY: {pk_str})")

                    if df_kfk_offset <= rds_kfk_offset: # 정상조회
                        return_dict = {k: v for k, v in return_dict.items() if v != 'None'} # None으로 빠진 컬럼은 creatDataFrame 수행시 null로 잡힘
                        break
                    elif df_kfk_offset > rds_kfk_offset and retry_cnt > 0: # 데이터프레임의 offset이 크며 update TX 조회이므로 대기 후 재시도(Redis 적재가 늦어지는 경우)
                        retry_cnt = retry_cnt - 1
                        print(f'Redis(TABLE: {redis_tbl_nm}, KEY: {pk_str}) has offset: {rds_kfk_offset}. This is lower than Dataframe offset({df_kfk_offset}), retry occurred (remain retry count: {retry_cnt})')
                        time.sleep(0.5)
                        continue
                    elif df_kfk_offset > rds_kfk_offset and retry_cnt == 0:
                        raise Exception(f"Redis(TABLE: {redis_tbl_nm}, KEY: {pk_str}) offset({rds_kfk_offset}) is lower than Dataframe offset({df_kfk_offset}) even after retry count: {retry_cnt}")
                elif op_type is None:
                    return_dict = {k: v for k, v in return_dict.items() if v != 'None'} # None으로 빠진 컬럼은 creatDataFrame 수행시 null로 잡힘
                    break

            if len(return_dict) == 0: # 조회 결과가 없으면 continue 처리
                continue

            for pk_col in pk_col_lst:
                return_dict[pk_col] = str(row[pk_col])

            rslt_lst.append(return_dict)

        return_df = spark.createDataFrame(rslt_lst, str_schema)

        # send_hget 시작에서 증복제거를 하므로 데이터셋은 중복제거 없이 바로 리터하도록 수정
        # return_df = reurn_df.dropDuplicates(pk_col_lst)

        if decimal_cols: # decimal 컬럼 변환
            for col_nm, db_type in decimal_cols.items():
                return_df = return_df.withColumn(col_nm, col(col_nm).cast(db_type))

        if ts_cols: # timestamp 컬럼 변환
            for col_nm, db_type in ts_cols.items():
                return_df = return_df.withColumn(col_nm, to_timestamp(from_unixtime(col(col_nm).cast(LongType())/1000)))

        return return_df


    @reconnect_on_failure
    def send_hget_by_chg_key(self, df: DataFrame, redis_tbl_nm, pseudo_key_lst, org_key_lst, schema, spark: SparkSession):
        '''
        원래의 PK가 아닌 컬럼으로 Redis 동기화한 hset 데이터를 조회할 경우 사용하는 함수
        - df: streaming에서 인입된 데이터 프레임, redis를 검색하기 위한 값을 가지고 있는 데이터프레임
        - redis_tbl_nm: redis에서 검색할 테이블명
        - pseudo_key_lst: redis 키로 지정한 일반 컬럼 리스트, 이 값을 이용하여 redis 조회
        - org_col_lst: redis_tbl_nm의 PK 컬럼
        - schema: redis 조회 시 전체 field를 가져오므로  필요한 컬럼만 리턴할 수 있도록 schema 명시
                  redis 조회 후 dataframe으로 변환 시 우선 string으로 생성한 후 return_df를 타입에 맞게 적용
        '''

        if isinstance(schema, str):
            str_schema = schema
            decimal_cols = {}
            ts_cols = {}
        else:
            str_schema = ' STRING,'.join(col.name for col in schema) + ' STRING'
            decimal_cols = {col.name: col.dataType for col in schema if isinstance(col.dataType, DecimalType)}
            ts_cols = {col.name: col.dataType for col in schema if isinstance(col.dataType, TimestampType)}

        pk_val_lst = ''
        field_org_key_lst = [col for col in org_key_lst if col not in pseudo_key_lst] # pseudo_key_list에 있는 컬럼 제외
        rslt_lst = []
        for row in df.collect():
            pk_val_lst = [redis_tbl_nm] + pseudo_key_lst # 첫 요소로 테이블명 고정

            for pk_col in pseudo_key_lst:
                pk_val_lst.append(str(row[pk_col])) # 컬럼 값을 입력

            pk_str = ':'.join(pk_val_lst)
            rslt_dict = self.redis_client.hgetall(pk_str)
            rslt_dict = {k: v for k, v in rslt_dict.items() if v != 'None'} # None으로 빠진 컬럼은 creatDataFrame 수행시 null로 잡힘

            if len(rslt_dict) == 0: # 조회 결과가 없으면 continue 처리
                continue

            key_set = []
            divided_key_rslt_lst = []
            for k, v in rslt_dict.items():
                key_lst = k.split(':')[:-1] # 맨 우측은 컬럼명은 제외하고 org_key의 값으로만 이루어진 값을 list로 변환
                key_str = ':'.join(key_lst) # org_key 값만으로 이루어진 string (구분자 :)

                if key_str not in key_set:
                    key_set.append(key_str)
                    idx = key_set.index(key_str)

                    if idx >= len(divided_key_rslt_lst):
                        divided_key_rslt_lst.append(dict(zip(field_org_key_lst, key_lst)))
                        k = k.replace(f'{key_str}:', '')
                        divided_key_rslt_lst[idx][k] = v
                else:
                    idx = key_set.index(key_str)
                    k = k.replace(f'{key_str}:', '')
                    divided_key_rslt_lst[idx][k] = v

            # 전처리 완료된 딕셔너리에 key 값도 추가
            for i in range(len(divided_key_rslt_lst)):
                for pk_col in pseudo_key_lst:
                    divided_key_rslt_lst[i][pk_col] = str(row[pk_col])

            # 결과정리
            rslt_lst += divided_key_rslt_lst

        return_df = spark.createDataFrame(rslt_lst, str_schema)

        # send_hget 시작에서 증복제거를 하므로 데이터셋은 중복제거 없이 바로 리터하도록 수정
        # return_df = reurn_df.dropDuplicates(pk_col_lst)

        if decimal_cols: # decimal 컬럼 변환
            for col_nm, db_type in decimal_cols.items():
                return_df = return_df.withColumn(col_nm, col(col_nm).cast(db_type))

        if ts_cols: # timestamp 컬럼 변환
            for col_nm, db_type in ts_cols.items():
                return_df = return_df.withColumn(col_nm, to_timestamp(from_unixtime(col(col_nm).cast(LongType())/1000)))

        return return_df


    @reconnect_on_failure
    def send_hget_by_json(self, json_payload, redis_tbl_nm, pk_col_lst):
        '''
        json_payload 단건으로 redis 조회 후 dictionary 형태로 리턴하는 함수
        '''

        pk_val_lst = [redis_tbl_nm] # 첫 요소로 테이블명 고정
        for pk_col in pk_col_lst:
            pk_val_lst.append(str(json_payload.get(pk_col)))

        pk_str = ':'.join(pk_val_lst)
        return_df = self.redis_client.hgetall(pk_str)

        return return_df