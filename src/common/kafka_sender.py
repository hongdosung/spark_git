import json
from pyspark.sql.functions import udf, lit, from_json, to_json, struct
from pyspark.sql.types import LongType, IntegerType, StringType
from pyspark.sql.dataframe import DataFrame
from datetime import date


class KafkaSender():
    def __init__(self, write_topic_nm):
        self.bootstrap = '##bootstrap-servers##'
        self.write_topic_nm = write_topic_nm
        
    
    def set_wrtie_topic_nm(sekf, write_topic_nm):
        self.write_topic_nm = write_topic_nm
        
    
    def send_kafka_schema_payload(self, df: DataFrame, pk_col_lst: list, value_col_lst: list):
        '''
        write_topic_nm: Spark --> Kafka로 저장되는 토픽명
        pk_col_lst: Kafka Key 메시지 대상 리스트
        value_col_lst: Kafka Value 메시지 대상 리스트
        
        Key만 전송하는 경우(Delete 레코드) value_col_lst=None 로 입력
        Value만 전송하는 경우 pk_col_lst=None 로 입력
        '''
        
        def convert_ts_to_int(ts):
            if ts is not None:
                return_int = int(ts.timestamp()) * 1000 # ms 단위 타입스탬프 to int64
                
                return return_int
            else:
                return None
            
       
        def convert_date_to_int(dt):
            '''
            Kafka Produce 전 Date -> Unix Epoch Time 기준 경과 일수로 변환하기 위한 함수
            '''
            if dt is not None:
                print(dt)
                return (df - date(1970, 1, 1)).days
            else:
                return None
            
        
        def set_kafka_msg(part_df: DataFrame, col_lst: list, msg_type: str):
            '''
            msg_type: Key or Value
            '''
            
            org_col_lst = part_df.columns
            schema_json = json.loads(part_df.select(*col_lst).schema.json())
            # schema_json: {"fields":[{"metadata":{},"name":"컬럼명","nullable":true,"type":"string"},{"metadata":{},"name":"컬럼명","nullable":true,"type":"long"},{"metadata":{},"name":"__OP","nullable":true,"type":"string"}],"type":"struct"}
            
            new_schema_lst = []
            tot_schema = []            
            timestamp_col_lst = [] # timestamp를 int64로 변경하기 위해 컬럼 목록을 보관
            data_col_lst = []     # data를 int32로 변경하기 위해 컬럼 목록을 보관
            for row_dict in schema_json['fields']:
                new_schema_dict = {}
                for k, v in row_dict.items():
                    if k == 'metadata':
                        continue
                    elif k == 'nullable':
                        new_k = 'optional'
                    elif k == 'name':
                        new_k = 'field'
                    else:
                        new_k = k
                    
                    if k == 'type' and v in ['integer', 'long']:
                        v = 'int64'
                    elif k == 'type' and v in ['date']:
                        v = 'int32'
                        new_schema_dict['name'] = 'org.apache.kafka.connect.data.Date'
                        new_schema_dict['version'] = 1
                        data_col_lst.append(row_dict['name'])
                    elif k == 'type' and v.startswith('decimal'):
                        scale_precision = v.replace('decimal(', '').replace(')', '')
                        precision = scale_precision.split(',')[0]
                        scale = scale_precision.split(',')[1]
                        v = 'bytes' # decimal 타입은 bytes로 변환
                        
                        new_schema_dict['name']  = 'org.apache.kafka.connect.data.Decimal'
                        new_schema_dict['version'] = 1
                        new_schema_dict['parameters'] = {'scale': f'{scale}', 'connect.decimal.precision': f'{precision}'}
                    elif k == 'type' and v == 'timestamp':
                        v = 'int64'
                        new_schema_dict['name']  = 'org.apache.kafka.connect.data.Timestamp'
                        new_schema_dict['version'] = 1
                        timestamp_col_lst.append(row_dict['name']) # timestamp 타입의 컬럼명을 리스트에 추가
                    
                    new_schema_dict[new_k] = v
                
                new_schema_lst.append(new_schema_dict)
            
            tot_schema['fields'] = new_schema_lst
            tot_schema['type'] = 'struct'
            tot_schema['optional'] = False
            tot_schema['name'] = f'{self.write_topic_nm}.{msg_type}'
            tot_schema_str = json.dumps(tot_schema)
            
            convert_ts_to_int_UDF = udf(convert_ts_to_int, LongType())
            for ts_col in timestamp_col_lst:
                part_df = part_df.withColumn(ts_col, convert_ts_to_int_UDF(ts_col))
                
            convert_date_to_int_UDF = udf(convert_date_to_int, IntegerType())
            for dt_col in timestamp_col_lst:
                part_df = part_df.withColumn(dt_col, convert_date_to_int_UDF(dt_col))
            
            trans_df = part_df.withColumn('schema_str', lit(tot_schema_str)) \
                              .withColumn('schema', from_json('schema_str', 'struct<name:string, optional:boolean, type:string, fields:array<struct<field:string, optional:boolean, type:string, name:string, version:integer>>>')) \
                              .withColumn('payload', struct(*col_lst)) \
                              .withColumn(msg_type.lower(), to_json(struct('schema', 'payload'), options={'ignoreNullFields': False})) \
                              .select(*org_col_lst, msg_type.lower())
            
            return trans_df
        
        if pk_col_lst:
            return_df = set_kafka_msg(part_df=df, col_lst=pk_col_lst, msg_type='Key')
            if value_col_lst:
                return_df = set_kafka_msg(part_df=return_df, col_lst=value_col_lst, msg_type='Value').select('key', 'value')
            
            else: # No Value(Delete) Case
                return_df = return_df \
                            .withColumn('value', lit(None).cast(StringType())) \
                            .select('key', 'value')
        else: # No Key Case
            return_df = set_kafka_msg(part_df=df, col_lst=value_col_lst, msg_type='Value').select('value')
        
        
        # return_df.show(truncate=False)
        return_df.write \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.bootstrap_server) \
            .option('topic', self.write_topic_nm) \
            .save()
        
        
    def send_kafka_payload(self, df: DataFrame, value_col_lst: list, ignorenulls: bool, compression_type=None):
        def set_kafka_msg(part_dt: DataFrame, col_lst: list):
            trans_df = part_dt.select(to_json(struct(*col_lst), options={'ignoreNullFields': ignorenulls}).alias('value'))
            
            return trans_df
        
        return_df = set_kafka_msg(part_dt=df, col_lst=value_col_lst).select('value')
        # return_df.show(truncate=False)
        
        kafka_send_df = return_df \
            .write \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.bootstrap_server) \
            .option('topic', self.write_topic_nm) \
 
        if compression_type is not None:
            kafka_send_df = kafka_send_df.option('kafka.compression.type', compression_type)
        
        kafka_send_df.save()