import re
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.dataframe import DataFrame

class NullTransformer():
    '''
    stream 데이터프레임 get_json_object 함수로 처리 전 레코드 값이 null인 컬럼이 있다면 
    해당 컬럼을 명시적으로 null update가 발생한 트랜젼션이 경우
    get_json_object 함수 처리 후에 컬럼이 null인지, 원래 null 업데이트인지 구분이 되지 않으므로 
    get_json_object 함수 처리전에 null 값은 다른 값으로 치환하는 함수
    '''
    numericDftVal = -999999999
    timestampDftVal = 300000000000
    stringDftVal = '__UPDATE_NULL__'
    dateDftVal = 3333
        
    def __init__(self):
        pass
            
    
    @classmethod
    def set_default_to_null_return_value(cls, schema_json, col, value):
        schema_dict = json.loads(schema_json)
        for row_dict in schema_dict['fields']:
            col_type = row_dict['type']
            col_nm = row_dict['name']
            
            if col_nm == col:
                if (col_type in ['integer', 'long', 'double'] or col_type.startswith('decimal')) and value == NullTransformer.numericDftVal:
                    return None
                elif col_type == 'string' and value == NullTransformer.stringDftVal:
                    return None
                elif col_type == 'data' and value == NullTransformer.dateDftVal:
                    return None
                return value
        raise Exception(f'null_transformer: set_default_to_null_return_value에서 매칭되는 column이 없습니다.')
    
    
    @classmethod
    def set_default_to_null(cls, df: DataFrame, val_col_lst):
        df_schema = json.loads(df.select(*val_col_lst).schema.json())
        for row_dict in df_schema['fields']:
            col_type = row_dict['type']
            col_nm = row_dict['name']
            
            if col_type in ['integer', 'long', 'double'] or col_type.startswith('decimal'):
                df = df.replace(to_replace={NullTransformer.numericDftVal: None}, subset=col_nm)                
            elif col_type == 'string':
                df = df.replace(to_replace={NullTransformer.stringDftVal: None}, subset=col_nm)
            elif col_type == 'timestamp':
                df = df.replace(to_replace={NullTransformer.timestampDftVal: None}, subset=col_nm)
            elif col_type == 'data':
                df = df.replace(to_replace={NullTransformer.dateDftVal: None}, subset=col_nm)
        
        return df
    
    
    @classmethod
    def get_type_from_column(cls, col_nm, schemas):
        for struct_field_schema in schemas:
            if struct_field_schema.name == col_nm:
                return str(struct_field_schema.dataType)


    def get_default_value_by_type(self, col_type):
        if str(col_type) in ['IntegerType', 'LongType'] or str(col_type).startswith('DecimalType'):
            return NullTransformer.numericDftVal            
        elif str(col_type) == 'StringType':
            return NullTransformer.stringDftVal
        elif str(col_type) == 'TimestampType':
            return NullTransformer.timestampDftVal
        elif str(col_type) == 'DataType':
            return NullTransformer.dateDftVal
        else:
            raise Exception(f"Can't lookup dataType for column type({col_type}) at null_transformer.py")


    def change_to_default(self, value, schemas):
        op = re.compile('"__OP_TYPE":"([IUD]))')
        m = op.findall(value)
        if m[0] == 'U': # __OP_TPYE이 'U'인 경우만
            p = re.compile('"([A-Za-z0-9_]":null)') # null 값인 컬럼인 경우
            m = p.findall(value)
            if m:
                for tgt_col in m:
                    if tgt_col not in [x.name for x in schemas]: # load한 테이블에 포함되지 않은 컬럼은 불필요하므로 continue 처리
                        continue
                    
                    col_type = NullTransformer.get_type_from_column(tgt_col, schemas)
                    default_value = self.get_default_value_by_type(col_type)
                    value = value.replace(f'"{tgt_col}":null', f'"{tgt_col}":"{default_value}"')
        
        return value
    
    
    def set_null_to_default(self, df: DataFrame, schemas):
        default_value_udf = udf(lambda x: self.change_to_default(x, schemas), StringType())
        
        return df.withColumn('VALUE', default_value_udf(df.VALUE))
    
    
    def convert_value_null_to_op_type(self, steam_df: DataFrame):
        str_op_type_d = '{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"field":"__OP_TYPE}],"__OP_TYPE":"D"}}'
        stream_df = steam_df.fillna({'VALUE':str_op_type_d})
        
        return stream_df