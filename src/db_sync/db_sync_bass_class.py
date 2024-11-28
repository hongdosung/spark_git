from src.common.base_spark_class import BaseSparkClass
import json

class DBSyncBaseClass(BaseSparkClass):
    def __init__(self, *args):
        super().__init__(*args)
        self.get_redis_client_in = 'internal'
        self.get_redis_client_out = 'outbound'
        self.redis = self.get_redis_client(cluster=self.get_redis_client_in)
    
    
    def foreach_bach_process(self. df, epoch_id, tbl_nm, pk_col_lst, value_col_lst, spark):
        self.write_log('INFO', 'foreach_bach_process 시작', epoch_id)
        df = self.null_transformer.convert_value_null_to_op_type(df)
        df.persist()
        tot_cnt = df.count()
        
        self.write_log('INFO', f'streaming 건수: {tot_cnt}', epoch_id)
        insrt_cnt = 0
        updt_cnt = 0
        del_cnt = 0
        
        for row in df.collect():
            ### key 추출
            key = row['KEY']
            json_key = json.loads(key)
            json_key_payload = json_key['payload']
            
            ### value 추출
            value = row['VALUE']
            json_value = json.loads(value)
            json_value_payload = json_value['payload']
            op_type = json_value_payload['__OP_TYPE']
            
            ### kafka 메타정보 추출 및 value에 추가
            json_value_payload['KFK_PTT'] = row['partition']
            json_value_payload['KFK_OFFSET'] = row['offset']
            
            ### 전송로직
            if op_type == 'I':
                self.redis.send_hset_by_json(
                    json_payload  = json_value_payload,
                    tbl_nm        = tbl_nm,
                    pk_col_lst    = pk_col_lst,
                    value_col_lst = value_col_lst
                )
                insrt_cnt += 1
            elif op_type == 'U':
                self.redis.send_hset_by_json(
                    json_payload  = json_value_payload,
                    tbl_nm        = tbl_nm,
                    pk_col_lst    = pk_col_lst,
                    value_col_lst = value_col_lst
                )
                updt_cnt += 1
            elif op_type == 'D':
                self.redis.send_hset_by_json(
                    json_payload  = json_key_payload,
                    tbl_nm        = tbl_nm,
                    pk_col_lst    = pk_col_lst
                )
                del_cnt += 1
        
        df.unpersist()
        self.write_log(f'Insert: {insrt_cnt} 건, Update: {updt_cnt}건, Delete: {del_cnt}건')
        self.write_log('INFO', 'foreach_bach_process 종료', epoch_id)


    def foreach_bach_process_by_chk_key(self. df, epoch_id, tbl_nm, pseudo_key_lst, org_col_lst, value_col_lst, spark, **kwargs):
        '''
        PK가 아닌 컬럼으로 REDIS 올릴 경우 사용되는 함수.
        pseudo_key_lst는 2개 이상의 pseudo_key 묶음이 올수 있음.
        이 경우 원래의 PK 컬럼의 N set, 일반 컬럼을 PK로 삼은 REDIS로 올린 1set 포함하여 N+1개 set를 REDIS에 sync 필요함
        '''
        self.write_log('INFO', 'foreach_bach_process_by_chk_key 시작', epoch_id)
        df = self.null_transformer.convert_value_null_to_op_type(df)
        df.persist()
        tot_cnt = df.count()
        
        self.write_log('INFO', f'streaming 건수: {tot_cnt}', epoch_id)
        insrt_cnt = 0
        updt_cnt = 0
        del_cnt = 0
        
        def _sync_to_redis(df, tbl_nm, pseudo_key, org_key_lst, value_col_lst):
            insrt_cnt = 0
            updt_cnt = 0
            del_cnt = 0        
            for row in df.collect():
                ### key 추출
                key = row['KEY']
                json_key = json.loads(key)
                json_key_payload = json_key['payload']
                
                ### value 추출
                value = row['VALUE']
                json_value = json.loads(value)
                json_value_payload = json_value['payload']
                op_type = json_value_payload['__OP_TYPE']
                
                ### 전송로직
                if op_type == 'I':
                    self.redis.send_hset_by_json_chk_key(
                        json_payload   = json_value_payload,
                        tbl_nm         = tbl_nm,
                        pseudo_key_lst = pseudo_key,
                        org_key_lst    = org_key_lst,
                        value_col_lst  = value_col_lst
                    )
                    insrt_cnt += 1
                elif op_type == 'U':
                    # Update 시 원래의 값을 조회한 후 TO-BE된 값을 반영하여 처리하도록 함
                    # REDIS에 변경된 PK로 넣은 컬럼이 Update될 경우 기존 값은 Delete 처리
                    # (원래 KEY 구조 REDIS에서 기존 값을 조회해 온 후 AS-IS 컬럼 값 확인하여 Delete 수행)
                    is_pk_update = False
                    for pk_col in pseudo_key:
                        if pk_col in json_value_payload.keys():
                            is_pk_update = True
                    
                    org_value = self.redis.send_hget_by_json(
                        json_payload  = json_value_payload,
                        tbl_nm        = tbl_nm,
                        pk_col_lst    = org_key_lst                        
                    )

                    if is_pk_update:
                        ### 원래 값 Delete 수행
                        del_org_value = dict(json_key_payload, **org_value)
                        _org_key_lst = [col for col in org_key_lst if col not in pseudo_key]
                        self.redis.send_hdel_by_json(
                            json_payload       = del_org_value,
                            tbl_nm             = f"{tbl_nm}:{':'.join(pseudo_key)}",
                            pk_col_lst         = pseudo_key,
                            value_to_field_lst = _org_key_lst,
                            value_col_lst      = value_col_lst
                        )
                    
                    new_value = dict(org_value, **json_value_payload)
                    self.redis.send_hget_by_chg_key(
                            json_payload    = new_value,
                            tbl_nm          = tbl_nm,
                            pseudo_key_lst  = pseudo_key,
                            org_key_lst     = org_key_lst,
                            value_col_lst   = value_col_lst
                    )
                    updt_cnt += 1
                elif op_type == 'D':
                    # PK로 인식하는 일반 컬럼의 원래 값을 찾아와 삭제하도록 함.                    
                    org_value = self.redis.send_hget_by_json(
                        redis_tbl_nm  = tbl_nm,
                        pk_col_lst    = org_key_lst                        
                    )
                    
                    org_value = dict(org_value, **json_value_payload)
                    _org_key_lst = [col for col in org_key_lst if col not in pseudo_key]
                    self.redis.send_hset_by_json(
                        json_payload       = org_value,
                        tbl_nm             = f"{tbl_nm}:{':'.join(pseudo_key)}",
                        pk_col_lst         = pseudo_key,
                        value_to_field_lst = _org_key_lst,
                        value_col_lst      = value_col_lst
                    )
                    del_cnt += 1
            
            return insrt_cnt, updt_cnt, del_cnt
        
        if isinstance(pseudo_key_lst[0], str):
            all_pseudo_col_lst = pseudo_key_lst
            i, u, d = _sync_to_redis(df, tbl_nm, pseudo_key_lst, org_key_lst, value_col_lst)
            insrt_cnt += i
            updt_cnt += u
            del_cnt += d
        else:
            all_pseudo_col_lst = []
            for pseudo_key in pseudo_key_lst:
                i, u, d = _sync_to_redis(df, tbl_nm, pseudo_key, org_key_lst, value_col_lst)
                insrt_cnt += i
                updt_cnt += u
                del_cnt += d
            all_pseudo_col_lst += pseudo_key
        
        df.unpersist()
        self.write_log('INFO', 'foreach_bach_process_by_chk_key 종료', epoch_id)
        self.write_log(f'Insert: {insrt_cnt} 건, Update: {updt_cnt}건, Delete: {del_cnt}건')
        
        # 원래 KEY 구조의 REDIS 반영
        # 원래의 PK org_key_lst에 존재하며
        # 원래의 일반 컬럼은 pseudo_key_lst 전체 set에 있는 컬럼과 value_col_lst를 합친 컬럼임
        all_value_col = list({col_lst:'_' for col_lst in all_pseudo_col_lst + value_col_lst}.keys())
        normal_col_lst = [col for col in all_value_col if col not in org_key_lst]
        
        self.foreach_bach_process(df, epoch_id, tbl_nm, org_key_lst, normal_col_lst, spark)
        self.write_log(f'Insert: {insrt_cnt} 건, Update: {updt_cnt}건, Delete: {del_cnt}건')
        self.write_log('INFO', 'foreach_bach_process 종료', epoch_id)
    