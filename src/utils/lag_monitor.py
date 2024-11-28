# burrow API를 1분 마다 조회하여 lag 확인
# 비정상으로 확인된 consumer group 존재 시 /log/burrow/lag_consumer.log에 기록 (Polestar 연동)

import requests
from datetime import datetime
import yaml
import os

class LagMoniter():
    def __init__(self):
        if '##stage##' == 'DEV':
            server_lst = ['dpmst01','dpmst02']
        else:
            server_lst = ['ppmst01','ppmst02']
    
    self.set_http_endpoint(server_lst)
    self.last_alarm_target_file = '/log/burrow/last_target.yml' # 이상 감지한 consumer를 연속적으로 알람 발생하지 않도록 감지한 대상 대상과 start_lag을 보관
    self.write_log_file = '/log/burrow/lag_consumer.log'
    self.re_alarm_period = 5 # 분단위


    def set_http_endpoint(self, server_lst):
        for server in server_lst:
            try:
                rsp = requests.get(f'http://{server}:8080/burrow/admin')
            except:
                continue
            else:
                self.http_endpoint = f'http://{server}:8080/v3/kafka/sbp_kafka'
                return
            
        raise Exception('연결 가능한 burrow 서버가 없습니다.')


    def get_consumers_status(self):
        now = datetime.now()
        # 마지막으로 감지했던 타겟정보 load
        if os.path.isfile(self.last_alarm_target_file):
            last_alarm_target = yaml.safe_load(open(self.last_alarm_target_file)) or {}
        else:
            last_alarm_target = {}
        
        new_alarm_target = {}
        
        # consumer group 조회
        resp_dict = requests.get(f'{self.http_endpoint}/consumer').json()
        consumer_lst = resp_dict['conumers']
        for consumer in consumer_lst:
            rslt = self.get_abnorm_consumer(consumer)
            if rslt:
                status = rslt[0]
                lag_per_topic = rslt[1]
                total_lag = rslt[2]
                
                # 마지막에 수집했던 정보 추출
                last_saved_info = last_alarm_target.get(consumer) or {}
                last_saved_status = last_alarm_target.get('status')
                last_saved_count = last_alarm_target.get('count')
                
                # 마지막으로 감지한 대상의 stauts 정보가 일치하면 이미 감지한 대상이므로 skip 처리하되 1시간 마다 재 알람
                if last_saved_status is not None and last_saved_status == status:
                    count = last_saved_count + 1
                    new_alarm_target[consumer] = {'status':status, 'count':count}
                    
                    # 비정상 상태에서 1시간 이상 경과하면 재 알람
                    if count % self.re_alarm_period == 0:
                        self.do_alarm(consumer, status, lag_per_topic, total_lag, count)
                else: # 첫 감지이거나 status가 바뀐 경우 count=1로 시작
                    count = 1
                    self.do_alarm(consumer, status, lag_per_topic, total_lag, count)
                    new_alarm_target[consumer] = {'status':status, 'count':count}
        
        target_yaml = yaml.dump(new_alarm_target)
        with open(self.last_alarm_target_file, 'w') as f:
            f.write(target_yaml)
            
    
    def do_alarm(self, consumer, status, lag_per_topic, total_lag, count):
        now = datetime.now()
        # 마지막으로 감지했던 타겟정보 load
        if status == 'ERR':
            err_comment = 'Commit Offset 증가하지 않음'
        elif status == 'WARN':
            err_comment = 'Commit Offset 증가하나 lag 지속 증가 중'
        else:
            err_comment = '상태확인 필요'
            
        if count == 1:
            status_comment = '첫 번째 발생'
        else:
            hour = count // 60
            minute = count % 60
            if hour == 0:
                status_comment = f'{minute}분 경과'
            else:
                status_comment = f'{hour}시간 {minute}분 경과'
        
        msg = f'''Comsumer lag 발생 Alarm ({now}) Consumer Group명: {consumer} status: {status}({err_comment})) topic {lag_per_topic} total lag: {total_lag} {status_comment}\n'''
        
        # 로그파일 기록(Poestar 감지용)
        with open(self.write_log_file, 'a') as f:
            f.write(msg)
            
    def get_abnorm_consumer(self, consumer_group):
        '''
        비정상적 consumer만 추출하여 상태 리턴
        '''
        status_dict = requests.get(f'{self.http_endpoint}/consumer/{consumer_group}/status').json().get('status')
        status = status_dict.get('status')
        
        if status in ['WARN', 'ERR', 'STOP:', 'STALL']:
            partition_lst = status_dict.get('partitions')
            status = status_dict.get('status')
            max_start_offset = status_dict.get('maxlag').get('start').get('offset')
            lag_per_topic = {}
            for partition_dict in partition_lst:
                topic = partition_dict['topic']
                cur_lag = partition_dict['current_lag']
                
                if topic in lag_per_topic:
                    lag_per_topic[topic] = lag_per_topic[topic] + cur_lag
                else:
                    lag_per_topic[topic] = cur_lag
            
            total_lag = status_dict.get('totallag')
            
            return status, lag_per_topic, total_lag
        else:
            return


    def _main(self):
         consumer_lst = self.get_consumer_lists()
         for consumer in consumer_lst:
             self.get_consumers_status(consumer)
             

lag_moniter = LagMoniter()
lag_moniter.get_consumers_status()