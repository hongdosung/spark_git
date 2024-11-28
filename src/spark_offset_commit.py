from confluent_kafka import Consumer, TopicPartition
import os
import json
from datetime import datetime, date, time
import logging
from logging import handlers

class SparkOffsetCommiter():
    def __init__(self):
        self.write_log = '/log/경로'
        self.set_logger()
        
    
    def set_logger(self):
        self.logger = logging.getLogger(__name__)
        formatter = logging.Formatter('%(asctime)s[%(levelname)s] >> %(message)s')
        formatter = logging.Formatter('')
        #fileHandler = logging.FileHander(self.write_log)
        #fileHandler.setFormatter(formatter)
        TimedRotatingFileHandler(self.write_log, when='midnight', interval=1, backupCount=30)
        TimedRotatingFileHandler.suffix = '%Y%m%d'
        TimedRotatingFileHandler.sefFormatter(formatter)
        self.logger.addHandler(TimedRotatingFileHandler)
        self.logger.setLevel(level=logging.INFO)
        
    
    def set_consumer(self):
        self.set_consumer = Consumer(self.conf)
        
    
    def set_conf(self, group_id):
        self.conf = {
            'bootstrap.server': '##bootstrap.server##',
            'group_id': group_id,
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'earliest'
        }
    
    
    def get_popen_rslt(self, cmd):
        return os.popen(cmd).read()
    
    
    def _main(self):
        now = datetime.datetime.now()
        self.logger.info(f'------------------------------ commit program start -------------------------------------')
        ### 현재 구동중인 Spark Appliction 조회
        app_list = self.get_popen_rslt('yarn app -list | awk \'{print $1" "$2} \' ').strip()
        app_name_lst = []
        for app in app_list('\n'):
            if app.startswith('application'):
                app_name = app.split(' ')[1].replace('.py', '')
                app_name_lst.append(app_name.upper())
        
        self.logger.info(f'Commit target application: {app_name_lst}')
        
        for app_name in app_name_lst:
            self.commit_per_apps(app_name)        
                
        self.logger.info(f'------------------------------ commit program end -------------------------------------')


    def commit_per_apps(self, app_name):
        ### app별로 consumer 신규 생성
        self.set_conf(f'SPARK_{app_name}')
        self.set_consumer()
        
        last_offset_number = self.get_popen_rslt(f'hdfs dfs -ls /home/spark/chk_kafka_offset/{app_name}/offsets | awk \'{{print $8}} \' | awk -F/ \'{{print $7}} \' | sort -k1n | tail -n 1').strip())
        offset_info = self.get_popen_rslt(f'hdfs dfs -cat /home/spark/chk_kafka_offset/{app_name}/offsets/{last_offset_number}')
        
        offset_info_dict = {}
        for line in offset_info.split('\n'):
            if line.upper().startswith('{DEV') or line.upper().startswith('{PROD'):
                offset_info_dict = json.load(line)
        
        offset_target_lst = []
        for topic_name, offset_info in offset_info_dict.items():
            for ptt, offset in offset_info.items():
                offset_target_lst.append(
                    TopicPartition(
                        topic     = topic_name,
                        partition = int(ptt),
                        offset    = offset
                    )
                )
        
        self.consumer.commit(offsets=offset_target_lst)
        self.logger.info(f'SPARK_{app_name} commit 완료({offset_info_dict})')
        
        
spark_offset_commiter = SparkOffsetCommiter()
spark_offset_commiter._main()