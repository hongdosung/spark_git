import psycopg2

class PostgresSender():
    def __init__(self, host, port, dbname, user, passwd):
        self.host = host
        self.port = port 
        self.dbname = dbname
        self.user = user
        self.passwd = passwd
    
    
    def get_con(self):
        try:
            self.conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user, passwd=self.passwd)           
        except:
            raise Exception(f"can't connect database({self.database_name}) by user: {self.user} / passwd: {self.passwd}")