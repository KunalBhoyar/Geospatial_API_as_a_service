import sqlite3
import json
import boto3
from dotenv import load_dotenv
import os
import time,datetime

load_dotenv()
class database_methods():
    def __init__(self):
        conn = sqlite3.connect(os.environ.get('DbUser'))
        self.cursor_user = conn.cursor()
        self.cursor_user.execute('''CREATE TABLE IF NOT EXISTS USER (id INTEGER PRIMARY KEY AUTOINCREMENT ,username TEXT NOT NULL UNIQUE, password TEXT NOT NULL, tier TEXT NOT NULL,current_count NUMBER DEFAULT 0 ,last_request_time DATETIME ,created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        conn_geo = sqlite3.connect(os.environ.get('DbGeo'))
        conn.commit()
        self.cursor_geo = conn_geo.cursor()
        
    def create_connection(self,database_name):
        conn = sqlite3.connect(os.environ.get('DbPath')+"/"+database_name+".db")
        cursor = conn.cursor()
        return conn,cursor
        
    def to_dict_list(self,rows, columns):
        return [dict(zip(columns, row)) for row in rows]

    def get_allowed_count(self, tier):
        if tier == 'free':
            return 10
        if tier == 'gold':
            return 15
        if tier == 'platinum':
            return 20
    def check_if_eligible(self,username):
        try:
            _,cursor_user=self.create_connection('USER_DATA')
            query=f"select current_count,tier,last_request_time from USER where username='{username}'"
            cursor_user.execute(query)
            rows = cursor_user.fetchall()
            response=self.return_json(rows,cursor_user)[0]
            if response['last_request_time'] == None:
                response['last_request_time']=datetime.datetime.now()
                self.update_last_req_time(username,datetime.datetime.now())
                self.update_count_for_user(username,0)
            last_request_time = datetime.datetime.strptime(response['last_request_time'], '%Y-%m-%d %H:%M:%S.%f')
            time_elapsed=datetime.datetime.now() - last_request_time
            if time_elapsed > datetime.timedelta(hours=1):
                self.update_count_for_user(username,1)
                return True
            elif time_elapsed < datetime.timedelta(hours=1):
                allowed_count=self.get_allowed_count(response['tier'])
                if response['current_count'] == allowed_count:
                    return False
                elif int(response['current_count']) < allowed_count:
                    self.update_count_for_user(username,response['current_count']+1)
                    return True
        except Exception as e:
            print("check_if_eligible: "+str(e))
            return "failed_insert"
        
    def update_last_req_time(self,username,timestamp):
        try:
            conn,cursor_user=self.create_connection('USER_DATA')
            query=f"UPDATE USER SET last_request_time = '{timestamp}' where username='{username}'"
            cursor_user.execute(query)
            conn.commit()
            conn.close()
        except Exception as e:
            print("update_last_req_time: "+str(e))
            return "failed_insert"
       
    def update_count_for_user(self,username,count):
        try:
            conn,cursor_user=self.create_connection('USER_DATA')
            query=f"UPDATE USER SET current_count = {count} where username='{username}'"
            cursor_user.execute(query)
            conn.commit()
            conn.close()
        except Exception as e:
            print("update_count_for_user: "+str(e))
            return "failed_insert"     
    def add_user(self,username,password,tier):
        try:
            conn,cursor_user=self.create_connection('USER_DATA')
            cursor_user.execute("INSERT INTO USER (username, password,tier) VALUES (?,?,?)", (username,password,tier))
            conn.commit()
            conn.close()
            return "user_created"
        except Exception as e:
            print("add_user: "+str(e))
            return "failed_insert"
        
    def update_password(self,username,password):
        try:
            conn,cursor_user=self.create_connection('USER_DATA')
            query=f"UPDATE USER SET password = '{password}' where username='{username}'"
            cursor_user.execute(query)
            conn.commit()
            conn.close()
            return "password_updated"
        except Exception as e:
            print("update_password: "+str(e))
            return "update_failed"
        
    
    def fetch_user(self,user_name):
        try:
            _,cursor_user=self.create_connection('USER_DATA')
            query=f"select * from USER where username='{user_name}'"
            cursor_user.execute(query)
            rows = cursor_user.fetchall()
            if len(rows)==0:
                return "no_user_found"
            else:
                print(self.return_json(rows,cursor_user))
                return self.return_json(rows,cursor_user)
        except Exception as e:
            return 'Exception'

    
    def return_json (self, rows,cursor):
        result = []
        for row in rows:
            d = dict(zip([col[0] for col in cursor.description], row))
            result.append(d)
        # Convert the list of dictionaries to a JSON string
        json_result = json.dumps(result)
        json_obj = json.loads(json_result)
        # Print the JSON string
        return(json_obj)

    #### Query for GOES ####
    
    def geos_get_year(self):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute('SELECT DISTINCT year from goes18meta')
        rows = cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)

    def geos_get_day(self,year):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute(f'SELECT DISTINCT day from goes18meta where year={year}')
        rows=cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)

    def geos_get_hour(self,year, day):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute(f'SELECT DISTINCT hour from goes18meta where year={year} and day={day}')
        rows=cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)
    
    #### Query for NEXRAD ####
    def nexrad_get_year(self):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute(f'SELECT DISTINCT year from nexradmetadata')
        rows=cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)

    def nexrad_get_month(self,year):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute(f'SELECT DISTINCT month from nexradmetadata where year={year}')
        rows=cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)

    def nexrad_get_day(self,year, month):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute(f'SELECT DISTINCT day from nexradmetadata where year={year} and month={month}')
        rows=cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)

    def nexrad_get_sites(self,year, month, day):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute(f'SELECT DISTINCT stationcode from nexradmetadata where year={year} and month={month} and day={day}')
        rows=cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)

    #nexrad sites

    def get_nexrad_sites(self):
        _,cursor_geo=self.create_connection('GEOSPATIAL_DATA')
        cursor_geo.execute(f'SELECT lat, lon from nexrad_sites_data')
        rows=cursor_geo.fetchall()
        return self.return_json(rows,cursor_geo)
    
    def downloadFileAndMove(self, fileName, AWS_ACCESS_KEY_ID ,AWS_SECRET_ACCESS_KEY):
        print("FileName", fileName, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        try:
            session = boto3.Session(
                aws_access_key_id = AWS_ACCESS_KEY_ID,
                aws_secret_access_key = AWS_SECRET_ACCESS_KEY
            )
            
            s3 = session.resource('s3')

            copy_source = {
                'Bucket': "noaa-goes18",
                'Key': fileName
            }

            bucket = s3.Bucket('damg7245-s3-storage')
            
            bucket.copy(copy_source, fileName)
            return True
        except Exception as e:
            print(e)
            return False
