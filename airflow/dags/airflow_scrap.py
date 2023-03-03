from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.data_context.types.base import(
    DataContextConfig,
    CheckpointConfig
)


import os
import requests
import re
import sqlite3
import boto3
import time
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import logging


#load env variables
dotenv_path = Path('./dags/.env')
load_dotenv(dotenv_path=dotenv_path)
# load_dotenv()

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:$S'
)


base_path = "/opt/airflow/working_dir"
ge_root_dir = os.path.join(base_path, "great_expectations")
DATABASE_URL = 'sqlite:///./GEOSPATIAL_DATA.db'


# provide credentials ####################################
s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
                        )


dag = DAG(
    dag_id="metadata_scrapper_1",
    schedule="0 0 * * *",   #run daily - at midnight
    start_date=days_ago(0),
    catchup=False,
    tags=["assignments_2", "damg7245"],
)

def goes18_data():
    
    scraped_goes18_dict = {
        'id': [],
        'product': [],
        'year': [],
        'day': [],
        'hour': []
    }

    id=1    #for storing as primary key in db
    prefix = "ABI-L1b-RadC/"    #just one product to consider as per scope of assignment
    result = s3client.list_objects(Bucket=os.environ.get('GOES18_BUCKET_NAME'), Prefix=prefix, Delimiter='/')
    
    #DB connection
    conn = sqlite3.connect('assignment\dags\database\GEOSPATIAL_DATA.db')
    c = conn.cursor()
    c.execute('''DROP TABLE IF EXISTS goes18meta''')
    c.execute('''CREATE TABLE goes18meta
                    (year INTEGER, month INTEGER, hour INTEGER)''')
    
    
    for pref in result.get('CommonPrefixes'):
        path = pref.get('Prefix').split('/')
        prefix_2 = prefix + path[-2] + "/"      #new prefix with added subdirectory path
        sub_folder = s3client.list_objects(Bucket=os.environ.get('GOES18_BUCKET_NAME'), Prefix=prefix_2, Delimiter='/')
        for p in sub_folder.get('CommonPrefixes'):
            sub_path = p.get('Prefix').split('/')
            prefix_3 = prefix_2 + sub_path[-2] + "/"    #new prefix with added subdirectory path
            sub_sub_folder = s3client.list_objects(Bucket=os.environ.get('GOES18_BUCKET_NAME'), Prefix=prefix_3, Delimiter='/')
            for q in sub_sub_folder.get('CommonPrefixes'):
                sub_sub_path = q.get('Prefix').split('/')
                sub_sub_path = sub_sub_path[:-1]    #remove the filename from the path
                scraped_goes18_dict['id'].append(id)   #map all scraped data into the dict
                scraped_goes18_dict['product'].append(sub_sub_path[0])
                scraped_goes18_dict['year'].append(sub_sub_path[1])
                scraped_goes18_dict['day'].append(sub_sub_path[2])
                scraped_goes18_dict['hour'].append(sub_sub_path[3])
                id+=1
        
    scr_goes18_df = pd.DataFrame(scraped_goes18_dict)
    
    database_file_name = 'GEOSPATIAL_DATA.db'
    goes_ddl_file_name = 'goes18meta.sql'
    goes18_tablename = 'goes18_METADATA'
    database_file_path = os.path.join(os.path.dirname(__file__), database_file_name)
    goes_ddl_file_path = os.path.join(os.path.dirname(__file__), goes_ddl_file_name)
    
    ###############################################################################################################
    # print(os.path.dirname(__file__))
    if not Path(database_file_path).is_file():
        logging.info(f"Database file not found, initilizing at: {database_file_path}")
        
        #create db
        with open(goes_ddl_file_path, 'r') as sql_file:
            sql_script = sql_file.read()
        db = sqlite3.connect(database_file_path)
        cursor = db.cursor()
        scr_goes18_df.to_sql(goes18_tablename, db, if_exists='replace', index=False) ################ missing table name
        
    else:
        logging.info("Database file already exist!")
        db = sqlite3.connect(database_file_path)
        cursor = db.cursor()
        scr_goes18_df.to_sql(goes18_tablename, db, if_exists='replace', index=False) ################ missing table name
        
    db.commit()
    db.close()
    ###############################################################################################################
    
    
def nexrad_data():
    
    scraped_nexrad_dict = {
        'id': [],
        'year': [],
        'month': [],
        'day': [],
        'ground_station': []
    }
    

    id=1    #for storing as primary key in db
    years_to_scrape = ['2022', '2023']   
    
    for year in years_to_scrape:
        prefix = year+"/"    #replace this with user input from streamlit UI with / in end
        result = s3client.list_objects(Bucket=os.environ.get('NEXRAD_BUCKET_NAME'), Prefix=prefix, Delimiter='/')
        #travesing into each subfolder and store the folder names within each
        for o in result.get('CommonPrefixes'):
            path = o.get('Prefix').split('/')
            prefix_2 = prefix + path[-2] + "/"      #new prefix with added subdirectory path
            sub_folder = s3client.list_objects(Bucket=os.environ.get('NEXRAD_BUCKET_NAME'), Prefix=prefix_2, Delimiter='/')
            for p in sub_folder.get('CommonPrefixes'):
                sub_path = p.get('Prefix').split('/')
                prefix_3 = prefix_2 + sub_path[-2] + "/"    #new prefix with added subdirectory path
                sub_sub_folder = s3client.list_objects(Bucket=os.environ.get('NEXRAD_BUCKET_NAME'), Prefix=prefix_3, Delimiter='/')
                for q in sub_sub_folder.get('CommonPrefixes'):
                    sub_sub_path = q.get('Prefix').split('/')   #remove the filename from the path
                    scraped_nexrad_dict['id'].append(id)   #map all scraped data into the dict
                    scraped_nexrad_dict['year'].append(sub_sub_path[0])
                    scraped_nexrad_dict['month'].append(sub_sub_path[1])
                    scraped_nexrad_dict['day'].append(sub_sub_path[2])
                    scraped_nexrad_dict['ground_station'].append(sub_sub_path[3])
                    id+=1
    
    scraped_nexrad_df = pd.DataFrame(scraped_nexrad_dict)
    ####################################################################################################
    database_file_name = 'GEOSPATIAL_DATA.db'
    nexrad_ddl_file_name = 'nexradmetadata'
    nex_tablename = 'NEXRAD_METADATA'
    database_file_path = os.path.join(os.path.dirname(__file__), database_file_name)
    nexrad_ddl_file_path = os.path.join(os.path.dirname(__file__), nexrad_ddl_file_name)
    
    if not Path(database_file_path).is_file():
        logging.info(f"Database file not found, initilizing at: {database_file_path}")
        
        with open(nexrad_ddl_file_path, 'r') as sql_file:
            sql_script = sql_file.read()
        db = sqlite3.connect(database_file_path)
        cursor = db.cursor()
        scraped_nexrad_df.to_sql(nex_tablename, db, if_exists='replace', index=False)
        
        
    else:
        logging.info("Database file already exist!")
        db = sqlite3.connect(database_file_path)
        cursor = db.cursor()
        scraped_nexrad_df.to_sql(nex_tablename, db, if_exists='replace', index=False)
    
    db.commit()
    db.close()
    
    
    ####################################################################################################
    
    
def export_to_csv():
    
    
    s3res = boto3.resource('s3', region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'))
    s3res.Bucket(os.environ.get('MY_BUCKET_NAME')).upload_file("./dags/GEOSPATIAL_DATA.db", "db_files/GEOSPATIAL_DATA.db")
    
    database_file_name = 'GEOSPATIAL_DATA.db'
    database_file_path = os.path.join(os.path.dirname(__file__),database_file_name)
    conn = sqlite3.connect(database_file_path, isolation_level=None, detect_types=sqlite3.PARSE_COLNAMES)
    
    
    # change table names #######################
    goesdf = pd.read_sql_query('SELECT * from goes18_METADATA', conn)
    nex = pd.read_sql_query('SELECT * from NEXRAD_METADATA', conn)
    

    
    
    # change key value ########################
    # s3client = boto3.client('s3', region_name='us-east-1',
    #                 aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
    #                 aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'))
    

    
    s3client.put_object(Body=goesdf.to_csv(index=False), Bucket=os.environ.get('MY_BUCKET_NAME'), Key='db_files/goes18_data.csv')
    s3client.put_object(Body=nex.to_csv(index=False), Bucket=os.environ.get('MY_BUCKET_NAME'), Key='db_files/nexrad_data.csv')
    
    
    goesdf.to_csv(f"/opt/airflow/working_dir/data/goes18_data.csv", sep=",", index=False)
    nex.to_csv(f"/opt/airflow/working_dir/data/nexrad_data.csv", sep=",", index=False)
    
    logging.info(goesdf)
    logging.info(nex)    



# def export_noaa_db(**kwargs):
#     engine = create_engine(DATABASE_URL)
#     engine.connect()
#     res = pd.read_sql()
    



    

with dag:

    goes18_metadata_ext = PythonOperator(
        task_id = 'goes18_data',
        python_callable = goes18_data
    )

    nexrad_metadata_ext = PythonOperator(
        task_id = 'nexrad_data',
        python_callable = nexrad_data
    )
    
    export_csv = PythonOperator(
        task_id = 'export_to_csv',
        python_callable = export_to_csv
    )
    
    ge_goes18_context_ck_pass = GreatExpectationsOperator(
        task_id = "ge_goes18_context_ck_pass",
        data_context_root_dir = ge_root_dir,
        checkpoint_name='goes18_ck_v1',
        fail_task_on_validation_failure=False,
        return_json_dict = True
    )
    
    ge_nexrad_context_ck_pass = GreatExpectationsOperator(
    task_id = "ge_nexrad_context_ck_pass",
    data_context_root_dir = ge_root_dir,
    checkpoint_name='nexrad_ck_v1',
    fail_task_on_validation_failure=False,
    return_json_dict = True
    )
    
    


goes18_metadata_ext >> nexrad_metadata_ext >> export_csv >> ge_goes18_context_ck_pass >> ge_nexrad_context_ck_pass