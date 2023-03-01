from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
from typing import Union
from fastapi import Depends, FastAPI, HTTPException, status
from .user_auth import AuthHandler
from .database_util import database_methods
from .aws_functions import aws_function

app = FastAPI()
auth_handler = AuthHandler()
users = []
db_method=database_methods()
aws_functions=aws_function()

##Class for userinput of data for the requests
class UserInput(BaseModel):
    year:int
    month:int
    date:int
    station:Optional [str] = None
    
##Class for user data to login and register
class UserData(BaseModel):
    username:str
    password: str
    tier: Optional[str] = 'free'
    
##Class for logging data into cloudwatch
class Logging(BaseModel):
    msg:str
    log_stream:Optional[str]='default'

##Class for query the cloudwatch datalog
class Log_Query(BaseModel):
    log_name:str
    status:int
    filter_range:Optional[str]='last_hour'
    username:Optional[str]='admin',
    api_name: Optional[str] = None

@app.get("/fetch_url_nexrad",status_code=status.HTTP_200_OK)
async def fetch_url(userinput: UserInput,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_nexrad_url = f"https://noaa-nexrad-level2.s3.amazonaws.com/index.html#{userinput.year:04}/{userinput.month:02}/{userinput.date:02}/{userinput.station}"
        aws_functions.create_AWS_logs(f"User = {username} , API = fetch_url_nexrad , Status= 200_Ok","fast-api_logs")
        return {'url': aws_nexrad_url }
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = fetch_url_nexrad , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get("/fetch_url_goes",status_code=status.HTTP_200_OK)
async def fetch_url(userinput: UserInput,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_nexrad_url = f"https://noaa-goes18.s3.amazonaws.com/index.html#ABI-L1b-RadC/{userinput.year:04}/{userinput.month:02}/{userinput.date:02}"
        aws_functions.create_AWS_logs(f"User = {username} , API = fetch_url_goes , Status= 200_Ok" ,"fast-api_logs")
        return {'url': aws_nexrad_url }
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = fetch_url_nexrad , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.post('/register', status_code=status.HTTP_201_CREATED)
async def register(auth_details: UserData):
    user_fetch_status=db_method.fetch_user(auth_details.username)
    if user_fetch_status != 'no_user_found' or user_fetch_status == 'Exception': 
        aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = register , Status= 400_bad_request" ,"fast-api_logs")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Username is taken')
    hashed_password = auth_handler.get_password_hash(auth_details.password)
    user_status=db_method.add_user(auth_details.username,hashed_password,auth_details.tier)
    if user_status=='failed_insert':
        aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = register , Status= 400_bad_request" ,"fast-api_logs")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Error')
    aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = register , Status= 201_created" ,"fast-api_logs")

@app.post('/forgot_password',status_code=status.HTTP_201_CREATED)
async def reset_password(auth_details: UserData):
    fetch_user_status=db_method.fetch_user(auth_details.username)
    if isinstance(fetch_user_status, str) and fetch_user_status == 'no_user_found':
        aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = forgot_password , Status= 401_unauthorized" ,"fast-api_logs")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid username and/or password')
    hashed_password = auth_handler.get_password_hash(auth_details.password)
    user_status=db_method.update_password(auth_details.username,hashed_password)
    if user_status=='update_failed':
        aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = forgot_password , Status= 400_bad_request" ,"fast-api_logs")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Error')
    aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = forgot_password , Status= 201_created" ,"fast-api_logs")

@app.post('/login',status_code=status.HTTP_200_OK)
async def login(auth_details: UserData):
    fetch_user_status=db_method.fetch_user(auth_details.username)
    if isinstance(fetch_user_status, str) and fetch_user_status == 'no_user_found':
        aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = login , Status= 401_unauthorized" ,"fast-api_logs")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid username and/or password')
    if not auth_handler.verify_password(auth_details.password, fetch_user_status[0]['password']):
        aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = login , Status= 401_unauthorized" ,"fast-api_logs")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid username and/or password')
    token = auth_handler.encode_token(fetch_user_status[0]['username'])
    aws_functions.create_AWS_logs(f"User = {auth_details.username} , API = login , Status= 200_ok" ,"fast-api_logs")
    return {auth_details.username:token}

@app.get('/geos_get_year',status_code=status.HTTP_200_OK)
async def goes_year(username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = {username} , API = geos_get_year , Status= 200_ok" ,"fast-api_logs")
        return db_method.geos_get_year()
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = geos_get_year , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/geos_get_day/{year}',status_code=status.HTTP_200_OK)
async def goes_year(year:int,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = {username} , API = geos_get_day , Status= 200_ok" ,"fast-api_logs")
        return db_method.geos_get_day(year)
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = geos_get_day , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')
    
@app.get('/geos_get_hour/{year}/{day}',status_code=status.HTTP_200_OK)
async def goes_year(year:int, day:int,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = {username} , API = geos_get_hour , Status= 200_ok" ,"fast-api_logs")
        return db_method.geos_get_hour(year, day)
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = geos_get_hour , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/nexrad_get_year',status_code=status.HTTP_200_OK)
async def goes_year(username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_year , Status= 200_ok" ,"fast-api_logs")
        return db_method.nexrad_get_year()
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_year , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/nexrad_get_month/{year}',status_code=status.HTTP_200_OK)
async def goes_year(year:int,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_month , Status= 200_ok" ,"fast-api_logs")
        return db_method.nexrad_get_month(year)
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_month , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/nexrad_get_day/{year}/{month}',status_code=status.HTTP_200_OK)
async def goes_year(year:int, month:int,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_day , Status= 200_ok" ,"fast-api_logs")
        return db_method.nexrad_get_day(year, month)
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_day , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/nexrad_get_sites/{year}/{month}/{day}',status_code=status.HTTP_200_OK)
async def goes_year(year:int, month:int, day:int,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_sites , Status= 200_ok" ,"fast-api_logs")
        return db_method.nexrad_get_sites(year, month, day)
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_get_sites , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/get_nexrad_sites',status_code=status.HTTP_200_OK)
async def goes_year(username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        return db_method.get_nexrad_sites()
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = get_nexrad_sites , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/copy_file_s3/{source_bucket_name}/{product}/{year}/{day}/{hour}/{filename}',status_code=status.HTTP_200_OK)
async def copy_file_s3(source_bucket_name:str,product:str,year:int,day:int,hour:int,filename:str,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        file = f'{product}/{year}/{day}/{hour}/{filename}'
        status_copy=aws_functions.downloadFileAndMove(source_bucket_name, file)
        aws_functions.create_AWS_logs(f"User = {username} , API = copy_file_s3 , Status= 200_ok" ,"fast-api_logs")
        if status_copy == False:
            aws_functions.create_AWS_logs(f"User = {username} , API = copy_file_s3 , Status= 400_bad_request" ,"fast-api_logs")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Invalid request')
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = copy_file_s3 , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')
    
@app.get('/copy_nexrad_file_s3/{source_bucket_name}/{year}/{month}/{day}/{site}/{filename}',status_code=status.HTTP_200_OK)
async def copy_file_s3(source_bucket_name:str,year:int,month:int,day:int,site:str,filename:str,username=Depends(auth_handler.auth_wrapper)):    
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        file = f'{year}/{month}/{day}/{site}/{filename}'
        status_copy=aws_functions.downloadFileAndMove(source_bucket_name, file)
        aws_functions.create_AWS_logs(f"User = {username} , API = copy_nexrad_file_s3 , Status= 200_ok" ,"fast-api_logs")
        if status_copy == False:
            aws_functions.create_AWS_logs(f"User = {username} , API = copy_nexrad_file_s3 , Status= 400_bad_request" ,"fast-api_logs")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Invalid request')
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = copy_nexrad_file_s3 , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get('/get_goes_by_filename/{source_bucket_name}/{filename}',status_code=status.HTTP_200_OK)
async def copy_file_s3(source_bucket_name:str, filename:str,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        file_prefix=aws_functions.get_geos_file_link(filename)
        if file_prefix == False:
            aws_functions.create_AWS_logs(f"User = {username} , API = get_goes_by_filename , Status= 400_bad_request" ,"fast-api_logs")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Invalid request')
        else:
            aws_functions.create_AWS_logs(f"User = {username} , API = get_goes_by_filename , Status= 200_ok" ,"fast-api_logs")
            return file_prefix
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = get_goes_by_filename , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')
    
@app.get('/nexrad_query_files/{year}/{month}/{day}/{site}',status_code=status.HTTP_200_OK)
async def copy_file_s3(year:int, month:int, day:int, site:str,username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        file_prefix=aws_functions.nexrad_query_files(year, month, day, site)
        if file_prefix == False:
            aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_query_files , Status= 400_bad_request" ,"fast-api_logs")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Invalid request')
        else:
            aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_query_files , Status= 200_ok" ,"fast-api_logs")
            return file_prefix
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = nexrad_query_files , Status= 429_limit_exceed","fast-api_logs")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get("/healthz",status_code=status.HTTP_200_OK)
async def hello(username=Depends(auth_handler.auth_wrapper)):
    eligible_status=db_method.check_if_eligible(username)
    if eligible_status:
        aws_functions.create_AWS_logs(f"User = Unauthenticated , API = healthz","default")
        return {"status": "connected"}
    else:
        aws_functions.create_AWS_logs(f"User = {username} , API = healthz , Status= 429_limit_exceed","default")
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail='Limit Exceeded')

@app.get("/",status_code=status.HTTP_200_OK)
def hello():
    return {"status": "connected"}

@app.post('/logging_cloudwatch',status_code=status.HTTP_200_OK)
def logging(logging:Logging):
    aws_functions.create_AWS_logs(f"User = Streamlit , API = logging_cloudwatch Msg: {logging.msg}",logging.log_stream)
    
    
@app.get('/get_log_count')
def get_log(queryLog:Log_Query,username=Depends(auth_handler.auth_wrapper)):
    status=f"Status= "+str(queryLog.status)
    return_logs=aws_functions.read_cloudwatch_logs(queryLog.log_name,status,queryLog.username,queryLog.filter_range,queryLog.api_name)
    return len(return_logs)

@app.get('/get_full_logs')
def get_log(queryLog:Log_Query,username=Depends(auth_handler.auth_wrapper)):
    status=f"Status= "+str(queryLog.status)
    return_logs=aws_functions.read_cloudwatch_logs(queryLog.log_name,status,queryLog.username,queryLog.filter_range,queryLog.api_name)
    return return_logs
