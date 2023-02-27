import boto3
import time
from dotenv import load_dotenv
import os
import string


class aws_function():
    def __init__(self):
        #loading env variables
        load_dotenv()
        # Define the AWS access key and secret
        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.logGroupName=os.environ.get('LOG_GROUP_NAME')
        self.logStreamNameFileCopy=os.environ.get('LOG_STREAM_NAME_FILE_COPY')
        self.logStreamNameFastApi=os.environ.get('LOG_STREAM_NAME_FAST_API')
        self.logStreamNameDefault=os.environ.get('LOG_STREAM_NAME_DEFAULT')
        self.logStreamNameStreamlit=os.environ.get('LOG_STREAM_NAME_STREAMLIT')
        self.bucket_name=os.environ.get('MY_BUCKET_NAME')
    # Create an S3 client using the access key and secret
    def init_resources(self):
        cloudwatch = boto3.client('logs', 
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name='us-east-1')
        return cloudwatch

    def get_log_stream_name(self,stream_name):
        if stream_name == 'fast-api_logs' :
            logStreamName=self.logStreamNameFastApi
        elif stream_name == 'file_copy':
            logStreamName=self.logStreamNameFileCopy
        elif stream_name == 'streamlit':
            logStreamName=self.logStreamNameStreamlit
        else:
            logStreamName=self.logStreamNameDefault
        return logStreamName
    def create_AWS_logs(self,msg,stream_name):
        logStreamName= self.get_log_stream_name(stream_name)
        cloudwatch = self.init_resources()
        cloudwatch.put_log_events(
                    logGroupName=self.logGroupName,
                    logStreamName=logStreamName,
                    logEvents=[
                            {
                                'timestamp': int(time.time() * 1000),
                                'message': msg
                            },
                        ]
                )
    
    #copy file from goes public bucket
    def downloadFileAndMove(self, source_bucketName, fileName):
        try:
            session = boto3.Session(
                aws_access_key_id = self.aws_access_key_id,
                aws_secret_access_key = self.aws_secret_access_key
            )
            s3 = session.resource('s3')
            copy_source = {
                'Bucket': source_bucketName,
                'Key': fileName
            }
            bucket = s3.Bucket(self.bucket_name)
            bucket.copy(copy_source, fileName)
            self.create_AWS_logs(f"File Copy: {fileName} from {source_bucketName} to {bucket}","file_copy")
            return True
        except Exception as e:
            print(e)
            self.create_AWS_logs(f"File Copy error: {e} from {source_bucketName} to {bucket}","file_copy")
            return False

    #search by filename
    def get_geos_file_link(self, filename):

        # print("get_geos_file_link", filename)

        try:
            file = filename.split("_")
            print(file)
            # prefix = "https://noaa-goes18.s3.amazonaws.com/"
            product = '-'.join(file[1].split("-")[:-1]).rstrip(string.digits)
            year = file[3][1:5]
            day = file[3][5:8]
            hour = file[3][8:10]

            file_prefix =  product + "/" + year + "/" + day + "/" + hour + "/" + filename

            # print("*****************************", file_prefix)

            goesFileStatus = self.downloadFileAndMove("noaa-goes18", file_prefix)
            print(goesFileStatus)
            if goesFileStatus:
                return {"file_prefix": file_prefix}
            return False
        
        except:
            return False
        
    def nexrad_query_files(self, year, month, day, site):

        # print(year, month, day, site)

        try:
            source_bucket_name = "noaa-nexrad-level2"

            s3client = boto3.client('s3',
                                region_name='us-east-1')

            # prefix = product + "/" + str(year) + "/" + str(day) + "/" + str(hour)
            prefix = str(year) + "/" + str(month) + "/" + str(day) + "/" + str(site)

            print(prefix)

            response = s3client.list_objects_v2(Bucket = source_bucket_name, Prefix = prefix )
            contents = response.get("Contents")
            
            files = []

            for content in contents:
                print(content['Key'])
                # files.append(content['Key'])
                files.append(content['Key'].split("/")[-1])

            return files
        
        except:
            print("Bucket or files not found")

    def get_nexrad_file_link(self, filename):
        try:
            source_bucket_name = "noaa-nexrad-level2"
            file = filename.split("_")

            site = file[0][:4]
            year = file[0][4:8]
            month = file[0][8:10]
            day = file[0][10:12]

            file_prefix =  year + "/" + month + "/" + day + "/" + site + "/" + filename

            fileStatus = self.downloadFileAndMove(source_bucket_name, file_prefix)

            if fileStatus:
                return file_prefix
            
            return fileStatus
        
        except:
            return False
    
    def read_cloudwatch_logs(self,stream_name,code,username,filter_range):
        logStreamName= self.get_log_stream_name(stream_name)
        cloudwatch = self.init_resources()
        response=cloudwatch.get_log_events(
            logGroupName=self.logGroupName,
            logStreamName=logStreamName
        )
        return self.filter_logs(cloudwatch,code,username,filter_range)
    
    def filter_logs(self,cloudwatch,code,username,filter_range):
        # write a CloudWatch Logs Insights query
        if username == 'admin':
            query = f"fields @timestamp, @message, @logStream | filter @message like /{code}/ | sort @timestamp desc"
        else: 
            query = f"fields @timestamp, @message, @logStream | filter @message like /{code}/ and @ message like /{username}/ | sort @timestamp desc"
        if filter_range == 'last_hour':
            time_passed=3600
        elif filter_range == 'last_day':
            time_passed=86400
        elif filter_range == 'last_week':
            time_passed=604800
        elif filter_range == 'last_month':
           time_passed=2629746 
        # execute the query and retrieve results
        query_response = cloudwatch.start_query(
            logGroupName=self.logGroupName,
            startTime=int((time.time() - time_passed) * 1000), # start time (in milliseconds) for the query (in this example, last hour)
            endTime=int(time.time() * 1000), # end time (in milliseconds) for the query (in this example, now)
            queryString=query
        )
        # retrieve query ID and status
        query_id = query_response['queryId']
        query_status = None
        # wait for query to complete
        while query_status == None or query_status == 'Running':
            print('Waiting for query to complete ...')
            time.sleep(1)
            query_status = cloudwatch.get_query_results(
                queryId=query_id
            )['status']
        # retrieve query results
        query_results = cloudwatch.get_query_results(
            queryId=query_id
        )
        return query_results['results']