'''
- This script is used to create S3 bucket with access log enabled and Enable Athena to analyze the S3 logs
- Resource created
    - Source S3 bucket (User Input Required)
    - S3 Access Log Bucket for Source bucket
    - S3 Log bucket for Athena
    - Athena Database and Table
    - Athena Workgroup
- Developer - K.Janarthanan
- Date - 18/5/2020
'''

import boto3
import sys
import datetime
import string
import random
import os
from botocore.exceptions import ClientError


regions=boto3.client('ec2')
response=regions.describe_regions()

print("\n--------------\nAWS S3 Region :\n--------------\n")

region_key={}
for i in range(len(response['Regions'])):
    region_key[i]=response['Regions'][i]['RegionName']
    print ("["+str(i)+"] "+response['Regions'][i]['RegionName'])

region_num=int(input("\nInput the Region Number : "))

if(region_num not in region_key.keys()):
    print("Region Key is invalid. Script existing")
    sys.exit()

bucket_name=input("\nInput your bucket name : ")

print("\nLogs\n-------\n")

os.environ['AWS_DEFAULT_REGION'] = region_key[region_num]
client=boto3.client('s3')

try:
    response_bucket= client.create_bucket(
        ACL='private',
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': region_key[region_num]
        },
    )
   
    if(response_bucket['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Status code from creation of source bucket"+bucket_name+" is not 200")
    else:
        print ("Source Bucket "+bucket_name+" is successfully created")

    #List bucket to get canonical user id of the user
    response_list=client.list_buckets()
    canonical_user_id=response_list['Owner']['ID']

    if(response_list['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Status code from list bucket operation inorder to retrieve Canonical User ID is not 200")
    else:
        print("Successfully retrieved Canonical User ID ")

    #Creating Access Log bucket
    #Name will be "source bucket"+"date"+"random string "
    date_append=str(datetime.date.today())
    random_append=''.join(map(str, random.sample(string.ascii_lowercase,3)))
    access_log_bucket=bucket_name+"-accesslog-"+date_append+"-"+random_append

    response_access = client.create_bucket(
        Bucket=access_log_bucket,
        CreateBucketConfiguration={
            'LocationConstraint': region_key[region_num]
        },
        GrantReadACP='uri=http://acs.amazonaws.com/groups/s3/LogDelivery',
        GrantWrite='uri=http://acs.amazonaws.com/groups/s3/LogDelivery',
        GrantFullControl='id='+str(canonical_user_id),
    )

    if(response_access['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Status code from creation of access log bucket"+access_log_bucket+" is not 200")
    else:
        print("Access Log bucket "+access_log_bucket+" is successfully created ")
      
    #Creation Athena Log bucket
    athena_bucket=bucket_name+"-athena-"+date_append+"-"+random_append

    response_athena = client.create_bucket(
        ACL='private',
        Bucket=athena_bucket,
        CreateBucketConfiguration={
            'LocationConstraint': region_key[region_num]
        },
    )

    if(response_athena['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Status code from creation of Athena Log bucket"+athena_bucket+" is not 200")
    else:
        print("Athena Log Bucket "+athena_bucket+" is successfully created ")
    

    #Block Public in all buckets

    #Making source bucket Block all public access
    response_block_source = client.put_public_access_block(
    Bucket=bucket_name,
    PublicAccessBlockConfiguration={
        'BlockPublicAcls': True,
        'IgnorePublicAcls': True,
        'BlockPublicPolicy': True,
        'RestrictPublicBuckets': True
        }
    )

    if(response_block_source['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Operation to Block public access to Bucket "+bucket_name+" is not 200")
    else:
        print("Successfully blocked public access to "+bucket_name)

    #Making Log bucket Block all public access
    response_block_log = client.put_public_access_block(
    Bucket=access_log_bucket,
    PublicAccessBlockConfiguration={
        'BlockPublicAcls': True,
        'IgnorePublicAcls': True,
        'BlockPublicPolicy': True,
        'RestrictPublicBuckets': True
        }
    )

    if(response_block_log['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Operation to Block public access to Bucket "+access_log_bucket+" is not 200")
    else:
        print("Successfully blocked public access to "+access_log_bucket)

    #Making Athena Log bucket Block all public access
    response_block_athena = client.put_public_access_block(
    Bucket=athena_bucket,
    PublicAccessBlockConfiguration={
        'BlockPublicAcls': True,
        'IgnorePublicAcls': True,
        'BlockPublicPolicy': True,
        'RestrictPublicBuckets': True
        }
    )

    if(response_block_athena['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Operation to Block public access to Bucket "+athena_bucket+" is not 200")
    else:
        print("Successfully Blocked public access to "+athena_bucket)

    #Enabling the Access Log on the source bucket
    response_enable_accesslog = client.put_bucket_logging(
    Bucket=bucket_name,
    BucketLoggingStatus={
        'LoggingEnabled': {
            'TargetBucket': access_log_bucket,
            'TargetPrefix': bucket_name,
            },
        },
    )

    if(response_enable_accesslog['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise Exception("Operation to enable Access Logging to Bucket "+bucket_name+" is not 200")
    else:
        print("Successfully enabled the Access Logging to the Bucket "+bucket_name+" with Target Bucket set to "+access_log_bucket)

    client=boto3.client('athena')

    database=('athena_analysis_'+bucket_name).replace("-","_")
    table=('log_'+bucket_name).replace("-","_")

    response_create_db = client.start_query_execution(
        QueryString='create database '+database,
        ResultConfiguration={
            'OutputLocation': 's3://'+athena_bucket
        },
)

    if(response_create_db['ResponseMetadata']['HTTPStatusCode'] != 200):
            raise Exception("Status code from creation of Athena DB "+databaset+" is not 200")
    else:
            print("Athena Database "+database+" is successfully created ")

    schema_table="CREATE EXTERNAL TABLE IF NOT EXISTS "+database+"."+table+"""(
            BucketOwner STRING,
            Bucket STRING,
            RequestDateTime STRING,
            RemoteIP STRING,
            Requester STRING,
            RequestID STRING,
            Operation STRING,
            Key STRING,
            RequestURI_operation STRING,
            RequestURI_key STRING,
            RequestURI_httpProtoversion STRING,
            HTTPstatus STRING,
            ErrorCode STRING,
            BytesSent BIGINT,
            ObjectSize BIGINT,
            TotalTime STRING,
            TurnAroundTime STRING,
            Referrer STRING,
            UserAgent STRING,
            VersionId STRING,
            HostId STRING,
            SigV STRING,
            CipherSuite STRING,
            AuthType STRING,
            EndPoint STRING,
            TLSVersion STRING
    ) 
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
           'serialization.format' = '1', 'input.regex' = '([^ ]*) ([^ ]*) \\\\\\[(.*?)\\\\\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \\\\\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\\\\" (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\") ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$' )
    LOCATION 's3://"""+access_log_bucket+"/'"

    response_create_table = client.start_query_execution(
        QueryString=schema_table,
        ResultConfiguration={
            'OutputLocation': 's3://'+athena_bucket
        },
    )

    if(response_create_table['ResponseMetadata']['HTTPStatusCode'] != 200):
            raise Exception("Status code from creation of Athena DB Table "+table+" is not 200")
    else:
            print("Athena Database Table "+table+" is successfully created ")

    response_workgroup = client.create_work_group(
    Name=bucket_name,
    Configuration={
        'ResultConfiguration': {
            'OutputLocation': 's3://'+athena_bucket,
        },
        'EnforceWorkGroupConfiguration': True,
    },
    Description='This workgroup is dedicated to '+bucket_name,
    )

    if(response_workgroup['ResponseMetadata']['HTTPStatusCode'] != 200):
            raise Exception("Status code from creation of Workgroup "+bucket_name+" is not 200")
    else:
            print("Workgroup creation "+bucket_name+" is successfully created ")

    print("\nResources Created\n---------------------")
    print("\nSource Bucket : "+bucket_name+"\nAccess Log Bucket : "+access_log_bucket+"\nAthena Log Bucket : "+athena_bucket+"\nAthena Database : "+database+"\nAthena Table : "+table+"\nAthena Workgroup Name : "+bucket_name)

except ClientError as e:
    if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print("Bucket is already existing and it's owned by You")

    elif e.response['Error']['Code'] == 'BucketAlreadyExists':
        print("Bucket Name is already existing. Please choose a different name")
    elif e.response['Error']['Code'] == 'ParamValidationError':
        print("Input Parameter Error")
    else:
        print("Unexpected error: %s" % e)
 