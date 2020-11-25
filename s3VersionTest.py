import boto3
import json
import datetime
#create the low level functional client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
iam = boto3.resource('iam')
sts = boto3.client('sts')
aws_lambda = boto3.client('lambda')


bucket_name = 'generatedbucket'
bucket_file_name = "first_data.json"
local_file_name = "data.json"
sample_data = json.dumps({
    'name':'aditya',
    'work':'python',
    'skill':['python','html'],
    'currentAvg': 70
})

update_sample_data = json.dumps({
    'name':'aditya',
    'work':'python',
    'skill':['python','html','java','css'],
    'framework':['springMVC',"HibernateMVC","django","flask"],
    'currentAvg': 93
})
def lambda_handler(event,context):

    clientResponse = s3_client.list_buckets()
    # fetch the list of exiting bucket
    bucket = s3_resource.Bucket(bucket_name)

    #Enabling Bucketversioning
    versioning = s3_resource.BucketVersioning(bucket_name = bucket_name)
    bucket.Versioning().enable()

    # uplad one json file in s3 bucket
    put_object = s3_client.put_object(Bucket = bucket_name, Key = 'sample_data.json', Body = sample_data)

    # upload_file = s3_resource.meta.client.upload_file(Filename ='sample_data.json',Bucket =bucket_name,Key=bucket_file_name)
    print("S3 Put Object content = ", put_object)

    # update data and put it again
    updated_put_object = s3_client.put_object(Bucket = bucket_name, Key = 'sample_data.json', Body = update_sample_data)

    # upload_file = s3_resource.meta.client.upload_file(Filename ='sample_data.json',Bucket =bucket_name,Key=bucket_file_name)
    print("S3 Put Object content update = ", updated_put_object)

    #get all versions
    all_Versions = s3_client.list_object_versions(Bucket=bucket_name)
    for ver in all_Versions:
        print("\n")
        # res = s3_client.get_object()
        print("Versions = ",ver)
        print("\n")
        
    
    # Print the bucket names one by one
    print('Printing bucket names...')
    bucketList = []
    for bucket in clientResponse['Buckets']:
        bucketList.append(bucket["Name"])
        print(f'Bucket Name: {bucket["Name"]}')

    return {
        "statusCode": 200,
        "body": json.dumps(
            {"message": bucketList,
             "bucket":bucket,
             # "object":object_version,
             "put object":put_object,
             "updated put object":updated_put_object,
             # "updated object version":new_object_version
             }
        )
    }




#Create bucket syntax
# bucket = s3_client.create_bucket(
# ACL = 'private' | 'public-read' | 'public-read-write' | 'authenticated-read',
# Bucket = bucket_name ,
# CreateBucketConfiguration = {
#     'LocationConstraint': s3_resource.meta.client.meta.region_name
# }
# CreateBucketConfiguration = {
#     'LocationConstraint': 'ap-south-1'
# },
# GrantFullControl = 'string',
# GrantRead = 'string',
# GrantReadACP = 'string',
# GrantWrite = 'string',
# GrantWriteACP = 'string',
# ObjectLockEnabledForBucket = True | False
# )
