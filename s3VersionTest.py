import boto3
import json
import datetime

# create the low level functional client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
iam = boto3.resource('iam')
sts = boto3.client('sts')
aws_lambda = boto3.client('lambda')
bucket_name = 'generatedbucket'
bucket_file_name = "first_data.json"
local_file_name = "data.json"
sample_data = {
    'name': 'aditya',
    'work': 'python',
    'skill': ['python', 'html'],
    'currentAvg': 70
}

update_sample_data = {
    'name': 'aditya',
    'work': 'python',
    'skill': ['python', 'html', 'java', 'css'],
    'framework': ['springMVC', "HibernateMVC", "django", "flask"],
    'currentAvg': 93
}


def create_bucket():
    # Create bucket syntax
    bucket = s3_client.create_bucket(Bucket = bucket_name)
    return bucket

def bucket_list():
    clientResponse = s3_client.list_buckets()
    # Print the bucket names one by one
    print('Printing bucket names...')
    bucketList = []
    for bucket in clientResponse['Buckets']:
        bucketList.append(bucket["Name"])
        print(f'Bucket Name: {bucket["Name"]}')
    response =  bucketList
    return response

def current_bucket():
    # fetch the list of exiting bucket
    bucket = s3_resource.Bucket(bucket_name)
    return bucket

def bucket_Version():
    # Enabling Bucketversioning
    versioning = s3_resource.BucketVersioning(bucket_name = bucket_name)
    current_bucket().Versioning().enable()

def put_object(bucket_name,key,body):
    object = s3_client.put_object(Bucket = bucket_name, Key = key, Body = body)
    return object

def get_all_version():
    # get all versions
    all_Versions = s3_client.list_object_versions(Bucket = bucket_name)
    for ver in all_Versions:
        print("\n")
        # res = s3_client.get_object()
        print("Versions = ", ver)
        print("\n")
    return all_Versions

def lambda_handler(event, context):
    # fetch the list of exiting bucket
    bucket =current_bucket()

    # Enabling Bucketversioning
    bucket_Version()

    # uplad one json file in s3 bucket
    first_object = put_object(bucket_name = bucket_name, key = 'sample_data.json', body = json.dumps(sample_data))

    # update data and put it again
    updated_object = put_object(bucket_name = bucket_name, key = 'sample_data.json',
                                         body = json.dumps(update_sample_data))
    return {
        "statusCode": 200,
        "body":"Version Updated!!!!!"        
    }
