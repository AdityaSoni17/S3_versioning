import boto3
import json
import datetime

class S3VersionTest:
    def __init__(self):
        # create the low level functional client
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.iam = boto3.resource('iam')
        self.sts = boto3.client('sts')
        self.aws_lambda = boto3.client('lambda')
        self.bucket_name = 'generatedbucket'
        self.bucket_file_name = "first_data.json"
        self.local_file_name = "data.json"
        self.sample_data = json.dumps({
            'name': 'aditya',
            'work': 'python',
            'skill': ['python', 'html'],
            'currentAvg': 70
        })

        self.update_sample_data = json.dumps({
            'name': 'aditya',
            'work': 'python',
            'skill': ['python', 'html', 'java', 'css'],
            'framework': ['springMVC', "HibernateMVC", "django", "flask"],
            'currentAvg': 93
        })

    def create_bucket(self):
        # Create bucket syntax
        bucket = self.s3_client.create_bucket(Bucket = self.bucket_name)
        return bucket

    def bucket_list(self):
        clientResponse = self.s3_client.list_buckets()
        # Print the bucket names one by one
        print('Printing bucket names...')
        bucketList = []
        for bucket in clientResponse['Buckets']:
            bucketList.append(bucket["Name"])
            print(f'Bucket Name: {bucket["Name"]}')
        response = {
            "status" : 200,
            "message": bucketList,
        }
        return response

    def current_bucket(self):
        # fetch the list of exiting bucket
        bucket = self.s3_resource.Bucket(self.bucket_name)
        return bucket

    def bucket_Version(self):
        # Enabling Bucketversioning
        versioning = self.s3_resource.BucketVersioning(bucket_name = self.bucket_name)
        self.current_bucket().Versioning().enable()

    def put_object(self,bucket_name,key,body):
        object = self.s3_client.put_object(Bucket = bucket_name, Key = key, Body = body)
        return object

    def get_all_version(self):
        # get all versions
        all_Versions = self.s3_client.list_object_versions(Bucket = self.bucket_name)
        for ver in all_Versions:
            print("\n")
            # res = s3_client.get_object()
            print("Versions = ", ver)
            print("\n")
        return all_Versions

    def lambda_handler(self,event, context):
        # fetch the list of exiting bucket
        bucket =self.current_bucket()

        # Enabling Bucketversioning
        self.bucket_Version()

        # uplad one json file in s3 bucket
        first_object = self.put_object(bucket_name = self.bucket_name, key = 'sample_data.json', body = self.sample_data)

        # update data and put it again
        updated_object = self.put_object(bucket_name = self.bucket_name, key = 'sample_data.json',
                                         body = self.update_sample_data)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {

                    "bucket": bucket,
                    "first object": first_object,
                    "updated object": updated_object,
                }
            )
        }
