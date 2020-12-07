import boto3
import json
import datetime

# Clients and Resources initialization
sqs_client = boto3.client('sqs')
sqs_resource = boto3.resource('sqs')

# #Dead letter queue arn
DEAD_LETTER_QUEUE_ARN = 'arn:aws:sqs:ap-south-1:288154355795:waitingQueue.fifo'  # here we need to assign manually dead letter queue arn
# DEAD_LETTER_MAIN_QUEUE= '' #we need to define ARN of dead letter queue here

#main queue url
MAIN_QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/288154355795/getDeadMsg.fifo'

#DLQ Url
DLQ_Url = 'https://sqs.ap-south-1.amazonaws.com/288154355795/waitingQueue.fifo'

print(sqs_client,sqs_resource)


def receive_messages_in_queue():
    return sqs_client.receive_message(
        QueueUrl = MAIN_QUEUE_URL,
        # AttributeNames= '',
        MaxNumberOfMessages = 10,
        MessageAttributeNames = [
            'All'
        ],
        VisibilityTimeout= 0,
        WaitTimeSeconds = 0
    )
def lambda_handler(event, context):
    # TODO implement
    
    response = receive_messages_in_queue()
    print("Msg = ",response)
    return {
            'statusCode': 200,
            'body': json.dumps('message received!!')
        }
