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


# Creating SQS with some parameters and enable DLQ
def create_SQS_fifo():
    createQueue = sqs_resource.create_queue(
        QueueName = 'getDeadMsg.fifo',  # For FIFO queue it must contains 'fifo' suffix at end of name.
        Attributes = {
            'RedrivePolicy': json.dumps({
                "deadLetterTargetArn": DEAD_LETTER_QUEUE_ARN,
                "maxReceiveCount": "10",
            }),
            "FifoQueue": "true",
        },
        # tags = {
        #     'string': 'string'
        # }
    )
    return createQueue


# sending msg using barches and here we are able to send any number of 
def send_batch_messages_to_queue():
    response = sqs_client.send_message_batch(
        QueueUrl = MAIN_QUEUE_URL,
        Entries =[
            {
                "Id":"item1",
                "MessageBody":"1",
                "MessageGroupId" :'messageGroup1'
             },
            {
                "Id": "item2",
                "MessageBody": "2",
                "MessageGroupId": 'messageGroup1'
            },
            {
                "Id": "item3",
                "MessageBody": "3",
                "MessageGroupId": 'messageGroup1'
            },            
        ] 
    )
    return response
    

def lambda_handler(event, context):
    # TODO implement
    print("\nBatch Message Queue = \n ",send_batch_messages_to_queue(),"\nThis message is sending to SQS")
    return {
        'statusCode': 200,
        'body': json.dumps("Data Send to Main Fifo SQS")
    }
