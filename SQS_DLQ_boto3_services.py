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
DLQ_Url = ''

# message
static_message = {
    "data": [
        {
            "item": "1"
        },
        {
            "item": "2"
        },
        {
            "item": "3"
        }
    ]
}


# sample_message = {
#     "data": {
#         "item1": "1",
#         "item2": "2",
#         "item3": "3"
#     }
# }

# data = dict(static_message)

# Creating SQS with some parameters and enable DLQ
def create_SQS_fifo():
    createQueue = sqs_resource.create_queue(
        QueueName = 'getDeadMsg.fifo',  # For FIFO queue it must contains 'fifo' suffix at end of name.
        Attributes = {
            'RedrivePolicy': json.dumps({
                "deadLetterTargetArn": DEAD_LETTER_QUEUE_ARN,
                "maxReceiveCount": "2",
            }),
            "FifoQueue": "true",
        },
        # tags = {
        #     'string': 'string'
        # }
    )
    return createQueue


# Creating dlq which are help to provide relationship with main SQS with help of aws arn
def create_dlq_sqs_fifo():
    createDlq = sqs_resource.create_queue(
        QueueName = 'waitingQueue.fifo',
        Attributes = {
            "FifoQueue": "true",
        }
    )
    return createDlq


# getting queue name list
def search_queue(QueueNamePrifix):  # (QueueNamePrifix,NextToken,MaxResults): all optional to search
    result = sqs_client.list_queues(
        QueueNamePrefix = QueueNamePrifix)  # (QueueNamePrefix=QueueNamePrifix,NextToken=NextToken,MaxResults=MaxResults)
    return result


# list all the queues
def list_queues():
    return sqs_client.list_queues()


# deletin queue
def delete_queue():
    return sqs_client.delete_queue(
        QueueUrl = 'main queue url'
    )


def get_queue_url(queuename):
    return sqs_client.get_queue_url(QueueName = queuename)


# sending message to sqs
def send_message_to_queue():
    response = sqs_client.send_message(
        QueueUrl = MAIN_QUEUE_URL,
        MessageAttributes = {
            'item1': {
                'DataType': 'String',
                'StringValue': '1'
            },
            'item2': {
                'DataType': 'String',
                'StringValue': '2'
            },
            'item3': {
                'DataType': 'String',
                'StringValue': '3'
            }
        },
        MessageBody = "The msg send successfully!!!!!!!!",
        # MessageDeduplicationId = 'duplicationTest',
        MessageGroupId = 'messageGroup1'
    )
    return response


# sending msg using barches
def send_batch_messages_to_queue():
    response = sqs_client.send_message_batch(
        QueueUrl = MAIN_QUEUE_URL,
        Entries = static_message
    )


# recieving msges
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


def delete_message_from_queue(receipt_handle):
    sqs_client.delete_message(
        QueueUrl = 'main queue url',
        ReceiptHandle = receipt_handle
    )


def purge_queue():
    return sqs_client().purge_queue(
        QueueUrl = 'MAIN_QUEUE_URL'
    )


def lambda_handler(event, context):
    # creating dlq
    # dlq_queue = create_dlq_sqs_fifo()
    dlq_queue = sqs_resource.get_queue_by_name(QueueName = 'waitingQueue.fifo')

    # main queue creating as standard type
    # main_queue = create_SQS()

    # Main queue of fifo type
    # main_queue= create_SQS_fifo()
    main_queue = sqs_resource.get_queue_by_name(QueueName = 'getDeadMsg.fifo')

    # send message
    messageSend1 = send_message_to_queue()
    messageSend2 = send_message_to_queue()
    messageSend3 = send_message_to_queue()
    messageSend4 = send_message_to_queue()


    msg_receive = receive_messages_in_queue()


    #response print
    print("\nDLQ = \n", dlq_queue)
    print("\nmain queue = \n", main_queue)
    print("\nMsg receive =\n ",msg_receive)
    print("\nmessage response 1 to 4 = \n", messageSend1, "\n", messageSend2, "\n", messageSend3, "\n", messageSend4)

    # get single message during queue
    # for msg in static_message["data"]:

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                # "DLQ Name" :dlq_queue,
                #   "message response": messageSend
            }
        )

    }
