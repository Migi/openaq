import boto3
import json

sqs_client = boto3.client('sqs')

messages = []

while True:
    resp = sqs_client.receive_message(
        QueueUrl='https://eu-west-1.queue.amazonaws.com/287820185021/openaq-michiel',
        AttributeNames=['All'],
        MaxNumberOfMessages=10
    )

    try:
        messages.extend(resp['Messages'])
    except KeyError:
        print("No more messages found. Total num messages downloaded: ", len(messages))
        break

    print("Num messsages so far: ", len(messages))

with open('sqs_messages.json', 'w') as outfile:
    json.dump(messages, outfile, indent=2)