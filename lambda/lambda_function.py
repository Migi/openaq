import json
import boto3
import mysql.connector
import dateutil

def insert_row(table_name, row, cursor):
    """
    insert_row() is a helper function that inserts the given dict as a row
    into the given table, where the keys in the dict are the column names.
    """
    keys = list(row.keys())
    query = "INSERT INTO `{}` (".format(table_name) \
        + ", ".join(["`"+key+"`" for key in keys]) \
        + ") VALUES (" \
        + ", ".join(["%("+key+")s" for key in keys]) \
        + ")"
    cursor.execute(query, row)

# Connect to the RDS database.
# (Note: I would use AWS Secrets Manager for the password, but I do not have the
# right permission (InterviewPolicy does not have secretsmanager:GetSecretValue).
conn = mysql.connector.connect(
    host = "rds-openaq-michiel.cnyif0bzxpjl.eu-west-1.rds.amazonaws.com",
    user = "admin",
    password = "bctlxRPOVspfUTqeDiHL",
    database = "db_openaq_michiel"
)
conn.autocommit = False
cursor = conn.cursor()

# Lambda function implementation:
def lambda_handler(event, context):
    for record in event['Records']:
        print(str(record))
        
        body = json.loads(record["body"])
        print(str(body))
        
        message_id = body["MessageId"]
        
        # Test if we have already processed this SQS message (specifically, this SNS ID):
        
        select_query = "SELECT `id` FROM `sqs_messages` WHERE `sqs_message_id` = %s"
        cursor.execute(select_query, (message_id, ))
        if cursor.fetchone() is not None:
            print("Already have this message ID. Ignoring...")
            continue
        
        # We have not seen this message ID yet, so construct the data to be inserted:
        
        message = json.loads(body["Message"])
        print(str(message))

        row = {}
        row["sqs_message_id"] = message_id
        row["date_utc"] = dateutil.parser.parse(message["date"]["utc"])
        row["date_local"] = message["date"]["local"]
        
        if "coordinates" in message:
            row["latitude"] = message["coordinates"]["latitude"]
            row["longitude"] = message["coordinates"]["longitude"]

        for key in ["parameter", "value", "unit", "location", "country"]:
            row[key] = message[key]
        
        row["source_name"] = message["sourceName"]
        row["source_type"] = message["sourceType"]
    
        if "city" in message:
            row["city"] = message["city"]
    
        if "mobile" in message:
            row["mobile"] = message["mobile"]
        
        if "averagingPeriod" in message:
            row["averaging_period_value"] = message["averagingPeriod"]["value"]
            row["averaging_period_unit"] = message["averagingPeriod"]["unit"]

        # Insert this measurement into the database
    
        insert_row("sqs_messages", row, cursor)
        measurement_id = cursor.lastrowid
        
        # Insert the attributions (if any)
    
        attributions = []
        if "attribution" in message:
            for attribution in message["attribution"]:
                attr_row = {}
                attr_row["measurement_id"] = measurement_id
                attr_row["name"] = attribution["name"]
                if "url" in attribution:
                    attr_row["url"] = attribution["url"]
                insert_row("sqs_attributions", attr_row, cursor)
        
        # We also store the data in a table measurements,
        # where (date_utc, parameter, longitude, latitude) is unique
        
        # If the message has no longitude/latitude, do not save it in this table
        
        if "coordinates" not in message:
            print("No coordinates in measurement. Ignoring...")
            
        # Test if we already have this (date_utc, parameter, longitude, latitude)

        select_query = "SELECT `id` FROM `measurements` WHERE `date_utc` = %s AND `parameter` = %s AND `latitude` = %s AND `longitude` = %s"
        cursor.execute(select_query, (row["date_utc"], row["parameter"], row["latitude"], row["longitude"]))
        if cursor.fetchone() is not None:
            print("Already have this measurement. Ignoring...")
            continue

        # We have not seen this measurement yet, so store it in the database

        del row["sqs_message_id"]
    
        insert_row("measurements", row, cursor)
        measurement_id = cursor.lastrowid

        # Also store the attributions

        attributions = []
        if "attribution" in message:
            for attribution in message["attribution"]:
                attr_row = {}
                attr_row["measurement_id"] = measurement_id
                attr_row["name"] = attribution["name"]
                if "url" in attribution:
                    attr_row["url"] = attribution["url"]
                insert_row("attributions", attr_row, cursor)
    
    conn.commit()
    print("lambda_handler() finished successfully")