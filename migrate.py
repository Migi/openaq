import json
import boto3
import mysql.connector
import dateutil
import pandas as pd
import io
import datetime

conn = mysql.connector.connect(
    host = "rds-openaq-michiel.cnyif0bzxpjl.eu-west-1.rds.amazonaws.com",
    user = "admin",
    password = "bctlxRPOVspfUTqeDiHL",
    database = "db_openaq_michiel"
)
cursor = conn.cursor()

df = pd.read_sql_query("SELECT * FROM sqs_messages WHERE country = 'BE' ORDER BY date_utc ASC", conn, index_col="id")

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
    print(query)
    print(row)
    cursor.execute(query, row)

for index, message in df.iterrows():
    row = {}
    row["date_utc"] = message["date_utc"]
    row["date_local"] = message["date_local"]
    
    row["latitude"] = message["latitude"]
    row["longitude"] = message["longitude"]

    for key in ["parameter", "value", "unit", "location", "country"]:
        row[key] = message[key]
    
    row["source_name"] = message["source_name"]
    row["source_type"] = message["source_type"]

    if "city" in message:
        row["city"] = message["city"]

    if "mobile" in message:
        row["mobile"] = message["mobile"]
    
    if "averaging_period_value" in message:
        row["averaging_period_value"] = message["averaging_period_value"]
        row["averaging_period_unit"] = message["averaging_period_unit"]

    # Insert this measurement into the database
    
    select_query = "SELECT `id` FROM `measurements` WHERE `date_utc` = %s AND `parameter` = %s AND `latitude` = %s AND `longitude` = %s"
    cursor.execute(select_query, (row["date_utc"], row["parameter"], row["latitude"], row["longitude"]))
    if cursor.fetchone() is not None:
        print("Already have this measurement. Ignoring...")
        continue

    insert_row("measurements", row, cursor)
    measurement_id = cursor.lastrowid
    
    # Insert the attributions (if any)
    select_query = "SELECT id, measurement_id, name, url FROM `sqs_attributions` WHERE `measurement_id` = %s"
    cursor.execute(select_query, (index, ))
    attribution_row = cursor.fetchone()
    assert attribution_row is not None
    while attribution_row is not None:
        attr_row = {}
        attr_row["measurement_id"] = measurement_id
        attr_row["name"] = attribution_row[2]
        attr_row["url"] = attribution_row[3]
        insert_row("attributions", attr_row, cursor)
        attribution_row = cursor.fetchone()
    
    conn.commit()

print("done.")