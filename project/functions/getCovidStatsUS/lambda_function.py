import os
import sys
import json
import psycopg2
from sodapy import Socrata
from datetime import date

def lambda_handler(event, context):
    # Set up working variables
    ret_dict = {'statusCode': 200, 'body': 'default message'}
    
    # Grab environment variables
    MY_APP_TOKEN = os.getenv("COVID_API")
    if len(MY_APP_TOKEN) == 0:
        # error: missing API key
        print("error: missing API key", file=sys.stderr)
        
        ret_dict['statusCode'] = 401
        ret_dict['body'] = 'error: missing API key'
        return ret_dict

    PG_DB_PASSWORD = os.getenv("PG_DB_PASSWORD")
    if len(PG_DB_PASSWORD) == 0:
        # error: missing API key
        print("error: missing Postgres database password", file=sys.stderr)
        
        ret_dict['statusCode'] = 401
        ret_dict['body'] = 'error: missing Postgres database password'
        return ret_dict

    # Configure a connection object
    connection = psycopg2.connect(user = "postgres",
                                  password = PG_DB_PASSWORD,
                                  host = "labspt13-res.cguagfajgk6j.us-west-1.rds.amazonaws.com",
                                  port = "5432",
                                  database = "resfeber")

    # Test the database connection
    try:
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(f"{connection.get_dsn_parameters()}\n", file=sys.stderr)

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print(f"You are connected to - {record}\n", file=sys.stderr)

    except (Exception, psycopg2.Error) as error :
        print (f"Error while connecting to PostgreSQL: {error}", file=sys.stderr)
        return

    # Validate the date parameter (e.g. "2020-11-01")
    req_date = event["date"]
    if len(req_date) != 10:
        # error: invalid date parameter
        print("error: invalid date parameter", file=sys.stderr)
        
        ret_dict['statusCode'] = 401
        ret_dict['body'] = 'error: invalid date parameter'
        return ret_dict
    
    # Construct a date/time string
    date_time = req_date + "T00:00:00.000"
    
    # Stand up an API client object
    client = Socrata('data.cdc.gov', MY_APP_TOKEN)
    # Fetch data from the API
    results = client.get("9mfq-cb36", submission_date=date_time)
    
    # Iterate through the results
    print(f"Number of elements in results: {len(results)}", file=sys.stderr)
    for rslt in results:
        print(rslt, file=sys.stderr)
    
    return {
        'statusCode': 200,
        'body': json.dumps("my message to you")
    }
