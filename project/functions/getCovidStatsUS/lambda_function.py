import os
import sys
import json
import psycopg2
from psycopg2.extras import execute_values
from sodapy import Socrata
from datetime import datetime, date

def lambda_handler(event, context):
    # Set up working variables
    ret_dict = {'statusCode': 200, 'body': json.dumps('default message')}
    
    # Grab environment variables
    MY_APP_TOKEN = os.getenv("COVID_API")
    if len(MY_APP_TOKEN) == 0:
        # error: missing API key
        print("ERROR: missing API key", file=sys.stderr)
        
        ret_dict['statusCode'] = 401
        ret_dict['body'] = json.dumps('error: missing API key')
        return ret_dict

    PG_DB_PASSWORD = os.getenv("PG_DB_PASSWORD")
    if len(PG_DB_PASSWORD) == 0:
        # error: missing API key
        print("ERROR: missing Postgres database password", file=sys.stderr)
        
        ret_dict['statusCode'] = 400
        ret_dict['body'] = json.dumps('error: missing Postgres database password')
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
        ret_dict['statusCode'] = 500
        ret_dict['body'] = json.dumps('error connecting to the Postgres database')
        return ret_dict

    # Validate the date parameter (e.g. "2020-11-01")
    req_date = event["date"]
    if len(req_date) != 10:
        # error: invalid date parameter
        print("error: invalid date parameter", file=sys.stderr)
        
        ret_dict['statusCode'] = 400
        ret_dict['body'] = json.dumps('error: invalid date parameter')
        return ret_dict
    
    # Construct a date/time string
    date_time = req_date + "T00:00:00.000"
    
    # Stand up an API client object
    client = Socrata('data.cdc.gov', MY_APP_TOKEN)
    # Fetch data from the API
    results = client.get("9mfq-cb36", submission_date=date_time)

    # Construct an Insert SQL statement
    sql = "INSERT INTO state_covid_stats (state, date, new_cases, time_created, covid_api_rsp, notes) " + \
          "VALUES %s ON CONFLICT (state, date) DO NOTHING"
    
    # Iterate through the results
    print(f"INFO: Number data points returned by the CDC API: {len(results)}", file=sys.stderr)
    vals_arr = []
    for rslt in results:
        tmp_tpl = (
            rslt["state"],
            req_date,
            int(float(rslt["new_case"])),
            datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            json.dumps(rslt),
            ""
        )

        # append this "row" dictionary to our running array
        vals_arr.append(tmp_tpl)

    # Do we have any data (rows) to insert into the database?
    if len(vals_arr) != 0:
        # yes: attempt the database insert
        try:
            # stand up a db cursor
            cursor = connection.cursor()
            # execute the insert for multiple rows
            execute_values(cursor, sql, vals_arr)
            # commit the db changes
            connection.commit()
            # close the db connection
            connection.close()

        except (Exception, psycopg2.Error) as error :
            print (f"ERROR: error occurred attempting to insert rows into the database: {error}", file=sys.stderr)
            ret_dict['statusCode'] = 500
            ret_dict['body'] = json.dumps('error connecting to the Postgres database')
            return ret_dict

    else:
        # no data to insert
            print (f"INFO: no Covid API data returned.  Nothing to insert into the database", file=sys.stderr)
            ret_dict['statusCode'] = 204
            ret_dict['body'] = json.dumps('no Covid API data returned; nothing to insert into the database')
            return ret_dict

    # Function completed processing
    print (f"INFO: finished inserting: {len(vals_arr)} rows", file=sys.stderr)
    ret_dict['statusCode'] = 200
    ret_dict['body'] = json.dumps(f"INFO: finished inserting: {len(vals_arr)} rows")
    return ret_dict

