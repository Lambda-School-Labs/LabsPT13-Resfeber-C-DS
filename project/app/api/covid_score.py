import os
import psycopg2
from fastapi import APIRouter, HTTPException
import pandas as pd
import plotly.express as px
from sodapy import Socrata
from datetime import datetime, date, timedelta
import re

router = APIRouter()

# Population by US state
state_pop = {
    "AK": 731545,
    "AL": 4903185,
    "AR": 3017825,
    "AZ": 7278717,
    "CA": 39512223,
    "CO": 5758736,
    "CT": 3565287,
    "DC": 705749,
    "DE": 973764,
    "FL": 21477737,
    "GA": 10617423,
    "HI": 1415872,
    "IA": 3155070,
    "ID": 1787065,
    "IL": 12671821,
    "IN": 6732219,
    "KS": 2913314,
    "KY": 4467673,
    "LA": 4648794,
    "MA": 6949503,
    "MD": 6045680,
    "MI": 9986857,
    "ME": 1344212,
    "MN": 5639632,
    "MO": 6137428,
    "MS": 2976149,
    "MT": 1068778,
    "NC": 10488084,
    "ND": 762062,
    "NE": 1934408,
    "NH": 1359711,
    "NJ": 8882190,
    "NM": 2096829,
    "NV": 3080156,
    "NY": 19453561,
    "OH": 11689100,
    "OK": 3956971,
    "OR": 4217737,
    "PA": 12801989,
    "RI": 1059361,
    "SC": 5148714,
    "SD": 884659,
    "TN": 6833174,
    "TX": 28995881,
    "UT": 3205958,
    "VA": 8535519,
    "VT": 623989,
    "WA": 7614893,
    "WI": 5822434,
    "WV": 1792147,
    "WY": 578759}

# Return Covid data for a given state and date from the database
def req_cdc_covid_dat(ste, dte):
    """
    req_cdc_covid_dat return Covid data for a given state and date
    from the backend data science database

    Parameters:
        "state": the targeted state (e.g. "CA")
        "date":  the string date of the covid data requested (format: YYYY-MM-DD)

    Returns:
        "ok":       boolean indicating a successful request
        "new_case": new cases logged in the API response
        "data":     copy of the CDC API response
        "error":    error message (if applicable)  
    """
    # Define a return object
    ret_dict = {
        "ok": False,
        "new_case":   None,
        "data":       None,
        "error":      "an error occurred"
    }

    # Read database configuration from environment variables
    DS_DB_HOST      = os.getenv("DS_DB_HOST")
    DS_DB_PORT      = os.getenv("DS_DB_PORT")
    DS_DB_NAME      = os.getenv("DS_DB_NAME")
    DS_DB_USER      = os.getenv("DS_DB_USER")
    DS_DB_PASSWORD  = os.getenv("DS_DB_PASSWORD")

    # Missing database configuration?
    if len(DS_DB_HOST) == 0   or        \
       len(DS_DB_NAME) == 0     or      \
       len(DS_DB_USER) == 0     or      \
       len(DS_DB_PASSWORD) == 0:
       # missing config
       ret_dict["error"] = "missing database configuration"

       return ret_dict

    # Configure a connection object
    connection = psycopg2.connect(user =        DS_DB_USER,
                                  password =    DS_DB_PASSWORD,
                                  host =        DS_DB_HOST,
                                  port =        DS_DB_PORT,
                                  database =    DS_DB_NAME)

    # Test the database connection
    try:
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(f"INFO: Connecting to the database with these credentials: {connection.get_dsn_parameters()}\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print(f"INFO: Successfully connected to the database: {record}\n")

    except (Exception, psycopg2.Error) as error :
        print (f"ERROR: error while connecting to PostgreSQL: {error}")
        ret_dict["error"] = "error connecting to the database"
        return ret_dict

    # Do we have a valid state value?
    if ste not in state_pop:
        # invalid state parameter
        ret_dict["error"] = "invalid state parameter: " + ste
        return ret_dict

    # Do we have a valid date value?
    if re.match(r"^\d{4}\-\d{2}\-\d{2}", dte) == None:
        # dte parameter value is not valid
        ret_dict["error"] = "invalid date parameter: " + dte
        return ret_dict

    # Query the database
    sql = "SELECT * FROM state_covid_stats WHERE state = %s AND date = %s"
    cvd_dict = {}
    try:
        cursor.execute(sql, )
        cvd_row = cursor.fetchone()
        cvd_dict["new_case"] = cvd_row[3]
        cvd_dict["data"]     = cvd_row[5]

    except (Exception, psycopg2.Error) as error:
        ret_dict["error"] = f"error fetching covid data for state: {ste} date: {dte}"
        return ret_dict
    
    # Return results
    ret_dict["ok"]       = True
    ret_dict["error"]    = None
    ret_dict["new_case"] = cvd_dict[0]["new_case"]
    ret_dict["data"]     = cvd_dict[0]
    return ret_dict

# Return Covid data for a given state and date from the database
def req_cdc_covid_dat(ste, dte):
    """
    req_cdc_covid_dat return Covid data for a given state and date
    from the backend data science database

    Parameters:
        "state": the targeted state (e.g. "CA")
        "date":  the string date of the covid data requested (format: YYYY-MM-DD)

    Returns:
        "ok":       boolean indicating a successful request
        "new_case": new cases logged in the API response
        "data":     copy of the CDC API response
        "error":    error message (if applicable)  
    """
    # Define a return object
    ret_dict = {
        "ok":         False,
        "new_case":   None,
        "data":       None,
        "error":      "an error occurred"
    }

    # Read database configuration from environment variables
    DS_DB_HOST      = os.getenv("DS_DB_HOST")
    DS_DB_PORT      = os.getenv("DS_DB_PORT")
    DS_DB_NAME      = os.getenv("DS_DB_NAME")
    DS_DB_USER      = os.getenv("DS_DB_USER")
    DS_DB_PASSWORD  = os.getenv("DS_DB_PASSWORD")

    # Missing database configuration?
    if len(DS_DB_HOST) == 0   or        \
       len(DS_DB_NAME) == 0     or      \
       len(DS_DB_USER) == 0     or      \
       len(DS_DB_PASSWORD) == 0:
       # missing config
       ret_dict["error"] = "missing database configuration"

       return ret_dict

    # Configure a connection object
    connection = psycopg2.connect(user =        DS_DB_USER,
                                  password =    DS_DB_PASSWORD,
                                  host =        DS_DB_HOST,
                                  port =        DS_DB_PORT,
                                  database =    DS_DB_NAME)

    # Test the database connection
    try:
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(f"INFO: Connecting to the database with these credentials: {connection.get_dsn_parameters()}\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print(f"INFO: Successfully connected to the database: {record}\n")

    except (Exception, psycopg2.Error) as error :
        print (f"ERROR: error while connecting to PostgreSQL: {error}")
        ret_dict["error"] = "error connecting to the database"
        return ret_dict

    # Do we have a valid state value?
    if ste not in state_pop:
        # invalid state parameter
        ret_dict["error"] = "invalid state parameter: " + ste
        return ret_dict

    # Do we have a valid date value?
    if re.match(r"^\d{4}\-\d{2}\-\d{2}", dte) == None:
        # dte parameter value is not valid
        ret_dict["error"] = "invalid date parameter: " + dte
        return ret_dict

    # Query the database
    sql = "SELECT * FROM state_covid_stats WHERE state = %s AND date = %s"
    cvd_dict = {}
    try:
        cursor.execute(sql, (ste, dte))
        cvd_row = cursor.fetchone()
        cvd_dict["new_case"] = cvd_row[3]
        cvd_dict["data"]     = cvd_row[5]

    except (Exception, psycopg2.Error) as error:
        ret_dict["error"] = f"error fetching covid data for state: {ste} date: {dte}"
        return ret_dict
    
    # Return results
    ret_dict["ok"]       = True
    ret_dict["error"]    = None
    ret_dict["new_case"] = cvd_dict["new_case"]
    ret_dict["data"]     = cvd_dict["data"]
    return ret_dict

# Generate score calculations for every state
def calc_covid_deltas():
    ret_map = {}

    # Iterate through the set of states
    for st in state_pop.keys():
        # Generate the covid score
        fn_map = gen_covid_score(st)

        # Was there an error the current function call
        if not fn_map["ok"]:
            ret_map[st] = None
            continue

        # Assign the calculated delta value
        ret_map[st] = fn_map["delta"]

    # Return the dictionary mapping states (e.g "GA") to their 
    # delta calculation (e.g. 0.041)
    return ret_map

# Route to return a covid score to the caller
@router.get('/covid_score_state/{state}')
async def get_covid_score_state(state: str):
    # Generate a covid score
    ret_dict = gen_covid_score(state)

    # Any errors generating the covid score
    if not ret_dict["ok"]:
        # an error occurred generating the covid score
        raise HTTPException(status_code=500, detail="an error occurred: " + ret_dict["error"])

    return ret_dict
