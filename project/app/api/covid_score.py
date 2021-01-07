import os
import re

import psycopg2
import datetime
from datetime import datetime, timedelta
import pytz
import json

from app.api.constants import STATE_POP

# Return Covid data for a given state and date from the database
def req_cdc_covid_dat(db_conn, ste, dte):
    """
    req_cdc_covid_dat return Covid data for a given state and date
    from the backend data science database

    Parameters:
        "db_conn":  database connection object
        "ste":      the targeted state (e.g. "CA")
        "dte":      the string date of the covid data requested (format: YYYY-MM-DD)

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

    # Validate the passed state parameter
    err = val_state(ste)
    if err is not None:
        ret_dict["error"] = err
        return ret_dict

    # Validate the passed date parameter
    err = val_date(dte)
    if err is not None:
        ret_dict["error"] = err
        return ret_dict

    # Query the database
    sql = "SELECT * FROM state_covid_stats WHERE state = %s AND date = %s"
    cvd_dict = {}
    try:
        cursor = db_conn.cursor()
        cursor.execute(sql, (ste, dte))
        cvd_row = cursor.fetchone()
        cvd_dict["new_case"] = cvd_row[3]
        cvd_dict["data"]     = cvd_row[5]

    except (Exception, psycopg2.Error) as error:
        ret_dict["error"] = f"error fetching covid data for state: {ste} date: {dte} - {error}"
        return ret_dict
    
    # Return results
    ret_dict["ok"]       = True
    ret_dict["error"]    = None
    ret_dict["new_case"] = cvd_dict["new_case"]
    ret_dict["data"]     = cvd_dict["data"]
    return ret_dict

# Generate a Covid "score" for a given state
def gen_covid_score(db_conn, ste):
    """
    gen_covid_score generates a covid score using
    CDC covid data obtained via a call to
    the req_cdc_covid_dat function 

    Parameters:
        "db_conn":  database connection object
        "ste":      the targeted state (e.g. "CA")

    Returns:
        "ok":       boolean indicating a successful request
        "score":    the state's covid score (0,1,2)
        "color":    the state's covid score as a color ("green", "yellow", "red")
        "data":     CDC covid API data
        "error":    error message (if applicable)
    """
    # Define a return object
    ret_dict = {
        "ok":    False,
        "score": None,
        "delta": None,
        "color": None,
        "data":  None,
        "error": "an error occurred"
    }

    # Validate the passed state parameter
    err = val_state(ste)
    if err is not None:
        ret_dict["error"] = err
        return ret_dict

    # Read configuration into variables from environment variables
    INT_START         = int(os.getenv("INT_START"))
    INT_END           = int(os.getenv("INT_END"))
    THRESHOLD_LOW     = float(os.getenv("THRESHOLD_LOW"))
    THRESHOLD_HIGH    = float(os.getenv("THRESHOLD_HIGH"))

    # Generate dates for the start and end of the time interval
    req_try = True
    err_ctr = 0
    int_st  = INT_START
    int_ed  = INT_END
    while (req_try and err_ctr < 5):
        req_try = False  # Assume we get an error calling the Covid API

        # Generate the start and end date interval
        intv_start = datetime.strftime(datetime.today() - timedelta(days=int_st), "%Y-%m-%d")
        intv_end   = datetime.strftime(datetime.today() - timedelta(days=int_ed), "%Y-%m-%d")

        # Fetch new cases associated with the interval dates
        intv_start_ncases = req_cdc_covid_dat(db_conn, ste, intv_start)
        intv_end_ncases   = req_cdc_covid_dat(db_conn, ste, intv_end)

        # Error fetching from the Covid API?
        if not intv_start_ncases["ok"] or not intv_end_ncases["ok"]:
            # Error occurred, adjust look up period by 1 day and try again
            req_try = True          # error occurred keep trying
            err_ctr = err_ctr + 1   # increment the error try counter
            int_st  = int_st  + 1   # increment the start date lookback value
            int_ed  = int_ed  + 1   # increment the end date lookback value
            print("INFO: Covid API request failed. Attempting retry")

    # Outstanding error calling the Covid API?
    if not intv_start_ncases["ok"] or not intv_end_ncases["ok"]:
        # yes: can't get Covid data at this time
        ret_dict["error"] = "error fetching Covid data; no data at this time"
        return ret_dict

    # Calculate new cases per 100,000 people
    start_ncases_p100k = float(intv_start_ncases["new_case"])/float(STATE_POP[ste])*100000
    end_ncases_p100k   = float(intv_end_ncases["new_case"])/float(STATE_POP[ste])*100000

    # Check for division by zero (a really small start_ncases_p100k value)
    # Calculate the delta in new cases per 100000
    if start_ncases_p100k < .015:
        # Assume this value of start_ncases_p100k is essentially zero
        delta = end_ncases_p100k
    else:
        delta = (end_ncases_p100k - start_ncases_p100k)/start_ncases_p100k

    ret_dict["delta"] = delta

    # Generate a covid score
    scr_dict          = calc_covid_score(delta, THRESHOLD_LOW, THRESHOLD_HIGH)
    ret_dict["ok"]    = scr_dict["ok"]
    ret_dict["score"] = scr_dict["score"]
    ret_dict["color"] = scr_dict["color"]
    ret_dict["error"] = None
      
    # Assign the most recent CDC Covid API data
    ret_dict["data"] = {"start": intv_start_ncases["data"], "end": intv_end_ncases["data"]}

    # Return 
    return ret_dict

# Generate score calculations for every state
def calc_covid_deltas(db_conn):
    """
    calc_covid_deltas generates the numeric delta covid change
    for each/all states (rather than the covid score)

    Parameters:
        "db_conn":  database connection object

    Returns a dictionary with key/values:
        "state": numeric delta covid change (e.g. 0.057, -0.032)
    """
    ret_map = {}

    # Iterate through the set of states
    for st in STATE_POP.keys():
        # Generate the covid score
        fn_map = gen_covid_score(db_conn, st)

        # Was there an error the current function call
        if not fn_map["ok"]:
            ret_map[st] = None
            continue

        # Assign the calculated delta value
        ret_map[st] = fn_map["delta"]

    # Return the dictionary mapping states (e.g "GA") to their 
    # delta calculation (e.g. 0.041)
    return ret_map

# Validate the state value
def val_state(ste):
    """
    Validate the passed value as a US state (e.g. 'CO')
    """
    # Do we have a valid state value?
    if ste not in STATE_POP:
        # invalid state parameter
        return "invalid state parameter: " + ste

    return None

# Validate the date value
def val_date(dte):
    """
    Validate the passed value as a date (e.g. '2021-01-01')
    """
    # Do we have a valid date value?
    if re.match(r"^\d{4}\-\d{2}\-\d{2}$", dte) == None:
        # dte parameter value is not valid
        return "invalid date parameter: " + dte

    return None

# Generate a covid "score"
def calc_covid_score(delta, lo, hi):
    """
    Calcuate a covid score given the numeric change
    and both a low and high scoring threshold
    """
    ret_dict = {
        "ok": False,
        "score": None,
        "color": None
    }

    if delta < lo:
        ret_dict["ok"] = True
        ret_dict["score"] = 0
        ret_dict["color"] = "green"
    elif delta < hi:
        ret_dict["ok"] = True
        ret_dict["score"] = 1
        ret_dict["color"] = "yellow"
    else:
        ret_dict["ok"] = True
        ret_dict["score"] = 2
        ret_dict["color"] = "red"

    return ret_dict

# Fetch today's calc_covid_deltas from the database if it exists
# If the database record does not exist, call the calc_covid_deltas function
# and generate the covid deltas values for every state
def calc_covid_deltas_db(db_conn):
    """
    calc_covid_deltas_db returns the covid case deltas over an interval (defined by env vars)
    for each US state

    The logic attempts to find a pre-generated value from the database.  If it does not exist
    then the state delta values are calculated using daily state level covid data resident
    in the database by calling the calc_covid_deltas function

    Parameters:
        "db_conn":  database connection object

    Returns a dictionary with key/values:
        "state": numeric delta covid change (e.g. 0.057, -0.032)
    """
    # Get today's date
    today_PT = datetime.now(pytz.timezone('US/Pacific')).strftime("%Y-%m-%d")

    # Query the database to see if we have `calc_covid_deltas` results saved to the database for today
    sql = "SELECT date, json_doc FROM state_covid_deltas_daily WHERE date = %s"
    cvd_dict = {}
    try:
        # Attempt the select query and grab the row's 'is_valid' and 'json_doc' 
        cursor = db_conn.cursor()
        qry_data = (today_PT, )
        cursor.execute(sql, qry_data)
        cvd_row = cursor.fetchone()
        cvd_dict["date"]     = cvd_row[0]
        cvd_dict["json_obj"] = cvd_row[1]

    except (Exception, psycopg2.Error) as error:
        # Error querying for calc_covid_deltas values stored in the database
        print(f"INFO: error querying pre-generated calc_covid_deltas values stored in the database. See: {error}")
        print("INFO: generating calc_covid_deltas values")
        return calc_covid_deltas(db_conn)

    # Have valid results. Return the results (dict object)
    return cvd_dict["json_obj"]
