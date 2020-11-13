from sodapy import Socrata

# Population by US state
state_pop = {
  "AK": 731545,
  "AL": 4903185,
  "AR": 3017825,
  "AZ": 7278717,
  "CA": 39512223,
  "CN": 3565287,
  "CO": 5758736,
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
  "MN": 1344212,
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

# Execute a GET request to the CDC Covid API to retrieve state level covid details
def req_cdc_covid_dat(ste, dte):
    """
    req_cdc_covid_dat makes an HTTP GET request to the 
    CDC Covid API and accepts a response

    Parameters:
        "state": the targeted state (e.g. "CA")
        "date":  the date of the covid data requested

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

    # Fetch the Covid API key from an environment variable
    MY_APP_TOKEN = os.getenv("COVID_API")

    # Do we have a valid state value
    if ste not in state_pop:
        # invalid state parameter
        ret_dict["error"] = "invalid date value: " + ste
        return ret_dict

    # Construct a date/time string
    date_time = dte.strftime("%Y-%m-%d") + "T00:00:00.000"

    # Stand up an API client object
    client = Socrata('data.cdc.gov', MY_APP_TOKEN)
    # Fetch data from the API
    results = client.get("9mfq-cb36", state=ste, submission_date=date_time)

    # Do we have results?
    if len(results) == 0:
        # nothing returned - error condition
        ret_dict["error"] = "no data returned"
        return ret_dict
    
    # Return results
    ret_dict["ok"]       = True
    ret_dict["error"]    = None
    ret_dict["new_case"] = results[0]["new_case"]
    ret_dict["data"]     = results[0]
    return ret_dict



