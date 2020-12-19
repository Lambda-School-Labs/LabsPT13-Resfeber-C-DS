import logging
import random

from fastapi import APIRouter, HTTPException
import pandas as pd
from pydantic import BaseModel, Field, validator
import psycopg2


import os 
from dotenv import load_dotenv
import pandas as pd
import datetime
from sodapy import Socrata
from datetime import timedelta
import warnings
from joblib import load

import http.client
import json
from app.airbnb_helper_files.worker import return_avg_price

from app.api import gas_price


from app.api.dbsession import DBSession
import app.api.covid_score as scr


log = logging.getLogger(__name__)
router = APIRouter()
#C:\Users\porte\Richard_python\lambda\labs\LabsPT13-Resfeber-C-DS\project\app
# loading the model into this file
path_to_model =  os.path.join(os.path.dirname(__file__), "..", "models", "gradient_boost_model.joblib")

with warnings.catch_warnings():
      warnings.simplefilter("ignore", category=UserWarning)
      gradient_boost_model = load(path_to_model)

class Item(BaseModel):
    """Use this data model to parse the request body JSON."""

    x1: float = Field(..., example=3.14)
    x2: int = Field(..., example=-42)
    x3: str = Field(..., example='banjo')

    def to_df(self):
        """Convert pydantic object to pandas dataframe with 1 row."""
        return pd.DataFrame([dict(self)])

    @validator('x1')
    def x1_must_be_positive(cls, value):
        """Validate that x1 is a positive number."""
        assert value > 0, f'x1 == {value}, must be > 0'
        return value

# Create a database session object
db_sess = DBSession()

# Connect to the database
db_conn_attempt = db_sess.connect()

# Determine if any connection errors occurred
if db_conn_attempt["error"] == None:
    # no errors connecting, assign the connection object for use
    db_conn = db_conn_attempt["value"]
else:
    # a connection error has occurred
    log.error("error attempting to connect to the database: {err_str}".format(err_str=db_conn_attempt["error"]))

@router.get('/db_test')
async def db_test():
    """
    db_test tests the db session object's connection 
    to the Postgres database
    """
    return db_sess.test_connection()

class Airbnb_Loc(BaseModel):
    """ 
    This class used to represent the request body
    when getting info about Airbnb prices
    """
    lat: float = Field(example=40.7128)
    lon: float = Field(example=74.0060)
    room_type: str = Field(example="Entire home/apt")# or "Private room\" or \"Hotel room\" or \"Shared room\"
    num_nights: int = Field(example=1)

    def to_df(self):
        """
        Convert pydantic object to pandas dataframe with 1 row.
        """
        return pd.DataFrame([dict(self)])

    def room_to_num(self, room:str):
        """
        This is the function that will make a numerical representation of the 
        room_type.  This is used to make it so that the room_type
        feature can be used in the model for predicting.

        """
    # the string is checked
        if room == "Private room":
            return 0
        if room == "Entire home/apt":
            return 1
        if room == "Hotel room":
            return 2
        if room == "Shared room":
            return 3

class covid_state(BaseModel):
    state: str = Field(example="CA")


# Route to return a covid score to the caller
@router.get('/covid_score_state/{state}')
async def get_covid_score_state(state: str):
    """
    Return the 'Covid Score' for the given state (e.g. 'CA')

    ### Request Parameter
        - state (e.g. CA)

    ### Response
        - `ok`:       boolean indicating a successful request
        - `score`:    the state's covid score (0,1,2)
        - `color`:    the state's covid score as a color ("green", "yellow", "red")
        - `data`:     CDC covid API data
        - `error`:    error message (if applicable)
    """
    # Generate a covid score
    ret_dict = scr.gen_covid_score(db_conn, state)

    # Any errors generating the covid score
    if not ret_dict["ok"]:
        # an error occurred generating the covid score
        raise HTTPException(status_code=500, detail="an error occurred: " + ret_dict["error"])
        
    return ret_dict
  

@router.post('/state_covid')
async def covid_by_state(state: covid_state):
    MY_APP_TOKEN = str(os.getenv("COVID_API"))
    client = Socrata('data.cdc.gov',MY_APP_TOKEN)
    q = '''
    SELECT * 
    ORDER BY submission_date DESC
    LIMIT 1000
    '''
    results = client.get("9mfq-cb36", query = q)
    df = pd.DataFrame.from_records(results)
    # state_requested = pd.DataFrame([state])
    # state = state_requested.iloc[0]

    last_week = str(datetime.date.today() - timedelta(days = 7))

    # filter df for above info and get the total
    new= df[(df['state'] == state.state) & (df['submission_date'] > last_week)]
    new_cases= new['new_case'].astype('float').sum()
    return new_cases

@router.post('/airbnb')
async def airbnb_price(airbnb : Airbnb_Loc):
    """ using the model to make a prediction"""
    # # changing to numerical value of room type
    # room_type = airbnb.room_to_num(airbnb.room_type)
    # price = gradient_boost_model.predict(pd.DataFrame({"lat": airbnb.lat, "lon": airbnb.lon, "room_type": room_type, "num_nights": airbnb.num_nights}, index=[0]))[0]
    # # rounding to two decimals
    # return round(price, ndigits=2)
    return return_avg_price(lat=airbnb.lat, lon=airbnb.lon, room_type=airbnb.room_type, 
                            num_nights=airbnb.num_nights)

@router.get('/fuel/{ste}')
def get_gas_price_state(ste):
    """
    Get the states current gas price.

    """
    # Set up an HTTP connection object
    MY_APP_TOKEN = str(os.getenv("GAS_API"))
    conn = http.client.HTTPSConnection("api.collectapi.com")
    headers = {
        'content-type': "application/json",
        'authorization': MY_APP_TOKEN
    }


    # Execute the HTTP GET request
    conn.request("GET", "/gasPrice/stateUsaPrice?state=" + ste , headers=headers)
    res = conn.getresponse()
    data = res.read()

    # Convert the byte data to a json string
    data_json = data.decode("utf-8")
    # Load the json string into a python dict
    
    data_dict = json.loads(data_json)


    # Return the state level gas prices
    return data_dict['result']['state']


