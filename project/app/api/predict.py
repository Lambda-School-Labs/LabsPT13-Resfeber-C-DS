import logging
import random

from fastapi import APIRouter
import pandas as pd
from pydantic import BaseModel, Field, validator


import os 
from dotenv import load_dotenv
import pandas as pd
import datetime
from sodapy import Socrata
from datetime import timedelta
import warnings
from joblib import load


from app.airbnb_helper_files.worker import return_avg_price


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

@router.post('/predict')
async def predict(item: Item):
    """
    Make random baseline predictions for classification problem 🔮

    ### Request Body
    - `x1`: positive float
    - `x2`: integer
    - `x3`: string

    ### Response
    - `prediction`: boolean, at random
    - `predict_proba`: float between 0.5 and 1.0, 
    representing the predicted class's probability

    Replace the placeholder docstring and fake predictions with your own model.
    """

    X_new = item.to_df()
    log.info(X_new)
    y_pred = random.choice([True, False])
    y_pred_proba = random.random() / 2 + 0.5
    return {
        'prediction': y_pred,
        'probability': y_pred_proba
    }


@router.post('/state_covid')
async def covid_by_state(state: dict):
    MY_APP_TOKEN = os.getenv("COVID_API")
    client = Socrata('data.cdc.gov',MY_APP_TOKEN)
    q = '''
    SELECT * 
    ORDER BY submission_date DESC
    LIMIT 1000
    '''
    results = client.get("9mfq-cb36", query = q)
    df = pd.DataFrame.from_records(results)
    state_requested = pd.DataFrame([state])
    state = state_requested.iloc[0][0]

    last_week = str(datetime.date.today() - timedelta(days = 7))

    # filter df for above info and get the total
    new= df[(df['state'] == state) & (df['submission_date'] > last_week)]
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
    return return_avg_price(lat=airbnb.lat, lon=airbnb.lon, room_type=airbnb.room_type, num_nights=airbnb.num_nights)

