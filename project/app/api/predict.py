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

from app.api.dbsession import DBSession
import app.api.covid_score as scr

log = logging.getLogger(__name__)
router = APIRouter()

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