import logging
import random

from fastapi import APIRouter
import pandas as pd
from pydantic import BaseModel, Field, validator

import psycopg2

import os 
from dotenv import load_dotenv
import pandas as pd
import datetime
from sodapy import Socrata
from datetime import timedelta


log = logging.getLogger(__name__)
router = APIRouter()

#* Create a connection to the DS API Postgres database
# Read database configuration from environment variables
DS_DB_HOST      = os.getenv("DS_DB_HOST")
DS_DB_PORT      = os.getenv("DS_DB_PORT")
DS_DB_NAME      = os.getenv("DS_DB_NAME")
DS_DB_USER      = os.getenv("DS_DB_USER")
DS_DB_PASSWORD  = os.getenv("DS_DB_PASSWORD")

# Missing database configuration?
if len(DS_DB_HOST)      == 0     or      \
    len(DS_DB_NAME)     == 0     or      \
    len(DS_DB_USER)     == 0     or      \
    len(DS_DB_PASSWORD) == 0:
    log.error("One or more DB environment variables are missing.")

# Configure a Postgres database connection object
pg_conn = psycopg2.connect(user =        DS_DB_USER,
                           password =    DS_DB_PASSWORD,
                           host =        DS_DB_HOST,
                           port =        DS_DB_PORT,
                           database =    DS_DB_NAME)

# Test the database connection
try:
    cursor = pg_conn.cursor()
    # Print PostgreSQL Connection properties
    log.info("Connecting to the database with these credentials: {conn_details}\n".format(conn_details=pg_conn.get_dsn_parameters()))

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    log.info("Successfully connected to the database: {conn_details}\n".format(conn_details=record))

except (Exception, psycopg2.Error) as error:
    log.error("Error while connecting to PostgreSQL: {err}".format(err=error))


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

