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

@router.get('/fuel')
def get_gas_price_state(ste):
  """
    Get the states current gas price.

  """

  conn = http.client.HTTPSConnection("api.collectapi.com")
  headers = {
      'content-type': "application/json",
      'authorization': "apikey 4UOowiehxUBAIpKW7urqFe:1MiseQe8cyBvfROjBTLagh"
      }


 
  conn.request("GET", "/gasPrice/stateUsaPrice?state=" + ste , headers=headers)
  res = conn.getresponse()
  data = res.read()


  data_json = data.decode("utf-8")

  data_dict = json.loads(data_json)



  return data_dict['result']['state']

