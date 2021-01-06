from fastapi import APIRouter, HTTPException
import pandas as pd
import datetime
from sodapy import Socrata
from datetime import timedelta
from app.api.constants import STATE_POP, statecodes
from app.api.viz_prep import viz_readiness, viz_readiness_covid_score
import plotly.graph_objects as go
import os 
from dotenv import load_dotenv


router = APIRouter()


@router.get('/viz/covid_pop')
async def viz(statecode: str):
    """
    Visualize covid cases reported to the CDC in last 14 days per state population 
    
    ### Path Parameter
    `statecode`: The [USPS 2 letter abbreviation](https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations#Table) 
    (case insensitive) for any of the 50 states or the District of Columbia.

    ### Response
    JSON string to render with [react-plotly.js](https://plotly.com/javascript/react/) 
    """

    df3 = viz_readiness(STATE_POP, statecodes)

    fig = go.Figure(data=go.Choropleth(
    locations=df3['index'], # Spatial coordinates
    z = df3['covid_case_per_pop'].astype(float), # Data to be color-coded
    locationmode = 'USA-states', # set of locations match entries in `locations`
    colorscale = 'Reds',
    colorbar_title = "Covid by Population",
    ))

    fig.update_layout(
        title_text = 'Current US Covid Cases by State',
        geo_scope='usa', # limite map scope to USA
    )

    return fig.to_json()

@router.get('/viz/covid_score')
async def viz_covid():
    """
    Visualize covid score based off of covid cases reported in the last 14 days to the CDC

    ### Response
    JSON string to render with [react-plotly.js](https://plotly.com/javascript/react/
    """
    df3 = viz_readiness_covid_score(STATE_POP)

    fig = go.Figure(data=go.Choropleth(
    locations=df3['index'], # Spatial coordinates
    z = df3['covid_score'].astype(float), # Data to be color-coded
    locationmode = 'USA-states', # set of locations match entries in `locations`
    colorscale = 'Reds',
    colorbar_title = "Covid Score: Statistical Difference in Covid Cases",
    ))

    fig.update_layout(
        title_text = 'Covid Score by State',
        geo_scope='usa', # limite map scope to USA
    )

    return fig.to_json()