from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

def test_req_cdc_covid_dat():
    """Return 200 Success when input is valid."""
    response = client.get('/covid_score_state/GA')
    assert response.status_code == 200

def test_invalid_input():
    """Return 500 Validation Error when the state abbreviation is invalid."""
    response = client.get('/covid_score_state/XX')
    assert response.status_code == 500
