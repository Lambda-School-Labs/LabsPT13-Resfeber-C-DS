import http.client
import json


# Execute a request to the CDC Covid API
def get_gas_price_state(ste):
  """
     Get the states current gas price.
  """

  # Fetch the Gas API key from an environment variable
  MY_GAS_PRICE_API_TOKEN = os.getenv("GAS_API")

  # Set up an HTTP connection object
  conn = http.client.HTTPSConnection("api.collectapi.com")
  headers = {
      'content-type': "application/json",
      'authorization': "MY_GAS_PRICE_API_TOKEN"
      }

  # Execute the HTTP GET request
  conn.request("GET", "/gasPrice/stateUsaPrice?state=" + ste, headers=headers)
  res = conn.getresponse()
  data = res.read()

  # Convert the byte data to a json string
  data_json = data.decode("utf-8")
  # Load the json string into a python dict
  data_dict = json.loads(data_json)

  # Return the state level gas prices
  return data_dict['result']['state']
