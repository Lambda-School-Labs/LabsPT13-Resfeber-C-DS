import datetime
from sodapy import Socrata
from datetime import timedelta
from app.api.constants import STATE_POP, statecodes

def viz_readiness(state_pops, state_codes):
  MY_APP_TOKEN = str(os.getenv("COVID_API"))
  client = Socrata('data.cdc.gov',MY_APP_TOKEN)
  q = '''
    SELECT * 
    ORDER BY submission_date DESC
    LIMIT 1000
    '''
  results = client.get("9mfq-cb36", query = q)
  df = pd.DataFrame.from_records(results)
    

  last_week = str(datetime.date.today() - timedelta(days = 14))
  dict_covid = {}

  for state in state_codes:
    new= df[(df['state'] == state) & (df['submission_date'] > last_week)]
    new_cases= new['new_case'].astype('float').sum()
    dict_covid[state] = new_cases
  df = pd.DataFrame.from_dict(dict_covid, orient='index')
  df.reset_index()
  df2 = pd.DataFrame.from_dict(state_pops, orient='index')
  df2.reset_index()
  df3 = pd.concat([df, df2], axis=1)
  df3.columns = ['covid_cases', 'population']
  empty_list = []
  length = len(df3)
  for i in range(0, length):
    empty_list.append(df3.iloc[i][0]/df3.iloc[i][1])
  df3['covid_case_per_pop'] = empty_list
  df3 = df3.reset_index()

  return df3