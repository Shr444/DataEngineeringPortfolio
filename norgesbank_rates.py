from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd

def fetch_exchange_rates():
    url = 'https://data.norges-bank.no/api/data/EXR/B.EUR+USD+GBP.NOK.SP?format=sdmx-json&lastNObservations=1'
    response = requests.get(url)
    data = response.json()
    series = data['data']['dataSets'][0]['series']
    currencies = data['data']['structure']['dimensions']['series'][1]['values']
    date = data['data']['structure']['dimensions']['observation'][0]['values'][0]['id']
    rows = []
    for i, currency in enumerate(currencies):
        key = f'0:{i}:0:0'
        rate = series[key]['observations']['0'][0]
        rows.append({
            'date': date,
            'currency': currency['id'],
            'currency_name': currency['name'],
            'rate_to_nok': float(rate)
        })
    df = pd.DataFrame(rows)
    df.to_csv('/home/shrad/rawfiles/exchange_rate.csv', index=False)

with DAG(
	  dag_id = "norges_bank_exchange_rates",
	  start_date = datetime(2026,1,1),
	  schedule = "@daily",
	  catchup= False,
) as dag:
	  task = PythonOperator(
		   task_id = "fetch_rates",
		   python_callable = fetch_exchange_rates,
)
