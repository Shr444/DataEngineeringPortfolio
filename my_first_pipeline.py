from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def cleanData():
    df = pd.read_csv('/home/shrad/rawfiles/data.csv')
    df = df.dropna(subset=['name'])
    median_salary = df['salary'].median()
    df['salary'] = df['salary'].fillna(median_salary)
    df['age'] = df['age'].fillna('unknown')
    df['city'] = df['city'].fillna('unknown')
    df['department'] = df['department'].fillna('unknown')
    df.to_csv('/home/shrad/rawfiles/clean_data.csv', index=False)
    print(f"Done — {len(df)} rows saved")

with DAG(
	dag_id= "my_first_pipeline",
	start_date = datetime(2024, 1, 1),
	schedule = '@daily',
	catchup = False,
) as dag:

	task = PythonOperator(
		task_id='clean_csv',
        	python_callable=cleanData,
	)
