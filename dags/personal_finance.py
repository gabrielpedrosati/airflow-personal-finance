# A data pipeline for my personal finances

#[START modules]
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta, date
import requests
import json
import os
#[END modules]

#[START functions]
def csv_files_available():
    cryptos = os.path.exists("./dags/data/cryptos.csv")
    stocks = os.path.exists("./dags/data/stocks.csv")
    
    if not cryptos:
        with open("./dags/data/cryptos.csv", "w") as cryptos_file:
            cryptos_file.write("Name,Coin,Price,Date\n")
    if not stocks:
        with open("./dags/data/stocks.csv", "w") as stocks_file:
            stocks_file.write("Symbol,Name,Price,Date\n")


def download_stocks():
    stocks = ['PETR4', 'PETR3', 'BRAP4', 'BRKM5', 'MRFG3', 'GOAU4', 'QUAL3', 'CMIN3', 'GGBR4']

    for stock in stocks:
        req = requests.get('https://brapi.dev/api/quote/{}'.format(stock))
        req_json = json.loads(req.text)
        symbol = req_json['results'][0]['symbol']
        short_name = req_json['results'][0]['shortName']
        price = str(req_json['results'][0]['regularMarketPrice'])
        date_ = str(date.today())

        with open('./dags/data/stocks.csv', 'a') as csv_file:
            csv_file.write(symbol)
            csv_file.write(',')
            csv_file.write(" ".join(short_name.split()))
            csv_file.write(',')
            csv_file.write(price)
            csv_file.write(',')
            csv_file.write(date_)
            csv_file.write('\n')

def download_cryptos():
    cryptos = ['BTC', 'ETH', 'DOGE', 'LTC', 'USDC']

    for crypto in cryptos:
        req = requests.get('https://brapi.dev/api/v2/crypto?coin={}'.format(crypto))
        req_json = json.loads(req.text)
        name = req_json['coins'][0]['coinName']
        coin = req_json['coins'][0]['coin']
        price = str(req_json['coins'][0]['regularMarketPrice'])
        date_ = str(date.today())

        with open('./dags/data/cryptos.csv', 'a') as csv_file:
            csv_file.write(name)
            csv_file.write(',')
            csv_file.write(coin)
            csv_file.write(',')
            csv_file.write(price)
            csv_file.write(',')
            csv_file.write(date_)
            csv_file.write('\n')
#[END modules]

#[START args]
default_args = {
    'owner':'pedrosa'
}
#[END args]

#[START DAG]
with DAG(
    dag_id="personal_finance",
    start_date=datetime(2023,1,2),
    schedule_interval="0 9 * * *",
    catchup=False,
    default_args=default_args
) as dag:

    is_brapi_available = HttpSensor(
        task_id="is_brapi_available",
        http_conn_id="brapi_conn",
        endpoint=""
    )

    are_csv_files_available = PythonOperator(
        task_id="are_csv_files_available",
        python_callable=csv_files_available
    )

    stocks_download = PythonOperator(
        task_id="stocks_download",
        python_callable=download_stocks
    )

    cryptos_download = PythonOperator(
        task_id="cryptos_download",
        python_callable=download_cryptos
    )

    is_brapi_available >> are_csv_files_available >> [stocks_download, cryptos_download]

#[END DAG]
