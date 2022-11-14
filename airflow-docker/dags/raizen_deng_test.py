from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from subprocess import  Popen
from os import makedirs, path, listdir
import urllib
import pandas as pd
import pyarrow

def download_store_file(xls_dir):
    """
    Download the xls file to the folder
    """
    url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
    xls_filename = url.split("/")[-1]
    makedirs(xls_dir, exist_ok=True)
    response = urllib.request.urlretrieve(url, f'{xls_dir}/{xls_filename}') 


def pre_process_file(convert_dir):
    """
    Make libreoffice open the xls file in order to show the other sheets that was part of the pivot table
    If you are using windows, libreoffice installation is required
    loffice = 'C:/Program Files/LibreOffice/program/soffice.exe'
    cmd = f"{loffice} --headless --convert-to xls --outdir {convert_dir} {file_path}"

    In the fuction we use the linux way to match with airflow
    """
    makedirs(convert_dir, exist_ok=True)
    file_path = "/home/airflow/raw_data/vendas-combustiveis-m3.xls"
    cmd = f"libreoffice --headless --convert-to xls --outdir {convert_dir} {file_path}"
    pr = Popen(cmd.split())
    pr.communicate()

def process_data(df):
    """
    Process data inside the xls file, using the unhiden sheets that showed after the pre process phase.
    Open the file based on the sheet name, rename columns and normalize it, remove unwanted data and create ne variables
    Redefine the columns orders, to match the test requirements.
    """
    
    df.columns = ['combustivel', 'ano', 'região', 'uf', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'total']
    df = df.melt(id_vars=['combustivel', 'ano', 'região', 'uf'])
    #df_totals = df.loc[df['variable'] == 'total']
    df = df.loc[df['variable'] != 'total']
    df["year_month"] = pd.to_datetime(df["ano"].astype(str) + "-" + df["variable"], format="%Y-%m")
    df["product"] = df["combustivel"].str.split("(").str[0].str.strip()
    df["unit"] = df["combustivel"].str.split("(").str[1].replace('\)', '', regex = True).str.strip()
    df["created_at"] = pd.Timestamp.today()
    df = df.drop(labels=["combustivel", "ano", "região", 'variable'], axis=1)
    df.rename(columns={"estado": "uf", "value": "volume"}, inplace=True)
    df['volume'] = pd.to_numeric(df['volume'])
    df.fillna(0, inplace=True)
    df = df.iloc[:, [2,0,3,4,1,5]]

    return df

def store_parquet(xls_sheet_name, p_dir, p_filename):
    """
    Store the result in .parquet files
    in this scenario we dont used partition because of the small size of the files, if that is needed use the code bellow as example:
    df.to_parquet(f'{parquet_dir}', partition_cols=['year_month'])
    """
    df = pd.read_excel('/home/airflow/converted_data/vendas-combustiveis-m3.xls', sheet_name=xls_sheet_name)
    df = process_data(df)
    makedirs(p_dir, exist_ok=True)
    df.to_parquet(p_dir + p_filename)

default_args = {
    'owner': 'Eduardo Alves Silva',
    'depends_on_past': False,
    'retries': 0
}

with DAG(
    dag_id="Raízen_Semantix_DataEngTest",
    start_date=days_ago(1),
    schedule_interval="@once",
    description="Resultado do teste da Raízen",
    catchup=False,
    default_args=default_args) as dag:
    
    task_download_xls = PythonOperator(
        task_id="download_xls",
        python_callable=download_store_file,
        dag=dag,
        op_kwargs={
            "xls_dir":"/home/airflow/raw_data",
        }
    )

    task_process_file = PythonOperator(
        task_id="pre_process_file",
        python_callable=pre_process_file,
        dag=dag,
        op_kwargs={
            "convert_dir": "/home/airflow/converted_data"
        }
    )

    task_store_derivated_fuels = PythonOperator(
        task_id="store_derivated_fuels",
        python_callable=store_parquet,
        dag=dag,
        op_kwargs={
            "xls_sheet_name": "DPCache_m3",
            "p_dir": "/home/airflow/partition_files/",
            "p_filename": "sales_oil_derivative_fuels.parquet"
        }
    )

    task_store_diesel = PythonOperator(
        task_id="store_diesel",
        python_callable=store_parquet,
        dag=dag,
        op_kwargs={
            "xls_sheet_name": "DPCache_m3_2",
            "p_dir": "/home/airflow/partition_files/",
            "p_filename": "sales_diesel.parquet"
        }
    )

    (
        task_download_xls >> task_process_file >> [task_store_derivated_fuels, task_store_diesel] 

    )