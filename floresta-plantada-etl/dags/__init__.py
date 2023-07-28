import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

import sys

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv
import logging

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_LOCATION = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_LOCATION

AMBIENTE = os.getenv("FLORESTA_PLANTADA_ENV")

PATH_ARQUIVO_CONFIG = f"{ROOT_DIR}/.env.test"

logging.basicConfig(format="[%(levelname)s] [%(asctime)s] %(message)s",
                    level=logging.INFO,
                    datefmt="%d/%m/%y %H:%M:%S",
                    encoding="utf-8")

if AMBIENTE is None:
    logging.info("Variável de ambiente FLORESTA_PLANTADA_ENV não existe.")
    AMBIENTE = 'DEV'

if AMBIENTE == 'PROD':
    PATH_ARQUIVO_CONFIG = f'{ROOT_DIR}/.env'

load_dotenv(PATH_ARQUIVO_CONFIG)

logging.info(f"Ambiente da aplicação:{AMBIENTE}")
logging.info(f"Python: {os.getenv('PYSPARK_PYTHON')}")

URL_DATASET = "https://snif.florestal.gov.br/images/xls/recursos_florestais/RF_FPlantada_IBGE-PEVS_SIDRA_Brasil_2013-2020_16-08-2022_DadosAbertos.csv"

with DAG(
        f"floresta_plantada_download_arquivo",
        description="Fazer o download diário do arquivo Florestas Plantadas",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        # schedule="@daily",
        tags=["ingest", "download"]
) as dag:
    task_fazer_download_arquivo = BashOperator(
        task_id=f"tsk_baixar_arquivo_floresta_plantada",
        bash_command=f"python {os.getcwd()}/dags/job_download_arquivo.py -u {URL_DATASET}",
        dag=dag
    )

with DAG(
        f"floresta_plantada_converter_arquivo_para_parquet",
        description="Fazer a conversão dos arquivos *.csv para .parquet",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        # schedule="@daily",
        tags=["transform", "convert"]
) as dag:
    task_converter_arquivos_csv_para_parquet = BashOperator(
        task_id="tsk_converter_arquivo_para_parquet",
        bash_command=f"python {os.getcwd()}/dags/job_converter_arquivo.py -f floresta_plantada.csv",
        dag=dag
    )

with DAG(
        f"floresta_plantada_salvar_dados_OLTP",
        description="Fazer a inclusão dos dados do arquivo .parquet no banco de dados",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        # schedule="@daily",
        tags=["ingest", "banco_dados"]
) as dag:
    task_salvar_banco_dados = BashOperator(
        task_id="tsk_salvar_banco_dados",
        bash_command=f"python {os.getcwd()}/dags/job_inserir_dados_oltp.py -f floresta_plantada.csv",
        dag=dag
    )

with DAG(
        f"floresta_plantada_salvar_dados_DW",
        description="Salvar dados no datawarehouse",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        # schedule="@daily",
        tags=["ingest", "datawarehouse"]
) as dag:
    task_salvar_dados_DW = BashOperator(
        task_id="tsk_criar_datawarehouse",
        bash_command=f"python {os.getcwd()}/dags/job_inserir_dados_dw.py",
        dag=dag
    )

trigger_converter_arquivo_csv_para_parquet = TriggerDagRunOperator(
    task_id=f"tgr_converter_arquivo_para_parquet",
    trigger_dag_id=f"floresta_plantada_converter_arquivo_para_parquet",
)

trigger_salvar_banco_dados = TriggerDagRunOperator(
    task_id=f"tgr_salvar_dados_OLTP",
    trigger_dag_id=f"floresta_plantada_salvar_dados_OLTP",
)

trigger_salvar_dados_DW = TriggerDagRunOperator(
    task_id=f"tgr_salvar_dados_DW",
    trigger_dag_id=f"floresta_plantada_salvar_dados_DW",
)

task_fazer_download_arquivo >> trigger_converter_arquivo_csv_para_parquet
task_converter_arquivos_csv_para_parquet >> trigger_salvar_banco_dados
task_salvar_banco_dados >> trigger_salvar_dados_DW
task_salvar_dados_DW