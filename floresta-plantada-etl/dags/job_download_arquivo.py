import argparse
import logging
import os
from datetime import datetime

import requests

from client_s3 import clientS3

parser = argparse.ArgumentParser(prog="Projeto Florestas Plantadas - Download",
                                 description="Job Download Arquivo Florestas Plantadas")

parser.add_argument("-u", "--url")

try:
    args = parser.parse_args()
    urlArquivo = args.url

    anoMesDia = datetime.now().strftime("%Y-%m-%d")

    ANO = datetime.now().strftime("%Y")
    MES = datetime.now().strftime("%m")
    DIA = datetime.now().strftime("%d")

    nomeArquivo = "floresta_plantada.csv"

    diretorio = f"year={ANO}/month={MES}/day={DIA}/downloaded/{nomeArquivo}"

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; "
                      "Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}

    response = session.get(urlArquivo, headers=headers, verify=False)

    logging.info(f"Arquivo: {nomeArquivo} baixado com sucesso.")

    arquivoCsv = response.content

    clientS3.put_object(
        Body=arquivoCsv, Bucket='snif-florestal-raw',
        Key=diretorio)

    logging.info(f"Arquivo salvo em: {diretorio}")

except Exception as e:
    logging.error(f"Erro ao baixar e salvar o arquivo {urlArquivo}. [{e.args}]")
    raise e
