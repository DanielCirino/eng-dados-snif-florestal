# -*- coding: utf-8 -*-

import argparse
import logging

from client_s3 import clientS3
import client_spark

parser = argparse.ArgumentParser(prog="Projeto Florestas Plantadas - Converter Arquivo CSV",
                                 description="Job converter arquivos")

parser.add_argument("-f", "--filename")

try:
    args = parser.parse_args()
    nomeArquivoCompactado = args.filename

    BUCKET_RAW = "snif-florestal-raw"
    BUCKET_STAGE = "snif-florestal-stage"
    BUCKET_ANALYTICS = "snif-florestal-analytics"

    objetosS3 = clientS3.list_objects(Bucket=BUCKET_RAW)

    for obj in objetosS3.get("Contents"):
        chave = obj["Key"]
        ano, mes, dia, etapa, nomeArquivo = chave.split("/")

        if etapa == "downloaded":
            diretorioArquivoCSV = nomeArquivo[:-4]

            logging.info(f"Lendo arquivo arquivo {chave}")
            spark_client = client_spark.obterSparkClient(f"convert-{diretorioArquivoCSV}")

            dfArquivo = spark_client \
                .read \
                .csv(f"s3a://{BUCKET_RAW}/{chave}", sep=";", header=True, encoding="iso-8859-1") \
                .withColumnsRenamed(
                {
                    "Ano": "data_base",
                    "Ano_PEVS": "ano_data_base",
                    "Região": "regiao",
                    "Nome UF": "nome_uf",
                    "Sigla UF": "sigla_uf",
                    "Código Microrregião": "codigo_microrregiao",
                    "Nome Microrregião": "nome_microrregiao",
                    "Microrregião Geográfica": "microrregiao",
                    "Código Município Completo": "codigo_municipio",
                    "Município": "municipio",
                    "Município Geográfico": "municipio_geografico",
                    "Espécie florestal": "especie_florestal",
                    "Área (ha)": "area_ha"
                })\
                .na.fill(value="0",subset=["area_ha"])

            logging.info(f"Total registros no arquivo: {dfArquivo.count()}")
            print("===========================================")
            dfArquivo.show(5)
            print("===========================================")

            logging.info(f"Salvando arquivo {nomeArquivo} no bucket stage no formato *.csv...")

            dfArquivo.write.csv(
                path=f"s3a://{BUCKET_STAGE}/{diretorioArquivoCSV}",
                mode="overwrite",
                header=True,
                sep=",")

            logging.info(f"Arquivo {nomeArquivo} salvo no bucket stage [{BUCKET_STAGE}/{diretorioArquivoCSV}].")

            logging.info(f"Salvando arquivo {nomeArquivo} no bucket analytics no formato *.parquet...")

            dfArquivo.write.parquet(
                path=f"s3a://{BUCKET_ANALYTICS}/{diretorioArquivoCSV}",
                mode="overwrite")

            logging.info(f"Arquivo {nomeArquivo} salvo no bucket analytics [{BUCKET_ANALYTICS}/{diretorioArquivoCSV}].")

            # Fazer cópia do arquivo para a pasta processado e apagar da pasta de download
            novaChave = chave.replace("downloaded", "processed")

            clientS3.copy_object(
                CopySource={'Bucket': BUCKET_RAW, 'Key': chave},
                Bucket=BUCKET_RAW,
                Key=novaChave)

            clientS3.delete_object(Bucket=BUCKET_RAW, Key=chave)

            logging.info(f"Arquivo {nomeArquivo} movido para a pasta de processados.")

except Exception as e:
    logging.error(f"Erro ao processar o arquivo {nomeArquivoCompactado}. [{e.args}]")
    raise e
