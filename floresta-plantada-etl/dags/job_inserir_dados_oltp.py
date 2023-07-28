# -*- coding: utf-8 -*-

import argparse
import logging

import client_spark
from pyspark.sql.functions import col, to_date

parser = argparse.ArgumentParser(prog="Projeto Florestas Plantadas - Carregar",
                                 description="Job inserir dados no banco de dados")

parser.add_argument("-f", "--filename")

try:
    args = parser.parse_args()
    nomeArquivo = args.filename

    BUCKET_ANALYTICS = "snif-florestal-analytics"

    diretorioArquivo = nomeArquivo[:-4]

    logging.info(f"Lendo arquivo arquivo *.parquet do diretório {diretorioArquivo}")

    spark_client = client_spark.obterSparkClient(f"ingest-{diretorioArquivo}")
    dfArquivo = spark_client \
        .read \
        .parquet(f"s3a://{BUCKET_ANALYTICS}/{diretorioArquivo}", header=True)

    df = dfArquivo \
        .na.fill(value=0, subset="area_ha") \
        .withColumn("area_ha", dfArquivo.area_ha.cast("int")) \
        .withColumn("data_base", to_date(col("data_base"), "dd/MM/yyyy"))

    logging.info(f"Total registros no arquivo: {df.count()}")

    print("===========================================")
    df.show(5)
    print("===========================================")

    logging.info(f"Salvando dados [{diretorioArquivo}] no banco de dados")
    client_spark.salvarDataFrameBancoDados(df,"snif_florestal", diretorioArquivo)

except Exception as e:
    logging.error(f"Erro ao converter arquivo para parquet {nomeArquivo}. [{e.args}]")
    raise e
