# -*- coding: utf-8 -*-

import argparse
import logging

import client_spark
from pyspark.sql.functions import date_format

parser = argparse.ArgumentParser(prog="Projeto Florestas Plantadas - Carregar DW",
                                 description="Job inserir dados no datawarehouse")


def salvarDimensaoLocalizacao():
    SQL_DIM_TEMPO = """
        SELECT 
            regiao, 
            nome_uf, 
            sigla_uf,
            codigo_microrregiao, 
            nome_microrregiao, 
            microrregiao, 
            codigo_municipio,
            municipio, 
            municipio_geografico
        FROM floresta_plantada
        group by regiao, 
            nome_uf, 
            sigla_uf,
            codigo_microrregiao, 
            nome_microrregiao, 
            microrregiao, 
            codigo_municipio,
            municipio, 
            municipio_geografico
    """
    dfDimLocalizacao = client_spark.getDataframeFromSql(spark_client, "snif_florestal", SQL_DIM_TEMPO)
    logging.info(f"Total registros de localização: {dfDimLocalizacao.count()}")
    print("===========================================")
    dfDimLocalizacao.show(5)
    print("===========================================")
    logging.info(f"Criando tabela dimensão localização no DW")
    client_spark.salvarDataFrameBancoDados(dfDimLocalizacao, "dw_snif_florestal", "dim_localizacao")


def salvarDimensaoTempo():
    SQL_RANGE_DATAS = """
    SELECT 
        min(data_base) data_inicial,
        max(data_base) data_final
    FROM public.floresta_plantada
    """
    dfRangeDatas = client_spark.getDataframeFromSql(spark_client, "snif_florestal", SQL_RANGE_DATAS)
    dfDatas = dfRangeDatas.selectExpr("explode(sequence(data_inicial,data_final)) as data")
    dfDimTempo = dfDatas \
        .withColumn("ano", date_format(dfDatas.data, "yyyy")) \
        .withColumn("mes", date_format(dfDatas.data, "MM")) \
        .withColumn("dia", date_format(dfDatas.data, "dd")) \
        .withColumn("ano_mes", date_format(dfDatas.data, "yyyy-MM")) \
        .withColumn("trimestre", date_format(dfDatas.data, "yyyy-Q"))
    logging.info(f"Total registros de tempo: {dfDimTempo.count()}")
    print("===========================================")
    dfDimTempo.show(5)
    print("===========================================")
    logging.info(f"Criando tabela dimensão tempo no DW")
    client_spark.salvarDataFrameBancoDados(dfDimTempo, "dw_snif_florestal", "dim_tempo")


def salvarDimensaoEspecie():
    SQL_DIM_ESPECIE = """
        SELECT 
            especie_florestal
        FROM public.floresta_plantada
        GROUP BY especie_florestal   
    """
    dfDimEspecie = client_spark.getDataframeFromSql(spark_client, "snif_florestal", SQL_DIM_ESPECIE)
    logging.info(f"Total registros de especie: {dfDimEspecie.count()}")
    print("===========================================")
    dfDimEspecie.show(5)
    print("===========================================")
    logging.info(f"Criando tabela dimensão espécie no DW")
    client_spark.salvarDataFrameBancoDados(dfDimEspecie, "dw_snif_florestal", "dim_especie")


def salvarFatoArea():
    SQL_FATO_AREA = """
        SELECT 
            data_base,
            codigo_municipio,
            especie_florestal, 
            area_ha
        FROM public.floresta_plantada
    """
    dfFatoArea = client_spark.getDataframeFromSql(spark_client, "snif_florestal", SQL_FATO_AREA)
    logging.info(f"Total registros de área: {dfFatoArea.count()}")
    print("===========================================")
    dfFatoArea.show(5)
    print("===========================================")
    logging.info(f"Criando tabela fato área no DW")
    client_spark.salvarDataFrameBancoDados(dfFatoArea, "dw_snif_florestal", "fat_area")


try:
    args = parser.parse_args()
    logging.info(f"Lendo dados OLTP ")
    spark_client = client_spark.obterSparkClient(f"ingest-dw")

    salvarDimensaoLocalizacao()
    salvarDimensaoTempo()
    salvarDimensaoEspecie()
    salvarFatoArea()

except Exception as e:
    logging.error(f"Erro ao salvar dados no DW. [{e.args}]")
    raise e
