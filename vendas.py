# Databricks notebook source
# MAGIC %md
# MAGIC > # MVP CONCLUSÃO DE SPRINT - CRIAÇÃO DE UM PIPELINE DE DADOS COM DELTA LIVE TABLES 
# MAGIC Lucas de Oliveira Noronha

# COMMAND ----------

# MAGIC %md
# MAGIC ### O notebook "vendas" prepara e organiza dados de vendas em um pipeline DLT. Inicia configurando o ambiente e copiando os dados necessários para o DBFS. Em seguida, define uma tabela de fatos de vendas e tres dimensões (clientes, categorias e filial) para análise, garantindo a qualidade dos dados com expectativas DLT.
# MAGIC
# MAGIC A base de dados é uma amostra de tres meses de vendas de uma empresa
# MAGIC
# MAGIC paths
# MAGIC https://raw.githubusercontent.com/LDONoronha/data_engineering/main/vendas.csv
# MAGIC
# MAGIC Descrição das colunas:
# MAGIC
# MAGIC - COD_CLIENTE;
# MAGIC int;
# MAGIC PK CLIENTE
# MAGIC
# MAGIC - DEPARTAMENTO;
# MAGIC string;
# MAGIC Categoria ou familia do produto
# MAGIC
# MAGIC - MODO_PAGAMENTO;
# MAGIC string;
# MAGIC TIPO DE PAGAMENTO DO CLIENTE
# MAGIC
# MAGIC - COD_FILIAL;
# MAGIC int;
# MAGIC LOJA
# MAGIC
# MAGIC - UF;
# MAGIC string;
# MAGIC ESTADO
# MAGIC
# MAGIC - QTDE;
# MAGIC int;
# MAGIC QUANTIDADE DE ITENS COMPRADOS
# MAGIC
# MAGIC - VALOR_TOTAL;
# MAGIC double;
# MAGIC VALOR DE RECEITA DE VENDAS
# MAGIC
# MAGIC - DATA;
# MAGIC timestamp;
# MAGIC DATA DE COMPRA
# MAGIC
# MAGIC - MES_ANO;
# MAGIC string;
# MAGIC DATA DE COMPRA FORMATO RESUMIDO
# MAGIC
# MAGIC

# COMMAND ----------

# Esta célula importa o módulo dlt, utilizado para definir tabelas e transformações em pipelines Delta Live Tables (DLT), e todas as funções do módulo pyspark.sql.functions, que são usadas para manipulação de colunas e dados em DataFrames do PySpark.

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

#Esta célula configura o ambiente, definindo variáveis de ambiente para o caminho do volume no catálogo Unity, a URL de download do conjunto de dados e o nome do arquivo. Em seguida, copia o arquivo CSV de vendas da URL especificada para o caminho do volume no Databricks File System (DBFS).

import os

os.environ["UNITY_CATALOG_VOLUME_PATH"] = "/Volumes/lucasdeoliveira/default/fatovendas/"
os.environ["DATASET_DOWNLOAD_URL"] = "https://raw.githubusercontent.com/LDONoronha/data_engineering/main/vendas.csv"
os.environ["DATASET_DOWNLOAD_FILENAME"] = "vendas.csv"

dbutils.fs.cp(f"{os.environ.get('DATASET_DOWNLOAD_URL')}", f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}")

# COMMAND ----------

#Esta célula define uma tabela DLT chamada fato_vendas, contendo dados de vendas de um período de 3 meses. A função lê o arquivo CSV de vendas do caminho especificado, utilizando o Spark para inferir o esquema das colunas automaticamente e considerando a primeira linha como cabeçalho.

@dlt.table(
  comment="Base de dados com histórico de 3 meses de vendas de uma empresa."
)
def fato_vendas():
  df = spark.read.csv(f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}", header=True, inferSchema=True)
  return df

# COMMAND ----------

# Esta célula cria uma tabela DLT chamada dim_clientes, contendo uma lista única de códigos de clientes. Utiliza a expectativa dlt.expect para garantir que a coluna COD_CLIENTE não contenha valores nulos. A função seleciona a coluna COD_CLIENTE da tabela fato_vendas e remove duplicatas.

@dlt.table(
  comment="Base de Cliente."
)
@dlt.expect("valid_COD_CLIENTE", "COD_CLIENTE IS NOT NULL")
def dim_cliente():
  return (
    dlt.read("fato_vendas")
      .select("COD_CLIENTE")
      .distinct()
  )

# COMMAND ----------

# Esta célula define outra tabela DLT, dim_categoria, contendo categorias únicas de produtos vendidos. Assim como na célula anterior, uma expectativa é definida para garantir que a coluna DEPARTAMENTO não tenha valores nulos. A função seleciona a coluna DEPARTAMENTO da tabela fato_vendas e aplica o método distinct() para remover duplicatas.

@dlt.table(
  comment="Categoria dos produtos vendidos."
)
@dlt.expect("valid_DEPARTAMENTO", "DEPARTAMENTO IS NOT NULL")
def dim_categoria():
  return (
    dlt.read("fato_vendas")
      .select("DEPARTAMENTO")
      .distinct()
  )

# COMMAND ----------

# Esta célula define outra tabela DLT, dim_filial, contendo filiais únicas (lojas de vendasd) por estados. Assim como na célula anterior, uma expectativa é definida para garantir que a coluna COD_FILIAL não tenha valores nulos. A função seleciona as colunas UF e cod_filial da tabela fato_vendas e aplica o método distinct() para remover duplicatas.

@dlt.table(
  comment="Região e loja de compra."
)
@dlt.expect("valid_COD_FILIAL", "COD_FILIAL IS NOT NULL")
def dim_filial():
  return (
    dlt.read("fato_vendas")
      .select("UF","COD_FILIAL")
      .distinct()
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultar as tabelas criadas

# COMMAND ----------

# MAGIC %md
# MAGIC A partir das das tabelas criadas, prosseguindo para responder perguntas de negócios.

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando as tabelas criadas pro meio de consultas em SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lucasdeoliveira.vendas.fato_vendas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lucasdeoliveira.vendas.dim_cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lucasdeoliveira.vendas.dim_filial

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lucasdeoliveira.vendas.dim_categoria

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizar análises a partir dos dados

# COMMAND ----------

# MAGIC %md ### # Top 10 dos clientes com maior valor de compras em junho de 2024

# COMMAND ----------

# DBTITLE 1,# Top 10 dos clientes com maior valor de compras em junho de 2024
# MAGIC %sql
# MAGIC SELECT COD_CLIENTE, SUM(VALOR_TOTAL) AS total_compras
# MAGIC FROM lucasdeoliveira.vendas.fato_vendas
# MAGIC WHERE DATA >= '2024-06-01' AND DATA <= '2024-06-30'
# MAGIC GROUP BY COD_CLIENTE
# MAGIC ORDER BY total_compras DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md ### # Qual departamento os clientes top 10 compram?

# COMMAND ----------

# DBTITLE 1,Qual departamento os clientes top 10 compram?
# MAGIC %sql
# MAGIC WITH TopClientes AS (
# MAGIC     SELECT COD_CLIENTE, SUM(VALOR_TOTAL) AS total_compras
# MAGIC     FROM lucasdeoliveira.vendas.fato_vendas
# MAGIC     WHERE DATA >= '2024-06-01' AND DATA <= '2024-06-30'
# MAGIC     GROUP BY COD_CLIENTE
# MAGIC     ORDER BY total_compras DESC
# MAGIC     LIMIT 10
# MAGIC )
# MAGIC SELECT tc.COD_CLIENTE, d.DEPARTAMENTO, SUM(fv.VALOR_TOTAL) AS total_compras_departamento
# MAGIC FROM TopClientes tc
# MAGIC JOIN lucasdeoliveira.vendas.fato_vendas fv ON tc.COD_CLIENTE = fv.COD_CLIENTE
# MAGIC JOIN lucasdeoliveira.vendas.dim_categoria d ON fv.DEPARTAMENTO = d.DEPARTAMENTO
# MAGIC GROUP BY tc.COD_CLIENTE, d.DEPARTAMENTO
# MAGIC ORDER BY total_compras_departamento DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
