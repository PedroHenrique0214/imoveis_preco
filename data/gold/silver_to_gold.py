# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver to Gold para venda de Imóveis

# COMMAND ----------

# Importando os dados de SP
df_sp = spark.read.table('silver.dados_venda.sp_venda_silver')

# Importando os dados de RJ
df_rj = spark.read.table('silver.dados_venda.rj_venda_silver')

# COMMAND ----------

# Visualizando os dados de SP
df_sp.display()

# COMMAND ----------

# Visualizando os dados de RJ
df_rj.display()

# COMMAND ----------

# Vou diminuir as casas decimais para melhorar a visualização dos dados

from pyspark.sql.functions import round, col
from pyspark.sql.types import DoubleType, FloatType

for column_name in df_sp.columns:
    if isinstance(df_sp.schema[column_name].dataType, (DoubleType, FloatType)):
        df_sp = df_sp.withColumn(column_name, round(col(column_name), 4))


for column_name in df_rj.columns:
    if isinstance(df_rj.schema[column_name].dataType, (DoubleType, FloatType)):
        df_rj = df_rj.withColumn(column_name, round(col(column_name), 4))


df_sp.display(), df_rj.display()

# COMMAND ----------

from pyspark.sql.functions import lit

# Colocando uma coluna com a cidade
df_sp = df_sp.withColumn("Cidade", lit("São Paulo"))

df_rj = df_rj.withColumn("Cidade", lit("Rio de Janeiro"))

df_sp.display(), df_rj.display()

# COMMAND ----------

# Data frame concatenado das duas cidades
df_vendas = df_sp.union(df_rj)

# Ordena pelo campo 'date'
df_vendas = df_vendas.orderBy("datas")

# Mostra o resultado
df_vendas.display()

# COMMAND ----------

df_vendas.coalesce(1) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.vendas.df_vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver to Gold para aluguel de Imóveis

# COMMAND ----------

# Importando os dados de SP
df_sp_aluguel = spark.read.table('silver.dados_aluguel.sp_aluguel_silver')

# Importando os dados de RJ
df_rj_aluguel = spark.read.table('silver.dados_aluguel.rj_aluguel_silver')

# Visualizando os dados
df_sp_aluguel.display(), df_rj_aluguel.display()

# COMMAND ----------

# Vou diminuir as casas decimais para melhorar a visualização dos dados
for column_name in df_sp_aluguel.columns:
    if isinstance(df_sp_aluguel.schema[column_name].dataType, (DoubleType, FloatType)):
        df_sp_aluguel = df_sp_aluguel.withColumn(column_name, round(col(column_name), 4))


for column_name in df_rj_aluguel.columns:
    if isinstance(df_rj_aluguel.schema[column_name].dataType, (DoubleType, FloatType)):
        df_rj_aluguel = df_rj_aluguel.withColumn(column_name, round(col(column_name), 4))


df_sp_aluguel.display(), df_rj_aluguel.display()

# COMMAND ----------

# Colocando uma coluna com a cidade
df_sp_aluguel = df_sp_aluguel.withColumn("Cidade", lit("São Paulo"))

df_rj_aluguel = df_rj_aluguel.withColumn("Cidade", lit("Rio de Janeiro"))

df_sp_aluguel.display(), df_rj_aluguel.display()

# COMMAND ----------

# Data frame concatenando as duas cidades
df_aluguel = df_sp_aluguel.union(df_rj_aluguel)

# Ordena pelo campo 'date'
df_aluguel = df_aluguel.orderBy("datas")

# Mostra o resultado
df_aluguel.display()

# COMMAND ----------

# Salvando os dados de aluguel
df_aluguel.coalesce(1) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.aluguel.df_aluguel")
