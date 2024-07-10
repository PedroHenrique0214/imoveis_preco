# Databricks notebook source
# MAGIC %md
# MAGIC ### São Paulo:
# MAGIC  Dados de Venda

# COMMAND ----------

# DBTITLE 1,Pegando a tabela vendas de SP
df_sp_venda = spark.read.table("bronze.dados_sp.sp_venda")

# COMMAND ----------

# DBTITLE 1,Criando colunas ano e mês
from pyspark.sql.functions import split

df_sp_venda = df_sp_venda.withColumn("mes", split(df_sp_venda["data"], ", ").getItem(0)) \
                        .withColumn("ano", split(df_sp_venda["data"], ", ").getItem(1))

df_sp_venda.display()

# COMMAND ----------

# DBTITLE 1,Substituindo 'mar�o' por 'março'
from pyspark.sql.functions import when, col

df_sp_venda = df_sp_venda.withColumn("mes", when(col("mes") == "mar�o", "março").otherwise(col("mes")))

df_sp_venda.display()

# COMMAND ----------

# DBTITLE 1,Juntando a coluna mês e ano
from pyspark.sql.functions import col, lit, concat

df_sp_venda = df_sp_venda.withColumn("datas", concat(col("mes"), lit(" "), col("ano").cast("string")))

df_sp_venda.display()

# COMMAND ----------

# DBTITLE 1,Trocando "," por "."
from pyspark.sql.functions import regexp_replace, col

for column_name in df_sp_venda.columns:
    df_sp_venda = df_sp_venda.withColumn(column_name, regexp_replace(col(column_name), ",", "."))

df_sp_venda.display()

# COMMAND ----------

# DBTITLE 1,Criando nossa tabela silver

# Criar uma tabela temporária
df_sp_venda.createOrReplaceTempView("temp_table_sp")

# Executar a conversão com SQL
query = """
SELECT 
    CAST(`datas` AS String) AS datas,
    CAST(`mes` AS String) AS mes,
    CAST(`ano` AS String) AS ano,
    CAST(variacao_total_mensal AS Float) AS variacao_total_mensal,
    CAST(var_mensal_1D AS Float) AS var_mensal_1D,
    CAST(var_mensal_2D AS Float) AS var_mensal_2D,
    CAST(var_mensal_3D AS Float) AS var_mensal_3D,
    CAST(var_mensal_4D AS Float) AS var_mensal_4D,
    CAST(variacao_total_anual AS Float) AS variacao_total_anual,
    CAST(variacao_anual_1D AS Float) AS variacao_anual_1D,
    CAST(variacao_anual_2D AS Float) AS variacao_anual_2D,
    CAST(variacao_anual_3D AS Float) AS variacao_anual_3D,
    CAST(variacao_anual_4D AS Float) AS variacao_anual_4D,
    CAST(preco_medio_m2_total AS Float) AS preco_medio_m2_total,
    CAST(preco_medio_m2_1D AS Float) AS preco_medio_m2_1D,
    CAST(preco_medio_m2_2D AS Float) AS preco_medio_m2_2D,
    CAST(preco_medio_m2_3D AS Float) AS preco_medio_m2_3D,
    CAST(preco_medio_m2_4D AS Float) AS preco_medio_m2_4D
FROM temp_table_sp
"""

df_sp_venda_silver = spark.sql(query)
display(df_sp_venda_silver)

# COMMAND ----------

# DBTITLE 1,Salvando em silver
df_sp_venda_silver.coalesce(1) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.dados_venda.sp_venda_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Rio de Janeiro:
# MAGIC  Dados de Venda

# COMMAND ----------

# DBTITLE 1,Tabela Rio de Janeiro
df_rj_venda = spark.read.table("bronze.dados_rj.rj_venda")
df_rj_venda.display()

# COMMAND ----------

# DBTITLE 1,Separando mês e ano
from pyspark.sql.functions import split

df_rj_venda = df_rj_venda.withColumn("mes", split(df_rj_venda["data"], "/").getItem(0)) \
                        .withColumn("ano", split(df_rj_venda["data"], "/").getItem(1))

df_rj_venda.display()

# COMMAND ----------

from pyspark.sql.functions import split

df_rj_venda = df_rj_venda.withColumn("mes", split(df_rj_venda["data"], ", ").getItem(0)) \
                        .withColumn("ano", split(df_rj_venda["data"], ", ").getItem(1))

df_rj_venda.display()

# COMMAND ----------

df_rj_venda = df_rj_venda.withColumn("mes", when(col("mes") == "mar�o", "março").otherwise(col("mes")))

df_rj_venda.display()

# COMMAND ----------

df_rj_venda = df_rj_venda.withColumn("datas", concat(col("mes"), lit(" "), col("ano").cast("string")))

for column_name in df_rj_venda.columns:
    df_rj_venda = df_rj_venda.withColumn(column_name, regexp_replace(col(column_name), ",", "."))

df_rj_venda.display()

# COMMAND ----------

# Criar uma tabela temporária
df_rj_venda.createOrReplaceTempView("temp_table_rj")

# Executar a conversão com SQL
query = """
SELECT 
    CAST(`datas` AS String) AS datas,
    CAST(`mes` AS String) AS mes,
    CAST(`ano` AS String) AS ano,
    CAST(variacao_total_mensal AS Float) AS variacao_total_mensal,
    CAST(var_mensal_1D AS Float) AS var_mensal_1D,
    CAST(var_mensal_2D AS Float) AS var_mensal_2D,
    CAST(var_mensal_3D AS Float) AS var_mensal_3D,
    CAST(var_mensal_4D AS Float) AS var_mensal_4D,
    CAST(variacao_total_anual AS Float) AS variacao_total_anual,
    CAST(variacao_anual_1D AS Float) AS variacao_anual_1D,
    CAST(variacao_anual_2D AS Float) AS variacao_anual_2D,
    CAST(variacao_anual_3D AS Float) AS variacao_anual_3D,
    CAST(variacao_anual_4D AS Float) AS variacao_anual_4D,
    CAST(preco_medio_m2_total AS Float) AS preco_medio_m2_total,
    CAST(preco_medio_m2_1D AS Float) AS preco_medio_m2_1D,
    CAST(preco_medio_m2_2D AS Float) AS preco_medio_m2_2D,
    CAST(preco_medio_m2_3D AS Float) AS preco_medio_m2_3D,
    CAST(preco_medio_m2_4D AS Float) AS preco_medio_m2_4D
FROM temp_table_rj
"""

df_rj_venda_silver = spark.sql(query)
display(df_rj_venda_silver)

# COMMAND ----------

df_rj_venda_silver.coalesce(1) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.dados_venda.rj_venda_silver")
