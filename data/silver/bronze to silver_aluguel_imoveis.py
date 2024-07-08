# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ### São Paulo:
# MAGIC  Dados de Aluguel

# COMMAND ----------

# DBTITLE 1,Pegando a tabela
df_sp_aluguel = spark.read.table("bronze.dados_sp.sp_aluguel")
df_sp_aluguel.display()

# COMMAND ----------

# DBTITLE 1,Criando a coluna ano e mês
from pyspark.sql.functions import split

df_sp_aluguel = df_sp_aluguel.withColumn("mes", split(df_sp_aluguel["data"], ", ").getItem(0)) \
                        .withColumn("ano", split(df_sp_aluguel["data"], ", ").getItem(1))

df_sp_aluguel.display()

# COMMAND ----------

# DBTITLE 1,Substituindo 'mar�o' por 'março'
from pyspark.sql.functions import when, col

df_sp_aluguel = df_sp_aluguel.withColumn("mes", when(col("mes") == "mar�o", "março").otherwise(col("mes")))

# COMMAND ----------

# DBTITLE 1,Juntando a coluna mês e ano
from pyspark.sql.functions import col, lit, concat

df_sp_aluguel = df_sp_aluguel.withColumn("datas", concat(col("mes"), lit(" "), col("ano").cast("string")))

# COMMAND ----------

# DBTITLE 1,Mudando o nome de algumas colunas
df_sp_aluguel = df_sp_aluguel \
    .withColumnRenamed("var.mensalizada_total", "var_mensalizada_total") \
    .withColumnRenamed("var.mensalizada_1D", "var_mensalizada_1D") \
    .withColumnRenamed("var.mensalizada_2D", "var_mensalizada_2D") \
    .withColumnRenamed("var.mensalizada_3D", "var_mensalizada_3D") \
    .withColumnRenamed("var.mensalizada_4D", "var_mensalizada_4D")

# COMMAND ----------

# DBTITLE 1,Trocando "," por "."
from pyspark.sql.functions import regexp_replace, col

# Ensure column names with special characters are enclosed in backticks
for column_name in df_sp_aluguel.columns:
    df_sp_aluguel = df_sp_aluguel.withColumn(column_name, regexp_replace(col(column_name), ",", "."))

display(df_sp_aluguel)

# COMMAND ----------

# DBTITLE 1,Criando nossa tabela silver
# Criar uma tabela temporária
df_sp_aluguel.createOrReplaceTempView("temp_table_sp")

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
    CAST(preco_medio_m2_4D AS Float) AS preco_medio_m2_4D,
    CAST(var_mensalizada_total AS Float) AS var_mensalizada_total,
    CAST(var_mensalizada_1D AS Float) AS var_mensalizada_1D,
    CAST(var_mensalizada_2D AS Float) AS var_mensalizada_2D,
    CAST(var_mensalizada_3D AS Float) AS var_mensalizada_3D,
    CAST(var_mensalizada_4D AS Float) AS var_mensalizada_4D
FROM temp_table_sp
"""

df_sp_aluguel_silver = spark.sql(query)
display(df_sp_aluguel_silver)

# COMMAND ----------

# DBTITLE 1,Salvando em silver
df_sp_aluguel_silver.coalesce(1) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.dados_aluguel.sp_aluguel_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rio de Janeiro:
# MAGIC  Dados de Aluguel

# COMMAND ----------

df_rj_aluguel = spark.read.table("bronze.dados_rj.rj_aluguel")
df_rj_aluguel.display()

# COMMAND ----------

from pyspark.sql.functions import split

df_rj_aluguel = df_rj_aluguel.withColumn("mes", split(df_rj_aluguel["data"], ", ").getItem(0)) \
                        .withColumn("ano", split(df_rj_aluguel["data"], ", ").getItem(1))

df_rj_aluguel.display()

# COMMAND ----------

from pyspark.sql.functions import when, col

df_rj_aluguel = df_rj_aluguel.withColumn("mes", when(col("mes") == "mar�o", "março").otherwise(col("mes")))

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

df_rj_aluguel = df_rj_aluguel.withColumn("datas", concat(col("mes"), lit(" "), col("ano").cast("string")))

# COMMAND ----------

df_rj_aluguel = df_rj_aluguel \
    .withColumnRenamed("var.mensalizada_total", "var_mensalizada_total") \
    .withColumnRenamed("var.mensalizada_1D", "var_mensalizada_1D") \
    .withColumnRenamed("var.mensalizada_2D", "var_mensalizada_2D") \
    .withColumnRenamed("var.mensalizada_3D", "var_mensalizada_3D") \
    .withColumnRenamed("var.mensalizada_4D", "var_mensalizada_4D")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# Ensure column names with special characters are enclosed in backticks
for column_name in df_rj_aluguel.columns:
    df_rj_aluguel = df_rj_aluguel.withColumn(column_name, regexp_replace(col(column_name), ",", "."))

display(df_rj_aluguel)

# COMMAND ----------

# Criar uma tabela temporária
df_rj_aluguel.createOrReplaceTempView("temp_table_rj")

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
    CAST(preco_medio_m2_4D AS Float) AS preco_medio_m2_4D,
    CAST(var_mensalizada_total AS Float) AS var_mensalizada_total,
    CAST(var_mensalizada_1D AS Float) AS var_mensalizada_1D,
    CAST(var_mensalizada_2D AS Float) AS var_mensalizada_2D,
    CAST(var_mensalizada_3D AS Float) AS var_mensalizada_3D,
    CAST(var_mensalizada_4D AS Float) AS var_mensalizada_4D
FROM temp_table_rj
"""

df_rj_aluguel_silver = spark.sql(query)
display(df_rj_aluguel_silver)

# COMMAND ----------

df_rj_aluguel_silver.coalesce(1) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.dados_aluguel.rj_aluguel_silver")
