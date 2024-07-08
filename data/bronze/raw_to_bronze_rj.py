# Databricks notebook source
# DBTITLE 1,Trazendo do raw para bronze dados RJ
# Vamos ver se o dataset tá no arquivo certo
dbutils.fs.ls("/Volumes/raw/dados_rj/dados_rj/")

# COMMAND ----------

df = spark.read.format("csv") \
    .option("delimiter", ";") \
    .option("header", "true") \
    .load("/Volumes/raw/dados_rj/dados_rj/rj_venda.csv/")
df.display()

# COMMAND ----------

# Modificando o nome das colunas
df = df.withColumnRenamed("N�mero-�ndice Total", "numero_indice_total") \
    .withColumnRenamed("N�mero-�ndice 1D", "numero_indice_1D") \
    .withColumnRenamed("N�mero-�ndice 2D", "numero_indice_2D") \
    .withColumnRenamed("N�mero-�ndice 3D", "numero_indice_3D") \
    .withColumnRenamed("N�mero-�ndice 4D", "numero_indice_4D") \
    .withColumnRenamed("Var. mensal (%) Total", "variacao_total_mensal") \
    .withColumnRenamed("Var. mensal (%) 1D", "var_mensal_1D") \
    .withColumnRenamed("Var. mensal (%) 2D", "var_mensal_2D") \
    .withColumnRenamed("Var. mensal (%) 3D", "var_mensal_3D") \
    .withColumnRenamed("Var. mensal (%) 4D", "var_mensal_4D") \
    .withColumnRenamed("Var. em 12 meses (%) Total", "variacao_total_anual") \
    .withColumnRenamed("Var. em 12 meses (%) 1D", "variacao_anual_1D") \
    .withColumnRenamed("Var. em 12 meses (%) 2D", "variacao_anual_2D") \
    .withColumnRenamed("Var. em 12 meses (%) 3D", "variacao_anual_3D") \
    .withColumnRenamed("Var. em 12 meses (%) 4D", "variacao_anual_4D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) Total", "preco_medio_m2_total") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 1D", "preco_medio_m2_1D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 2D", "preco_medio_m2_2D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 3D", "preco_medio_m2_3D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 4D", "preco_medio_m2_4D") \
    .withColumnRenamed("Data", "data")

# COMMAND ----------

# Salvando o dataframe em delta na pasta bronze
df.coalesce(1).write.format("delta").saveAsTable("bronze.dados_rj.rj_venda")

# COMMAND ----------

df_2 = spark.read.format("csv") \
    .option("delimiter", ";") \
    .option("header", "true") \
    .load("/Volumes/raw/dados_rj/dados_rj/rj_aluguel.csv/")
df_2.display()


# COMMAND ----------

df_2 = df_2.withColumnRenamed("N�mero-�ndice Total", "numero_indice_total") \
    .withColumnRenamed("N�mero-�ndice 1D", "numero_indice_1D") \
    .withColumnRenamed("N�mero-�ndice 2D", "numero_indice_2D") \
    .withColumnRenamed("N�mero-�ndice 3D", "numero_indice_3D") \
    .withColumnRenamed("N�mero-�ndice 4D", "numero_indice_4D") \
    .withColumnRenamed("Var. mensal (%) Total", "variacao_total_mensal") \
    .withColumnRenamed("Var. mensal (%) 1D", "var_mensal_1D") \
    .withColumnRenamed("Var. mensal (%) 2D", "var_mensal_2D") \
    .withColumnRenamed("Var. mensal (%) 3D", "var_mensal_3D") \
    .withColumnRenamed("Var. mensal (%) 4D", "var_mensal_4D") \
    .withColumnRenamed("Var. em 12 meses (%) Total", "variacao_total_anual") \
    .withColumnRenamed("Var. em 12 meses (%) 1D", "variacao_anual_1D") \
    .withColumnRenamed("Var. em 12 meses (%) 2D", "variacao_anual_2D") \
    .withColumnRenamed("Var. em 12 meses (%) 3D", "variacao_anual_3D") \
    .withColumnRenamed("Var. em 12 meses (%) 4D", "variacao_anual_4D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) Total", "preco_medio_m2_total") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 1D", "preco_medio_m2_1D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 2D", "preco_medio_m2_2D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 3D", "preco_medio_m2_3D") \
    .withColumnRenamed("Pre�o m�dio (R$/m�) 4D", "preco_medio_m2_4D") \
    .withColumnRenamed("(% - mensalizada) Total", "var.mensalizada_total") \
    .withColumnRenamed("(% - mensalizada) 1D", "var.mensalizada_1D") \
    .withColumnRenamed("(% - mensalizada) 2D", "var.mensalizada_2D") \
    .withColumnRenamed("(% - mensalizada) 3D", "var.mensalizada_3D") \
    .withColumnRenamed("(% - mensalizada) 4D", "var.mensalizada_4D") \
    .withColumnRenamed("Data", "data")

# COMMAND ----------

df_2.coalesce(1).write.format("delta").saveAsTable("bronze.dados_rj.rj_aluguel")
