# Construção de uma medalion architecture no Databricks + Google Cloud

A partir de dados obtidos na FIPE (Fundação Instituto de pesquisas econômicas), sobre a relação de preços do metro de imóveis em São Paulo e outras cidades do Brasil, construimos um datalake e montamos um medalion architecture ralizando uma external location com *buckets* no GCP.

O processo completo de ingestão de dados foi o seguinte:
raw -> bronze -> silver -> gold

# Configuração do Databricks
Criei um cluster usando a versão trial do databricks e criamos novo workspace e nosso catalog para ingestão das tabelas. Após isso setamos nosso external location com o GCP fazendo uma conexão com o buckets que continha todas os nossos arquivos csv.
Fiz o volume dos dados na pasta *raw*.

* Cluster criado

![cluster criado](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/5665988d-3d99-4925-9c06-3c642c26d217)

* Ingestão dos dados no raw com external location

![criando o volume com o external location](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/1a1c4c65-20d5-42b5-978a-50a68710ab12)


* Nosso catalog ficou assim

![raw com external location](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/e58f9963-f80d-4199-9153-84fa98228b55)



# Leitura dos dados e envio para bronze
Com os dados em *raw*, fizemos nosso primeiro notebook e realizamos a leitura dos dados usando "spark" e modificamos os nomes das colunas do dataframe:

```python
# Nosso código para pegar e ler os dados em csv salvos no nosso raw que pegamos do bucket
df = spark.read.format("csv") \
  .option("delimiter", ";") \
  .option("header", "true") \
  .load("/Volumes/raw/dados_sp/dados_sp/sp_venda.csv")
```

- As colunas continha caracteres invalidos, impossibilitando o salvamento em bronze.

```python
# Mudei os nomes das colunas com esse código
df = df.withColumnRenamed("Numero-indice Total", "numero_indice_total") \
    .withColumnRenamed("Numero-indice 1D", "numero_indice_1D") \
    .withColumnRenamed("Numero-indice 2D", "numero_indice_2D") \
    .withColumnRenamed("Numero-indice 3D", "numero_indice_3D") \
    .withColumnRenamed("Numero-indice 4D", "numero_indice_4D") \
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
    .withColumnRenamed("Preco medio (R$/m2) Total", "preco_medio_m2_total") \
    .withColumnRenamed("Preco medio (R$/m2) 1D", "preco_medio_m2_1D") \
    .withColumnRenamed("Preco medio (R$/m2) 2D", "preco_medio_m2_2D") \
    .withColumnRenamed("Preco medio (R$/m2) 3D", "preco_medio_m2_3D") \
    .withColumnRenamed("Preco medio (R$/m2) 4D", "preco_medio_m2_4D") \
    .withColumnRenamed("Data", "data")
```
  
- Após a modificação dos nomes das colunas conseguimos por fim salvar em bronze.
```python
# Salvamos diretamente no bronze
df.coalesce(1).write.format("delta").saveAsTable("bronze.dados_sp.sp_venda")
```
- Fizemos o mesmo procedimento com os dados de aluguel e após isso o mesmo procedimento com os dados coletados do Rio de Janeiro.
