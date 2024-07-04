# Construção de uma medalion architecture no Databricks + Google Cloud

A partir de dados obtidos na FIPE (Fundação Instituto de pesquisas econômicas), sobre a relação de preços do metro de imóveis em São Paulo e outras cidades do Brasil, construimos um datalake e montamos um medalion architecture ralizando uma external location com *buckets* no GCP.

O processo completo de ingestão de dados foi o seguinte:
raw -> bronze -> silver -> gold

# Configuração do Databricks
Criei um cluster usando a versão trial do databricks e criamos novo workspace e nosso catalog para ingestão das tabelas. Após isso setamos nosso external location com o GCP fazendo uma conexão com o buckets que continha todas os nossos arquivos csv.
Fiz o volume dos dados na pasta *raw*.

Cluster criado 
![cluster criado](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/5665988d-3d99-4925-9c06-3c642c26d217)


# Leitura dos dados e envio para bronze
Com os dados em *raw*, fizemos nosso primeiro notebook e realizamos a leitura dos dados usando "spark" e modificamos os nomes das colunas do dataframe:
- As colunas continha caracteres invalidos, impossibilitando o salvamento em bronze.
- Após a modificação dos nomes das colunas conseguimos por fim salvar em bronze.


