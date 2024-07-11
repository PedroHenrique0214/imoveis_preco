# Construção de uma medalion architecture no Databricks + Google Cloud

### Introdução

A partir de dados obtidos na FIPE (Fundação Instituto de pesquisas econômicas), sobre a relação de preços do metro de imóveis em São Paulo e Rio de Janeiro, construi um datalake e montei um medalion architecture ralizando uma external location com *buckets* no GCP. Após isso criei um workflow com o databricks para automatizar o processo apenas atualizando os dados subindo os mesmos no bucket e realizei uma analise em cima dos dados obtidos através da última camada para encontrar alguns insights relevantes dos dados. 

Ao final do projeto respondi algumas questões como:
- "Qual o valor do metro quadrado ao longo dos anos? E como a variação foi diferente em cada cidade?"
- "Qual a relação da quantidade de dormitórios com o preço do metro quadrado?"
- "Existe uma tendência de preço que acompanha ambas as cidades?"

São questões simples, mas o projeto tem como principal objeto a criação de um datalake utilizando o Databricks + Google Cloud, o processo de ETL (Extração, transformação e Carregamento) dos dados e a criação de um workflow.

Link do site da FIPE: https://www.fipe.org.br/

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
Com os dados em *raw*, fizemos nosso primeiro notebook e realizamos a leitura dos dados usando *spark* e modificamos os nomes das colunas do dataframe:

```python
# Nosso código para pegar e ler os dados em csv salvos no nosso raw que pegamos do bucket
df = spark.read.format("csv") \
  .option("delimiter", ";") \
  .option("header", "true") \
  .load("/Volumes/raw/dados_sp/dados_sp/sp_venda.csv")
```

- As colunas continham caracteres invalidos, impossibilitando o salvamento em bronze, então usei uma função spark para renomear o nome das colunas.

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
  
- Após a modificação dos nomes das colunas conseguimos por fim salvar em bronze. Salvei os dados no formato delta no catalogo do databricks como tabela, para ficar acessível para quem quiser atualizar os dados diretamente de bronze para outros propósitos.
```python
# Salvamos diretamente no bronze
df.coalesce(1).write.format("delta").saveAsTable("bronze.dados_sp.sp_venda")
```
- Fizemos o mesmo procedimento com os dados de aluguel e após isso o mesmo procedimento com os dados coletados do Rio de Janeiro.

# Tratando os dados para a camada Silver

No tratamento dos dados de bronze para silver, realizei algumas modificações nos dados e criei novas colunas para facilitar uma futura análise.

- Criação de novas colunas de mês e ano
```python
df_sp_venda = df_sp_venda.withColumn("mes", split(df_sp_venda["data"], ", ").getItem(0)) \
                        .withColumn("ano", split(df_sp_venda["data"], ", ").getItem(1))

df_sp_venda.display()
```

- Criei uma tabela temporária em SQL para realizar algumas conversões nos dados e alterar a forma da minha tabela:
```python
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
```


- Realizei mais alguns tratamentos nos dados, pois os mesmo continham caractéres invalidos no dataframe, que impossibilitavam a conversão dos mesmos. Esse foi um processo demorado, pois muitas strings continham pontos ('.') e informações inválidas ('mar�o. 2008'), impossibilitando a transformação em float ou date. Após muitas transformações no meu dataset, consegui realizar as modificações que gostaria e realizei o salvamento dos dados em Silver. O código completo vocês poderão ver nos files.

- Eu também alterei a forma como os dados foram salvos. Se em bronze, os dados estavem separados por cidade, na camada silver os dados já estavam separados por 'venda' e 'aluguel', do modo como serão feitas as análises na camada gold.

# Transformando os dados para a camada Gold

- Importei os dados da minha camada silver com spark para fazer as transformações finais e enviar para a camada gold as minhas duas tabelas, venda e aluguel.

- Importei algumas funções do spark para realizar o arredondamento dos valores floats para apenas duas casas decimais após a vírgula, para facilitar a visualização dos dados.
```python
from pyspark.sql.functions import round, col
from pyspark.sql.types import DoubleType, FloatType

for column_name in df_sp.columns:
    if isinstance(df_sp.schema[column_name].dataType, (DoubleType, FloatType)):
        df_sp = df_sp.withColumn(column_name, round(col(column_name), 4))


for column_name in df_rj.columns:
    if isinstance(df_rj.schema[column_name].dataType, (DoubleType, FloatType)):
        df_rj = df_rj.withColumn(column_name, round(col(column_name), 4))
```

- Após isso, criei uma nova coluna em cada tabela, de São Paulo e Rio de Janeiro, informando o nome da cidade em cada uma delas, para realizar após isso a união de ambas e criar uma única tabela de venda com as informações de ambas as cidades. Utilizei a função lit do spark e após isso a union para fundir as duas tabelas, como no exemplo a seguir.
```python
from pyspark.sql.functions import lit

# Colocando uma coluna com a cidade
df_sp = df_sp.withColumn("Cidade", lit("São Paulo"))

df_rj = df_rj.withColumn("Cidade", lit("Rio de Janeiro"))

# Unindo as duas tabelas
df_vendas = df_sp.union(df_rj)

# Ordena pelo campo 'date'
df_vendas = df_vendas.orderBy("datas")
```

- Realizei o mesmo procedimento para a tabela de aluguel e salvei ambas. Pronto, temos nossas informações prontas para realizar um análise do mercado imobiliário dessas duas cidades.

*Workflow*

Assim ficou o fluxo final de ETL dos nossos dados no databricks. Foi craido um job para que os notebooks rodem automáticamente para cada atualização dos nossos dados em raw, para manter as informações atualizadas para uma futura análise do mercado imobiliário.
![job](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/678c2bf9-0cfe-4b04-8276-afac9ffc0872)

Em um futuro interesse, podem ser colocados dados de direfentes capitais do Brasil, para que se possa aumentar a capacidade de nossa análise, criando mais jobs e automatizando todas essas informações no nosso workflow.


# Análise dos dados

Após a conclusão de todo o processo de criação do datalake e do medallion archtecture, fiz uma breve análise em cima dos dados transformados. Criei um notebook no databricks e utilizei comandos SQL e funções do próprio databricks para criar gráficos em cima dos dados, possiblitando responder algumas de nossas questões iniciais. Os códigos em SQL podem ser vistos na pasta gold do repositório.

*Análise de Venda*

- Preço médio do metro quadrado

![m2_preco](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/0a86c44c-c55d-459a-b2f2-0837e00c95e8)

- Podemos notar até alguns anos atrás, o preço do metro quadrado era maior no Rio de Janeiro, apenas algum tempo atrás que São Paulo obteve um preço maior em relação a outra cidade. Apesar disso, podemos notar que a tendência de aumento de preço se manteve em ambas as cidades.


- Preço por dormitório

![preco_por_dormitorio](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/747bda25-6a76-4759-a07a-c33208c36e14)

- Habitações com 1 dormitório tendem a ser mais caros do que os que possuem 2 ou 3, mas a situação muda com habitações com 4 dormitórios.


- Variação anual do valor do metro quadrado

![var_m2_venda](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/37b0abdf-bb1a-413a-8367-04785b315101)

- O preço do metro quadrado deu apenas uma pequena diminuida na cidade do Rio de Janeiro durante os anos de 2015 até 2019, período que São Paulo, ainda com aumentos, obteve o posto de maior preço do metro quadrado entre as duas cidades.


*Análise de Aluguel*

- Preço médio por metro quadrado

![preco_medio_aluguel_m2](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/35b698ad-2889-4e6a-9aad-6befd34841e2)

- A situação é parecida com a de venda, com o preço para aluguel do metro quadrado de São Paulo apenas superando o Rio de Janeiro nos últimos anos.


- Preço por dormitório

![m2_por_quarto_aluguel](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/9e3ba504-ff9b-4637-b804-18671bef55ab)

- Novamente as tendências são parecidas de venda e aluguel por metro quadrado.


*Dashboard de ambas as análises feitas no databricks*

- Venda
![dashboard de vendas](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/b26efdf7-0c4c-42e1-a0e6-34db8885c23f)


- Aluguel
![dashboard de aluguel](https://github.com/PedroHenrique0214/imoveis_preco/assets/155765414/26bd4fba-44da-4002-b7fe-20597053cfb9)
