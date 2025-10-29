import findspark
# O findspark ajuda a inicializar o ambiente Spark no seu Python local
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, hour, dayofweek

# --- 1. Inicializar a Sessão Spark ---
# Esta é a "porta de entrada" para todo o trabalho com PySpark.
# .appName: Nome da aplicação no Spark UI.
# .master("local[*]"): Diz ao Spark para rodar em modo local, usando todos os cores disponíveis
#                      do seu processador (simulando um cluster).
spark = SparkSession.builder \
    .appName("RestauranteETL") \
    .master("local[*]") \
    .getOrCreate()

print("Sessão Spark inicializada com sucesso!")
print("-" * 40)


# --- 2. Ingestão de Dados (Leitura do CSV) ---
# O Spark lê o arquivo e cria um Spark DataFrame.
# .option("header", "true"): Usa a primeira linha como cabeçalho.
# .option("inferSchema", "true"): Tenta adivinhar o tipo de dado de cada coluna (String, Int, Double, etc.).
file_path = "data/raw/vendas_simuladas.csv"
df_vendas = spark.read.csv(file_path, header=True, inferSchema=True)

print("Dados lidos com sucesso. Esquema inicial:")
df_vendas.printSchema() # Mostra o nome e o tipo de cada coluna


# --- 3. Transformação de Dados (Processamento Básico) ---
# Vamos criar novas colunas a partir da coluna 'DataHora' para análise
df_processado = df_vendas.withColumn("DataHora", col("DataHora").cast("timestamp")) \
    .withColumn("Mes", month(col("DataHora"))) \
    .withColumn("Hora", hour(col("DataHora"))) \
    .withColumn("DiaSemana", dayofweek(col("DataHora"))) \
    .withColumn("Ticket_Medio", col("Valor_Total") / col("Quantidade"))


print("-" * 40)
print("Dados processados. Esquema após as transformações:")
df_processado.printSchema()


# --- 4. Exibição e Análise (Ações) ---
print("-" * 40)
print("Amostra dos Dados Processados (Action):")
# O .show(5) é uma "Action" que força o Spark a realmente executar o processamento e mostrar o resultado.
df_processado.show(5)


# --- 5. Fechar a Sessão Spark ---
# É sempre uma boa prática fechar a sessão ao final do script.
spark.stop()
print("-" * 40)
print("Sessão Spark finalizada.")