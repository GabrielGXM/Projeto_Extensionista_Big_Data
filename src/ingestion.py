from pyspark.sql import SparkSession

def load_data(spark: SparkSession, file_path: str):
    """
    Lê o arquivo CSV bruto e retorna o DataFrame do PySpark.
    """
    print("--- 2. Ingestão de Dados ---")
    df_vendas = spark.read.csv(
        file_path, 
        header=True, 
        inferSchema=True
    )
    print("Dados brutos lidos com sucesso.")
    df_vendas.printSchema()
    return df_vendas