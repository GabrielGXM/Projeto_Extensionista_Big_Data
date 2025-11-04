from pyspark.sql import DataFrame
from pyspark.sql.functions import col, month, hour, dayofweek

def transform_data(df: DataFrame):
    """
    Aplica as transformações de ETL: cast, criação de colunas de tempo e Ticket Médio.
    """
    print("--- 3. Transformação de Dados ---")
    # Converte DataHora para tipo timestamp e cria colunas de tempo
    df_processado = df.withColumn("DataHora", col("DataHora").cast("timestamp")) \
        .withColumn("Mes", month(col("DataHora"))) \
        .withColumn("Hora", hour(col("DataHora"))) \
        .withColumn("DiaSemana", dayofweek(col("DataHora"))) \
        .withColumn("Ticket_Medio", col("Valor_Total") / col("Quantidade"))
    
    print("Dados enriquecidos (colunas de tempo e ticket médio criadas).")
    return df_processado