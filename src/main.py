import findspark
findspark.init()

from pyspark.sql import SparkSession

# IMPORTAÇÕES DOS MÓDULOS DO PIPELINE
from ingestion import load_data
from processing import transform_data
from analysis import run_all_analysis
from reporting import create_reports

# --- 1. Inicializar a Sessão Spark ---
spark = SparkSession.builder \
    .appName("RestaurantePipeline") \
    .master("local[*]") \
    .getOrCreate()

print("Sessão Spark inicializada com sucesso!")
print("-" * 40)


# --- 2. EXECUTAR O PIPELINE ---
try:
    # 2.1. Ingestão: Lê os dados brutos
    raw_file_path = "data/raw/vendas_simuladas.csv"
    df_raw = load_data(spark, raw_file_path)

    # 2.2. Processamento: Transforma e enriquece
    df_processed = transform_data(df_raw)

    # 2.3. Análise: Executa todos os agrupamentos e salva em data/processed
    run_all_analysis(df_processed)

    # 2.4. Reporting: Gera os gráficos com base nos dados salvos
    create_reports()

except Exception as e:
    print(f"Ocorreu um erro durante a execução do pipeline: {e}")

finally:
    # --- 3. Fechar a Sessão Spark ---
    spark.stop()
    print("-" * 40)
    print("Pipeline de Big Data concluído e Sessão Spark finalizada.")