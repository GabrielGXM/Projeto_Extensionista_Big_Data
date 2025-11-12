from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, format_string, concat, lit, when

# Função auxiliar para formatar os resultados (reutilização de código)
def format_and_save(df: DataFrame, group_col: str, sum_col: str, output_path: str, order_col: str, is_desc: bool = True):
    """
    Função genérica para formatar o valor monetário e salvar o resultado.
    """
    df_final = df.withColumn(
        "Receita_Formatada",
        concat(lit("R$ "), format_string("%.2f", col(sum_col)))
    ).select(group_col, "Receita_Formatada").sort(col(order_col).desc() if is_desc else col(order_col).asc())

    # Salva o DataFrame no formato CSV na pasta processed
    df_final.coalesce(1).write.csv(
        path=output_path, 
        mode="overwrite", 
        header=True
    )
    print(f"Resultado da análise ('{group_col}') salvo em: {output_path}")
    df_final.show(10, truncate=False)


def run_all_analysis(df_processado: DataFrame):
    """
    Executa todas as análises de agrupamento do projeto.
    """
    print("-" * 40)
    print("INÍCIO DAS ANÁLISES E SALVAMENTO EM data/processed")
    print("-" * 40)

    # --- Análise 1: Receita por Categoria ---
    df_cat = df_processado.groupBy("Categoria").agg(sum("Valor_Total").alias("Receita_Total"))
    format_and_save(
        df_cat, "Categoria", "Receita_Total", 
        "data/processed/receita_por_categoria", 
        "Receita_Total", is_desc=True
    )

    # --- Análise 2: Receita por Hora do Dia ---
    df_hora = df_processado.groupBy("Hora").agg(sum("Valor_Total").alias("Receita_Horaria"))
    format_and_save(
        df_hora, "Hora", "Receita_Horaria", 
        "data/processed/receita_por_hora", 
        "Hora", is_desc=False
    )

    # --- Análise 3: Receita por Dia da Semana ---
    # Adicionando a lógica WHEN para mapear o DiaSemana (1=Dom, 7=Sáb)
    df_dia = df_processado.groupBy("DiaSemana").agg(sum("Valor_Total").alias("Receita_Semanal"))
    df_dia = df_dia.withColumn(
        "Dia", 
        when(col("DiaSemana") == 1, "Domingo")
        .when(col("DiaSemana") == 2, "Segunda")
        .when(col("DiaSemana") == 3, "Terça")
        .when(col("DiaSemana") == 4, "Quarta")
        .when(col("DiaSemana") == 5, "Quinta")
        .when(col("DiaSemana") == 6, "Sexta")
        .when(col("DiaSemana") == 7, "Sábado")
        .otherwise("Desconhecido")
    ).select("Dia", "Receita_Semanal","DiaSemana") # Apenas as colunas que importam para format_and_save

    format_and_save(
        df_dia, "Dia", "Receita_Semanal", 
        "data/processed/receita_por_dia_semana", 
        "DiaSemana", is_desc=False
    )

    # --- Análise 4: Lucratividade por Item (Foco no Ticket Médio) ---
    df_lucratividade = df_processado.groupBy("Item").agg(
        sum("Valor_Total").alias("Receita_Total_Item"),
        sum("Quantidade").alias("Qtd_Vendida")
    ).withColumn(
        "Ticket_Medio_Item", 
        col("Receita_Total_Item") / col("Qtd_Vendida")
    ).sort(col("Ticket_Medio_Item").desc())

    # Formatação especial para Ticket Médio
    df_final_lucratividade = df_lucratividade.withColumn(
        "Ticket_Medio_Formatado",
        concat(lit("R$ "), format_string("%.2f", col("Ticket_Medio_Item")))
    ).select("Item", "Qtd_Vendida", "Ticket_Medio_Formatado")

    df_final_lucratividade.coalesce(1).write.csv(
        path="data/processed/lucratividade_por_item", 
        mode="overwrite", 
        header=True
    )
    print(f"Resultado da análise ('Lucratividade') salvo em: data/processed/lucratividade_por_item")
    df_final_lucratividade.show(10, truncate=False)