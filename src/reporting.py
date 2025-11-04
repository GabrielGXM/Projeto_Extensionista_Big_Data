import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# Configurações de Caminho
REPORTS_PATH = "reports/figures"

# --- Funções Auxiliares ---

def load_and_clean_data(analysis_name: str, numeric_col: str, sort_col: str = None, is_monetary: bool = True):
    """
    Carrega o arquivo CSV processado, limpa a coluna monetária e ordena o DataFrame.
    """
    data_path = f"data/processed/{analysis_name}"
    
    try:
        csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
        if not csv_files:
            print(f"ERRO: Nenhum arquivo CSV encontrado em {data_path}.")
            return None

        csv_file = os.path.join(data_path, csv_files[0])
        df_report = pd.read_csv(csv_file)
        
            # 1. Leitura e Conversão Numérica (se for valor monetário)
        if is_monetary:
            # CORREÇÃO CRÍTICA: Busca por colunas que terminem com '_Formatada' OU '_Formatado'
            # Isso cobre tanto 'Receita_Formatada' quanto 'Ticket_Medio_Formatado'
            colunas_formatadas = [
                c for c in df_report.columns 
                if c.endswith('_Formatada') or c.endswith('_Formatado')
            ]
            
            if not colunas_formatadas:
                print(f"ERRO: Nenhuma coluna formatada encontrada para {analysis_name} em {df_report.columns}.")
                return None
            
            coluna_formatada = colunas_formatadas[0] # Pega a primeira coluna encontrada

            df_report['Valor_Numerico'] = (
                df_report[coluna_formatada]
                .str.replace('R$ ', '', regex=False)
                .str.replace(',', '.', regex=False)
            ).astype(float)
        
        # 2. Ordenação (se for especificada)
        if sort_col:
            # CORREÇÃO CRÍTICA: Ordenamos pela coluna numérica limpa ('Valor_Numerico')
            df_report = df_report.sort_values(by='Valor_Numerico', ascending=False)
            
        return df_report
        
    except Exception as e:
        print(f"Erro ao carregar ou limpar dados de {analysis_name}: {e}")
        return None

# --- Funções de Geração de Gráficos ---

def plot_receita_por_categoria():
    """Gera o gráfico de barras para Receita por Categoria."""
    df = load_and_clean_data("receita_por_categoria", "Receita_Total", sort_col='Valor_Numerico')
    if df is None: return

    plt.figure(figsize=(10, 6))
    plt.bar(df['Categoria'], df['Valor_Numerico'], color=['#3D9970', '#FF4136', '#FFDC00', '#0074D9'])
    
    plt.title('1. Receita Total por Categoria de Produto', fontsize=16)
    plt.xlabel('Categoria', fontsize=12)
    plt.ylabel('Receita Total (R$)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_PATH, "1_receita_por_categoria.png"))
    print("Gráfico 1 salvo: receita_por_categoria.png")


def plot_receita_por_hora():
    """Gera o gráfico de linha para Receita por Hora do Dia."""
    df = load_and_clean_data("receita_por_hora", "Receita_Horaria", sort_col='Hora', is_monetary=True)
    if df is None: return

    plt.figure(figsize=(12, 6))
    plt.plot(df['Hora'], df['Valor_Numerico'], marker='o', linestyle='-', color='#0074D9')
    
    plt.title('2. Tendência de Receita por Hora do Dia (10h às 23h)', fontsize=16)
    plt.xlabel('Hora do Dia (24h)', fontsize=12)
    plt.ylabel('Receita Total (R$)', fontsize=12)
    plt.xticks(np.arange(df['Hora'].min(), df['Hora'].max() + 1)) # Mostra todas as horas
    plt.grid(axis='both', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_PATH, "2_receita_por_hora.png"))
    print("Gráfico 2 salvo: receita_por_hora.png")


def plot_receita_por_dia_semana():
    """Gera o gráfico de barras para Receita por Dia da Semana."""
    df = load_and_clean_data("receita_por_dia_semana", "Receita_Semanal", sort_col='Receita_Semanal')
    if df is None: return

    # Define a ordem correta para os dias da semana
    day_order = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    df['Dia'] = pd.Categorical(df['Dia'], categories=day_order, ordered=True)
    df = df.sort_values('Dia')

    plt.figure(figsize=(10, 6))
    plt.bar(df['Dia'], df['Valor_Numerico'], color='#FF851B')
    
    plt.title('3. Receita Total por Dia da Semana', fontsize=16)
    plt.xlabel('Dia da Semana', fontsize=12)
    plt.ylabel('Receita Total (R$)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_PATH, "3_receita_por_dia_semana.png"))
    print("Gráfico 3 salvo: receita_por_dia_semana.png")


def plot_ticket_medio_por_item(top_n=15):
    """Gera o gráfico de barras horizontais para os top N itens com maior Ticket Médio."""
    # Note que a coluna Ticket_Medio_Formatado não possui o prefixo 'Receita_Formatada',
    # e a coluna numérica final que queremos é Ticket_Medio_Item.
    df = load_and_clean_data(
        "lucratividade_por_item", 
        "Ticket_Medio_Item", 
        sort_col='Ticket_Medio_Item', 
        is_monetary=True
    )
    if df is None: return

    # Pega apenas os Top N itens
    df_top = df.head(top_n).sort_values(by='Valor_Numerico', ascending=True)

    plt.figure(figsize=(10, 8))
    # Gráfico de barras horizontal
    plt.barh(df_top['Item'], df_top['Valor_Numerico'], color='#2ECC40')
    
    # Adiciona o Ticket Médio ao lado das barras
    for index, value in enumerate(df_top['Valor_Numerico']):
        plt.text(value, index, f'R$ {value:.2f}', va='center')

    plt.title(f'4. Top {top_n} Itens por Ticket Médio (Itens Mais Lucrativos)', fontsize=16)
    plt.xlabel('Ticket Médio (R$)', fontsize=12)
    plt.ylabel('Item', fontsize=12)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS_PATH, "4_ticket_medio_por_item.png"))
    print(f"Gráfico 4 salvo: ticket_medio_por_item.png (Top {top_n})")


def create_reports():
    """Função principal que orquestra a criação de todos os relatórios."""
    print("-" * 40)
    print("INÍCIO DA GERAÇÃO DE RELATÓRIOS (MATPLOTLIB/PANDAS)")
    print("-" * 40)

    # Cria a pasta de relatórios, se não existir
    os.makedirs(REPORTS_PATH, exist_ok=True)

    plot_receita_por_categoria()
    plot_receita_por_hora()
    plot_receita_por_dia_semana()
    plot_ticket_medio_por_item()

    print("-" * 40)
    print("Geração de relatórios concluída.")

if __name__ == "__main__":
    create_reports()