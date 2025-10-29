import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO DO DATASET ---
# Simular dados de 6 meses (ex: Jan a Jun)
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 6, 30)
num_records = 50000  # 50.000 linhas de transações (simulando Big Data em escala menor)

# Menu do Restaurante
menu = {
    'Bebida': ['Refrigerante', 'Água Mineral', 'Suco Natural', 'Cerveja Artesanal'],
    'Entrada': ['Batata Frita', 'Pão de Alho', 'Pastelzinho Misto'],
    'Prato Principal': ['Bife à Parmegiana', 'Salmão Grelhado', 'Risoto de Camarão', 'Pizza de Calabresa'],
    'Sobremesa': ['Pudim', 'Mousse de Chocolate', 'Sorvete'],
}

precos = {
    'Refrigerante': 6.00, 'Água Mineral': 4.00, 'Suco Natural': 8.00, 'Cerveja Artesanal': 25.00,
    'Batata Frita': 15.00, 'Pão de Alho': 12.00, 'Pastelzinho Misto': 18.00,
    'Bife à Parmegiana': 45.00, 'Salmão Grelhado': 60.00, 'Risoto de Camarão': 55.00, 'Pizza de Calabresa': 40.00,
    'Pudim': 10.00, 'Mousse de Chocolate': 12.00, 'Sorvete': 9.00,
}

# --- 2. GERAÇÃO DE DADOS ---
data_list = []
for i in range(num_records):
    # Data e Hora Aleatórias
    time_delta = end_date - start_date
    random_days = np.random.randint(time_delta.days)
    random_seconds = np.random.randint(86400) # 24*60*60
    data_hora = start_date + timedelta(days=random_days, seconds=random_seconds)

    # Escolher Categoria e Item
    categoria = np.random.choice(list(menu.keys()), p=[0.3, 0.2, 0.4, 0.1]) # Probabilidade de venda
    item = np.random.choice(menu[categoria])

    # Detalhes da Venda
    preco_unitario = precos[item]
    quantidade = np.random.randint(1, 4) # Vende 1 a 3 unidades do item

    data_list.append({
        'ID_Transacao': f'T{i+1:05}',
        'DataHora': data_hora,
        'Item': item,
        'Categoria': categoria,
        'Preco_Unitario': preco_unitario,
        'Quantidade': quantidade,
        'Valor_Total': preco_unitario * quantidade,
    })

df = pd.DataFrame(data_list)

# --- 3. SALVAR DATASET NO LOCAL CORRETO ---
# O local correto é a pasta raw/
output_path = 'data/raw/vendas_simuladas.csv'
df.to_csv(output_path, index=False)

print(f"Dataset de {len(df)} transações gerado com sucesso!")
print(f"Salvo em: {output_path}")

# Exemplo de como o dataset ficou (opcional)
# print("\nPrimeiras 5 linhas do DataFrame:")
# print(df.head())