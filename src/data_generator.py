import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO DO DATASET ---
# Simular dados de 6 meses (ex: Jan a Jun)
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 6, 30)
num_records = 50000  # 50.000 linhas de transações (simulando Big Data em escala menor)

#Horario de funcionamento

HORA_INICIO = 10
HORA_FIM = 18

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

# Diferença total em dias
days_range = (end_date - start_date).days

# Total de segundos entre 10:00 e 18:00 (8 horas * 3600 segundos/hora)
seconds_in_operation = (HORA_FIM - HORA_INICIO) * 3600

for i in range(num_records):
    # 1. Escolhe um dia aleatório dentro do range
    random_days = np.random.randint(days_range)
    
    # 2. Escolhe um segundo aleatório DENTRO do horário de funcionamento
    random_seconds_in_window = np.random.randint(seconds_in_operation)
    
    # 3. Calcula o offset: Dia inicial + dias aleatórios + (10h em segundos) + segundos aleatórios
    # data_base = start_date (01/01/2025 00:00:00)
    data_hora = start_date + \
                timedelta(days=random_days) + \
                timedelta(hours=HORA_INICIO) + \
                timedelta(seconds=random_seconds_in_window)

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