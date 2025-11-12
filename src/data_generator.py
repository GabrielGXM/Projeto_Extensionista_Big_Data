import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO DO DATASET ---
# Simular dados de 6 meses (ex: Jan a Jun)
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 6, 30)
num_records = 30000  # 50.000 linhas de transações (simulando Big Data em escala menor)

#Horario de funcionamento

HORA_INICIO = 10
HORA_FIM = 18

# Menu do Restaurante
menu = {
    'Cervejas': [
        'Brahma', 'Antarctica', 'Skol', 'Bohemia', 
        'Heineken 600ml', 'Budweiser Lata'
    ],
    'Bebidas_Nao_Alcoolicas': [
        'Refrigerante Lata 350ml', 'Refrigerante 600ml Guaraná', 
        'Refrigerante 2lt Coca-Cola', 'Suco de Laranja', 
        'Água s/ Gás', 'Limonada Suíça', 'Outros (Lata/Cop)', 'H2O'
    ],
    'Petiscos': [
        'Bolinho de Bacalhau (10 un)', 'Batata Frita', 
        'Filé de Frango Aperitivo', 'Peixe Frito (Porção)', 
        'Torresmo (porção)', 'Salsichão', 'Kibe (Porção)'
    ],
    'Pratos_Principais': [
        'Bacalhau à moda da casa', 'Picanha', 
        'Churrasco Simples', 'Contra Filé Simples', 
        'Contra Filé à Osvaldo Aranha', 'Filé de Frango à Milanesa', 
        'Bife à Milanesa', 'Feijoada Completa (Sexta/Sábado)' # Adicionado Feijoada
    ],
    'Lanches': [
        'Café', 'Misto Quente', 'Pão c/ Ovo', 
        'Pão c/ Manteiga na Chapa', 'Sanduíche Queijo Prato'
    ],
    'Guarnicoes_e_Saladas': [
        'Arroz', 'Arroz com Brócolis', 'Feijão', 'Farofa', 
        'Salada de Palmito (Grande)', 'Salada de Maionese'
    ],
    'Drinks_e_Destilados': [
        'Caipirinha', 'Cuba Libre', 'Martini', 'Vodka Ice', 
        'Velho Barreiro', 'Ballantine’s', 'Whisky Red Label'
    ],
    'Sobremesas': [
        'Pudim', 'Torta', 'Romeu e Julieta'
    ]
}

precos = {
    # Cervejas
    'Brahma': 11.00, 'Antarctica': 11.00, 'Skol': 11.00, 'Bohemia': 15.00, 'Heineken 600ml': 18.00, 'Budweiser Lata': 7.00,
    # Não-Alcoólicas
    'Refrigerante Lata 350ml': 6.00, 'Refrigerante 600ml Guaraná': 9.00, 'Refrigerante 2lt Coca-Cola': 18.00, 
    'Suco de Laranja': 10.00, 'Água s/ Gás': 3.00, 'Limonada Suíça': 30.00, 'Outros (Lata/Cop)': 7.00, 'H2O': 7.00,
    # Petiscos
    'Bolinho de Bacalhau (10 un)': 35.00, 'Batata Frita': 15.00, 'Filé de Frango Aperitivo': 39.90, 
    'Peixe Frito (Porção)': 28.00, 'Torresmo (porção)': 14.90, 'Salsichão': 20.00, 'Kibe (Porção)': 20.00,
    # Pratos Principais
    'Bacalhau à moda da casa': 60.00, 'Picanha': 50.00, 'Churrasco Simples': 30.00, 
    'Contra Filé Simples': 33.00, 'Contra Filé à Osvaldo Aranha': 33.00, 'Filé de Frango à Milanesa': 26.00, 
    'Bife à Milanesa': 26.00, 'Feijoada Completa (Sexta/Sábado)': 25.00,
    # Lanches
    'Café': 3.00, 'Misto Quente': 9.00, 'Pão c/ Ovo': 6.50, 'Pão c/ Manteiga na Chapa': 6.00, 
    'Sanduíche Queijo Prato': 12.00,
    # Guarnições e Saladas
    'Arroz': 4.00, 'Arroz com Brócolis': 8.00, 'Feijão': 2.50, 'Farofa': 4.00, 
    'Salada de Palmito (Grande)': 25.00, 'Salada de Maionese': 8.00,
    # Drinks e Destilados
    'Caipirinha': 12.00, 'Cuba Libre': 13.00, 'Martini': 14.00, 'Vodka Ice': 5.00, 
    'Velho Barreiro': 4.00, 'Ballantine’s': 10.00, 'Whisky Red Label': 90.00,
    # Sobremesas
    'Pudim': 5.00, 'Torta': 5.00, 'Romeu e Julieta': 6.00
}

# --- 2. GERAÇÃO DE DADOS ---
data_list = []
days_range = (end_date - start_date).days

# Distribuição de Probabilidade por HORA (PICO DE ALMOÇO)
# Horário de Funcionamento: 10h até 18h (8 horas)
horas = np.arange(HORA_INICIO, HORA_FIM) # Array [10, 11, 12, 13, 14, 15, 16, 17]

# Define pesos para cada hora (a soma deve ser 1.0)
# Pico de Almoço (12h e 13h) recebe maior peso: 0.20 + 0.20 = 40% das vendas
# As outras 6 horas (10, 11, 14, 15, 16, 17) dividem os 60% restantes: 0.60 / 6 = 0.10 cada
pesos_horas = [
    0.10,  # 10h
    0.10,  # 11h
    0.20,  # 12h <-- PICO
    0.20,  # 13h <-- PICO
    0.10,  # 14h
    0.10,  # 15h
    0.10,  # 16h
    0.10   # 17h
]

# Distribuição de Probabilidade por DIA DA SEMANA
# 0=Segunda, 1=Terça, ..., 6=Domingo
# Assumindo que o movimento é maior durante a semana e sexta/sábado.
# Sab/Dom recebem 50% menos peso que os dias de pico (Seg-Qui).
# O peso total deve ser 1.0 (7 dias)
dias_semana_pesos = [0.17, 0.17, 0.17, 0.17, 0.17, 0.07, 0.05] # Total: 1.00

for i in range(num_records):
    # 1. Escolhe um dia aleatório, mas garante que a prob de Sab/Dom seja menor
    random_days_offset = np.random.randint(days_range)
    data_base = start_date + timedelta(days=random_days_offset)
    
    # Verifica o dia da semana (0=Segunda, 6=Domingo)
    dia_semana_num = data_base.weekday() 
    
    # 2. DECIDE se vai gerar uma venda para este dia (para diminuir a frequência no fim de semana)
    # Criamos um filtro (hack) baseado na probabilidade do dia
    prob_venda_diaria = dias_semana_pesos[dia_semana_num] * 7 # Multiplicamos por 7 para normalizar a escala
    
    # Se o número aleatório for maior que a prob do dia, pulamos a venda.
    if np.random.rand() > prob_venda_diaria:
        continue # Pula este ciclo e gera a próxima transação

    # 3. Escolhe a HORA baseada nos pesos (PICO)
    hora_escolhida = np.random.choice(horas, p=pesos_horas)
    
    # 4. Define o minuto e segundo aleatórios dentro da hora escolhida
    random_minutes = np.random.randint(60)
    random_seconds = np.random.randint(60)

    # 5. Constrói a DataHora final
    data_hora = data_base + \
                timedelta(hours=int(hora_escolhida), minutes=random_minutes, seconds=random_seconds)
    
    # ... (Restante da lógica de seleção de categoria e item)
    categoria = np.random.choice(
        list(menu.keys()), 
        p=[0.10, 0.25, 0.10, 0.20, 0.05, 0.05, 0.15, 0.10]
    )
    item = np.random.choice(menu[categoria])

    preco_unitario = precos[item]
    quantidade = np.random.randint(1, 4)

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