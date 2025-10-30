# 🍽️ Projeto de Big Data Analytics para Restaurantes

## 💡 Visão Geral do Projeto

Este projeto de Big Data e Data Science tem como objetivo principal aplicar técnicas de processamento de dados distribuídos para analisar o **desempenho operacional e comercial** de um restaurante.

O foco é transformar dados de transações (vendas de itens, horários, categorias) em *insights* acionáveis que possam otimizar o estoque, a precificação e o planejamento de cardápio do estabelecimento.

O projeto foi desenvolvido como requisito da disciplina [Nome da Disciplina] da [Nome da Universidade/Faculdade].

## 📊 Objetivo Principal

Analisar um volume significativo de transações de vendas para identificar:
* Padrões de consumo por horário e dia da semana.
* Itens mais vendidos e categorias mais lucrativas.
* Otimização de preços e avaliação de desempenho de pratos.

## ⚙️ Tecnologias Utilizadas

A arquitetura do projeto é construída em Python e foca em ferramentas de processamento de dados em diferentes escalas:

| Categoria | Ferramenta | Descrição |
| :--- | :--- | :--- |
| **Processamento Distribuído (Big Data)** | **PySpark (Apache Spark)** | Utilizado para ingestão, transformação (ETL) e análise de grandes volumes de dados de forma escalável e distribuída. |
| **Manipulação de Dados (Local)** | **Pandas** | Utilizado para prototipagem inicial e geração do *dataset* simulado. |
| **Linguagem de Programação** | **Python** | Linguagem principal de desenvolvimento. |
| **Gerenciamento de Ambiente** | **Ambiente Virtual (`venv`)** | Garante o isolamento e a reprodutibilidade das dependências do projeto. |

## 📂 Estrutura do Projeto

O projeto segue a seguinte organização modular:

Big_Data_Project/ | ├── data/ │ ├── raw/ # Dados de vendas brutos (incluindo o CSV simulado) │ └── processed/ # Dados limpos e transformados pelo PySpark | ├── src/ # Código-fonte Python principal │ ├── data_generator.py # Script de geração dos dados simulados (Pandas) │ └── etl_pyspark.py # Script de processamento ETL e Análise (PySpark) | ├── notebooks/ # (Opcional) Jupyter Notebooks para exploração de dados ├── reports/ # Resultados de análise e visualizações └── requirements.txt # Lista de dependências Python

## 🚀 Como Executar

1.  **Clone o repositório:** `git clone https://docs.github.com/pt/migrations/importing-source-code/using-the-command-line-to-import-source-code/adding-locally-hosted-code-to-github`
2.  **Crie e Ative o Ambiente Virtual:**
    ```bash
    python -m venv venv
    source venv/Scripts/activate  # Para Git Bash/Linux
    # OU: venv\Scripts\activate   # Para CMD/PowerShell
    ```
3.  **Instale as Dependências:** `pip install -r requirements.txt`
4.  **Gere os Dados Simulados:** `python src/data_generator.py`
5.  **Execute o Processamento PySpark:** `python src/etl_pyspark.py`