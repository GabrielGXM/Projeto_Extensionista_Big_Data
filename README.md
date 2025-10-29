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