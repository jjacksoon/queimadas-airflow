# 🛰️ Pipeline de Monitoramento de Focos de Incêndio (INPE)

Este projeto implementa um pipeline de dados automatizado (ETL) para monitorar focos de incêndio no Brasil, utilizando dados oficiais disponibilizados diariamente pelo **INPE** (Instituto Nacional de Pesquisas Espaciais). 

O sistema é orquestrado pelo **Apache Airflow** e utiliza a **Arquitetura Medalhão** para garantir a linhagem, qualidade e integridade dos dados durante todo o processo.

## 🛠️ Tecnologias Utilizadas
* **Linguagem:** Python (Pandas para manipulação e limpeza de dados).
* **Orquestração:** Apache Airflow (via Astro CLI).
* **Banco de Dados:** PostgreSQL (Executado via Docker).
* **Infraestrutura:** Docker & Docker Compose para isolamento de ambiente.

## 🏗️ Arquitetura do Projeto
O pipeline segue um fluxo robusto dividido em três etapas principais:

1.  **Extração (Camada Bronze):** Coleta arquivos CSV dinâmicos do servidor do INPE baseados na data de execução. O script lida com a latência de disponibilização da fonte e garante o download seguro.
2.  **Transformação (Camada Silver):** Limpeza de dados com Pandas, incluindo seleção de colunas essenciais (`municipio`, `estado`, `risco_fogo`, etc.), tratamento de valores nulos e tipagem de datas (GMT/UTC).
3.  **Carga (Database):** Inserção no Postgres utilizando uma estratégia de **Idempotência**. O pipeline identifica as datas no arquivo e remove registros pré-existentes antes da nova carga, permitindo re-execuções sem duplicidade de dados.

### Diagrama de Fluxo
```graph LR
    %% Definição de Estilos (Cores)
    classDef bronze fill:#cd7f32,stroke:#333,stroke-width:2px,color:#fff;
    classDef silver fill:#c0c0c0,stroke:#333,stroke-width:2px,color:#000;
    classDef gold fill:#ffd700,stroke:#333,stroke-width:2px,color:#000;
    classDef airflow fill:#017cee,stroke:#333,stroke-width:2px,color:#fff;
    classDef inpe fill:#f9f9f9,stroke:#333,stroke-dasharray: 5 5;

    subgraph "Fonte Externa"
        INPE[Servidor INPE - CSV]:::inpe
    end

    subgraph "Orquestrador (Apache Airflow)"
        E[Tarefa: Extrair]:::airflow --> T[Tarefa: Transformar]:::airflow
        T --> L[Tarefa: Carregar]:::airflow
    end

    subgraph "Armazenamento (Docker)"
        R[(Camada Bronze: Raw CSV)]:::bronze
        S[(Camada Silver: Clean CSV)]:::silver
        DB[(PostgreSQL)]:::gold
    end

    %% Conexões
    INPE -.-> E
    E --> R
    R --> T
    T --> S
    S --> L
    L --> DB
```


## 🧠 Competências Demonstradas

Neste projeto, foram aplicados conceitos fundamentais de Engenharia de Dados que demonstram maturidade técnica para o nível Júnior/Pleno:

* ✅ **Orquestração de Workflows:** Configuração de DAGs no Apache Airflow, gerenciamento de dependências entre tarefas e agendamento inteligente baseado em fuso horário (UTC vs Brasília).
* ✅ **Qualidade e Integridade de Dados:** Implementação de lógica de idempotência para evitar duplicidade e garantir que o pipeline possa ser reiniciado sem corromper o banco de dados.
* ✅ **Contêinerização com Docker:** Desenvolvimento em ambiente isolado utilizando Docker e Docker Compose, garantindo a portabilidade do projeto entre diferentes máquinas.
* ✅ **Manipulação de Dados (Pandas):** Limpeza, filtragem e normalização de grandes volumes de dados (camada Silver) com foco em otimização de performance.
* ✅ **Integração com Bancos de Dados Relacionais:** Uso de SQLAlchemy para execução de queries transacionais e carga de DataFrames via `to_sql`.

## 👤 Sobre o Autor

## Jackson Nascimento - Engenheiro de Dados em formação | BI | Analytics

Projeto desenvolvido com foco em aprendizado real de engenharia de dados, indo além de tutoriais e demonstrando capacidade de estruturar pipelines próximos ao cenário profissional.

#### 🔗 LinkedIn: https://www.linkedin.com/in/jackson10/