Gere um README.md profissional para um projeto de Engenharia de Dados.

Contexto do projeto:

Nome do projeto: Retail Analytics with PySpark
<img width="1307" height="616" alt="medallion_spark" src="https://github.com/user-attachments/assets/fb639e83-7d0c-4889-b2f7-ccf47c2e5524" />

Objetivo:
Construir um pipeline de dados utilizando PySpark e arquitetura Medallion (Bronze, Silver, Gold) para processar dados de vendas de varejo e gerar métricas analíticas para dashboards (Power BI).

Tecnologias utilizadas:
- Apache Spark (PySpark)
- Docker
- Docker Compose
- Python
- Parquet
- Arquitetura Medallion
- Power BI (para visualização)

Infraestrutura:
O cluster Spark roda em Docker com:
- 1 Spark Master
- 2 Spark Workers

Estrutura do projeto:

project/
│
├── data/
│   ├── raw/        # dados brutos
│   ├── bronze/     # dados ingeridos
│   ├── silver/     # dados limpos e transformados
│   └── gold/       # métricas para analytics
│
├── src/
│   ├── bronze.py   # ingestão de dados
│   ├── silver.py   # limpeza e transformação
│   └── gold.py     # criação de métricas
│
├── docker-compose.yml
├── generate_raw_data.py
└── imagem/
    └── medallion_spark.png

Fluxo de dados:

RAW → BRONZE → SILVER → GOLD

Bronze:
- Ingestão do CSV
- Conversão para Parquet
- Estruturação do schema

Silver:
- Limpeza de dados
- Validação de regras de negócio
- Remoção de duplicados
- Padronização de campos

Gold:
Criação de métricas analíticas:

1. daily_sales_metrics
   - order_date
   - total_revenue
   - total_orders
   - avg_order_value

2. city_revenue_metrics
   - city
   - state
   - city_revenue
   - order_count
   - avg_order_value

3. product_category_performance
   - product_category
   - category_revenue
   - total_units_sold
   - order_count

4. order_status_metrics
   - order_status
   - count

Também inclua no README:

- descrição do projeto
- arquitetura do pipeline
- estrutura de pastas
- como rodar o projeto
- como subir o cluster Spark com Docker
- como executar scripts PySpark
- exemplos de consultas analíticas
- exemplos de dashboards possíveis no Power BI

O README deve ser bem estruturado, profissional e em inglês.
