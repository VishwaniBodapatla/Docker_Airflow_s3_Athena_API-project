# Docker_Airflow_s3_Athena_API-project


```mermaid
flowchart TD
    A[🔗 External API] --> B[⬇️ Extract in Python Script]
    B --> C[⚙️ Airflow Task: Transform & Clean Data]
    C --> D[🐳 Docker Container]
    D --> E[📁 Save to CSV/Parquet]
    E --> F[☁️ Upload to Amazon S3 Bucket]
    F --> G[✅ ETL Complete]

    subgraph Airflow DAG
        B
        C
        E
    end
