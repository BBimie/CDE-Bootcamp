
- Extract: Python
- Load / Staging: Postgres
- Transform: DBT

https://docs.google.com/document/d/1Qk6g6XdyIkPqS0qeT0uYkjEKfl_cSSYTg_hr5UCNFFI/edit?tab=t.0



elt-pipeline/
├── docker-compose.yml
├── .env
├── extractor/
│   ├── Dockerfile
│   ├── extract.py
│   └── requirements.txt
│
└── dbt/
    ├── Dockerfile
    ├── dbt.yml
    ├── profiles.yml
    ├── requirements.txt
    └── models/
        └── transform_dbt_model.sql