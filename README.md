# ESIM Operator Plan Ranker

Enterprise-grade Python worker for ranking eSIM operator/carrier packages by country, data allowance, duration/validity, and cost effectiveness.

## What it does

- Reads raw operator/carrier plan data from a MySQL database.
- Groups plans by country, data bundle, and validity/duration.
- Calculates cost-effective ranking using normalized package price values.
- Supports direct database access and SSH-tunnel database access.
- Writes ranked package results back to sorted package tables.

## Core features

| Feature | Description |
|---|---|
| Country-based ranking | Processes package options per supported country code. |
| Data bundle comparison | Compares plans by data allowance, including unlimited packages. |
| Duration-aware sorting | Handles validity/days-based package comparison. |
| Cost-effectiveness ranking | Sorts plans by calculated net price / ranking logic. |
| SSH tunnel support | Allows secure database access through an SSH bastion/server. |
| Environment-based config | Public-safe configuration through environment variables. |

## Project structure

```text
ESIM-Operator-Plan-Ranker/
├── Config/
│   └── global_config.py      # Environment-driven configuration
├── bpl/
│   └── bplinit.py            # Business processing and ranking workflow
├── controller/
│   └── controller.py         # Main orchestration controller
├── dpl/
│   └── dplinit.py            # Data persistence/database access layer
├── main.py                   # Worker entry point
├── requirements.txt          # Python dependencies
├── .env.example              # Example local configuration
└── .gitignore                # Public repo safety exclusions
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

Update `.env` with your own database and SSH values. Never commit `.env`, PEM keys, database passwords, or runtime logs.

## Run

```bash
python main.py
```

## Public repository safety notes

This repo was cleaned for public GitHub usage:

- Removed local virtual environment files.
- Removed runtime process/error logs.
- Removed Python cache files.
- Removed hardcoded credentials and local PEM path.
- Added `.gitignore` and `.env.example`.
- Added file-level developer signatures and function-level docstrings.

## Important implementation note

The SQL currently contains placeholder schema/table references such as `[SchemaName]`. Replace these with your actual schema/table names in private deployment code or inject them through configuration before production use.

## Developer signature

Developed and maintained by **Kashif Sattar**.
