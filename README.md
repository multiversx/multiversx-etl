
## Setup virtual environment

Create a virtual environment and install the dependencies:

```
python3 -m venv ./venv
source ./venv/bin/activate

pip install -r ./requirements.txt --upgrade
pip install -r ./requirements-dev.txt --upgrade
```

## Generate schema files

```
python3 -m multiversxetl generate_schema --input-folder=~/mx-chain-tools-go/elasticreindexer/cmd/indices-creator/config/noKibana/ --output-folder=./schema
```

## Quickstart

First, set the following environment variables:

```
export WORKSPACE=${HOME}/multiversx-etl
export INDEXER_URL=https://index.multiversx.com:443
export GCP_PROJECT_ID=...
export BQ_DATASET=multiversx_mainnet
export START_TIMESTAMP=1672243200
```

Then, plan ETL tasks (will add records in a Firestore database):

```
python3 -m multiversxetl plan_tasks_with_intervals --indexer-url=${INDEXER_URL} \
    --gcp-project-id=${GCP_PROJECT_ID}  --bq-dataset=${BQ_DATASET} \
    --start-timestamp=${START_TIMESTAMP}

python3 -m multiversxetl plan_tasks_without_intervals --indexer-url=${INDEXER_URL} \
    --gcp-project-id=${GCP_PROJECT_ID}  --bq-dataset=${BQ_DATASET}
```

**Note:** in order to remove all previously planned tasks, run the following commands:

```
firebase firestore:delete --project=${GCP_PROJECT_ID} --recursive tasks_with_interval
firebase firestore:delete --project=${GCP_PROJECT_ID} --recursive tasks_without_interval
```

