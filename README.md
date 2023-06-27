
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
python3 -m multiversxetl generate-schema --input-folder=~/mx-chain-tools-go/elasticreindexer/cmd/indices-creator/config/noKibana/ --output-folder=./schema
```

## Quickstart

First, set the following environment variables:

```
export WORKSPACE=${HOME}/multiversx-etl
export INDEXER_URL=https://index.multiversx.com:443
export GCP_PROJECT_ID=multiversx-blockchain-etl
export BQ_DATASET=mainnet
export START_TIMESTAMP=1596117600
export END_TIMESTAMP=1687880000
```

Then, plan ETL tasks (will add records in a Firestore database):

```
python3 -m multiversxetl plan-tasks-with-intervals --indexer-url=${INDEXER_URL} \
    --gcp-project-id=${GCP_PROJECT_ID}  --bq-dataset=${BQ_DATASET} \
    --start-timestamp=${START_TIMESTAMP} --end-timestamp=${END_TIMESTAMP}

python3 -m multiversxetl plan-tasks-without-intervals --indexer-url=${INDEXER_URL} \
    --gcp-project-id=${GCP_PROJECT_ID}  --bq-dataset=${BQ_DATASET}
```

**Note:** in order to remove all previously planned tasks, run the following commands:

```
firebase firestore:delete --project=${GCP_PROJECT_ID} --recursive tasks_with_interval
firebase firestore:delete --project=${GCP_PROJECT_ID} --recursive tasks_without_interval
```

Inspect the tasks:

```
python3 -m multiversxetl inspect-tasks --gcp-project-id=${GCP_PROJECT_ID}
```

Then, extract and load the data on _worker_ machines:

```
python3 -m multiversxetl extract-with-intervals --workspace=${WORKSPACE} --gcp-project-id=${GCP_PROJECT_ID}
python3 -m multiversxetl extract-without-intervals --workspace=${WORKSPACE} --gcp-project-id=${GCP_PROJECT_ID}
python3 -m multiversxetl load-with-intervals --workspace=${WORKSPACE} --gcp-project-id=${GCP_PROJECT_ID} --schema-folder=./schema
python3 -m multiversxetl load-without-intervals --workspace=${WORKSPACE} --gcp-project-id=${GCP_PROJECT_ID} --schema-folder=./schema
```

From time to time, you may want to run `inspect-tasks` again to check the progress.
