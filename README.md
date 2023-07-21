
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
python3 -m multiversxetl plan-tasks-with-intervals --gcp-project-id=${GCP_PROJECT_ID} --group=${GROUP} \
    --indexer-url=${INDEXER_URL} --bq-dataset=${BQ_DATASET} \
    --start-timestamp=${START_TIMESTAMP} --end-timestamp=${END_TIMESTAMP}

python3 -m multiversxetl plan-tasks-without-intervals --gcp-project-id=${GCP_PROJECT_ID} --group=${GROUP} \
    --indexer-url=${INDEXER_URL} --bq-dataset=${BQ_DATASET}
```

Inspect the tasks:

```
python3 -m multiversxetl inspect-tasks --group=${GROUP} --gcp-project-id=${GCP_PROJECT_ID}
```

Then, extract and load the data on _worker_ machines:

```
python3 -m multiversxetl extract-with-intervals --gcp-project-id=${GCP_PROJECT_ID} --workspace=${WORKSPACE} --group=${GROUP}  --num-threads=4
python3 -m multiversxetl extract-without-intervals --gcp-project-id=${GCP_PROJECT_ID} --workspace=${WORKSPACE} --group=${GROUP} --num-threads=4
python3 -m multiversxetl load-with-intervals --gcp-project-id=${GCP_PROJECT_ID} --workspace=${WORKSPACE} --group=${GROUP} --schema-folder=./schema --num-threads=4
python3 -m multiversxetl load-without-intervals --gcp-project-id=${GCP_PROJECT_ID} --workspace=${WORKSPACE} --group=${GROUP} --schema-folder=./schema --num-threads=4
```

From time to time, you may want to run `inspect-tasks` again to check the progress.
