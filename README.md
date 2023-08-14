# MultiversX ETL

ETL (extract, transform and load) tools for publishing MultiversX blockchain data (extracted from a standard MultiversX Elasticsearch instance) on Google BigQuery.

## Published data

 - [Mainnet](https://console.cloud.google.com/bigquery?page=dataset&d=mainnet&p=multiversx-blockchain-etl)
 - [Devnet (new)](https://console.cloud.google.com/bigquery?page=dataset&d=devnet&p=multiversx-blockchain-etl)
 - [Devnet (old)](https://console.cloud.google.com/bigquery?page=dataset&d=devnet_1648551600&p=multiversx-blockchain-etl)
 - [Testnet](https://console.cloud.google.com/bigquery?page=dataset&d=testnet&p=multiversx-blockchain-etl)

## Setup virtual environment

Create a virtual environment and install the dependencies:

```
python3 -m venv ./venv
source ./venv/bin/activate

pip install -r ./requirements.txt --upgrade
pip install -r ./requirements-dev.txt --upgrade
```

## Run the tests

```
export PYTHONPATH=.
pytest -m "not integration"
```

## Generate schema files

```
python3 -m multiversxetl generate-schema --input-folder=~/mx-chain-tools-go/elasticreindexer/cmd/indices-creator/config/noKibana/ --output-folder=./schema
```

## Quickstart

First, set the following environment variables:

```
export GCP_PROJECT_ID=multiversx-blockchain-etl
export GROUP=mainnet
export WORKSPACE=${HOME}/multiversx-etl
export INDEXER_URL=https://index.multiversx.com:443
export BQ_DATASET=mainnet
export START_TIMESTAMP=1596117600
export END_TIMESTAMP=1689920000
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

While the tasks are running, you may want to regularly execute `inspect-tasks` again to check the progress.

At times, the _load_ step could fail due to, say, new fields added to Elasticsearch indices (of which the BigQuery schema was not aware). In this case, re-generate the schema files (see above), then update the BigQuery with the `bq` command (example below is for the `tokens` table):

```
bq update ${GCP_PROJECT_ID}:${BQ_DATASET}.tokens schema/tokens.json
```

## Check loaded data

```
python3 -m multiversxetl check-loaded-data --gcp-project-id=${GCP_PROJECT_ID} --bq-dataset=${BQ_DATASET} --indexer-url=${INDEXER_URL} \
    --start-timestamp=${START_TIMESTAMP} --end-timestamp=${END_TIMESTAMP}
```

## Management (Google Cloud Console)

Below are a few links useful for managing the ETL process. They are only accessible to the MultiversX team.

 - [Firestore Dashboard](https://console.cloud.google.com/firestore/databases/-default-/data/panel?project=multiversx-blockchain-etl): inspect and manage the Firestore collections that store the metadata of the ETL tasks.
 - [BigQuery Workspace](https://console.cloud.google.com/bigquery?project=multiversx-blockchain-etl): inspect and manage the BigQuery datasets and tables.
 - [Analytics Hub](https://console.cloud.google.com/bigquery/analytics-hub/exchanges?project=multiversx-blockchain-etl): create and publish data listings.
 - [Logs Explorer](https://console.cloud.google.com/logs/query?project=multiversx-blockchain-etl): inspect logs.
