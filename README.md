# MultiversX ETL

ETL (extract, transform and load) tools for publishing MultiversX blockchain data (extracted from a standard MultiversX Elasticsearch instance) on Google BigQuery.

## Published data

[Mainnet](https://console.cloud.google.com/marketplace/product/bigquery-public-data/blockchain-analytics-multiversx-mainnet-eu)

## Setup virtual environment

Create a virtual environment and install the dependencies:

```
python3 -m venv ./venv
source ./venv/bin/activate

pip install -r ./requirements.txt --upgrade
pip install -r ./requirements-dev.txt --upgrade
```

## Running the unit tests

```
export PYTHONPATH=.
pytest -m "not integration"
```

## Running the integration tests

Generally speaking, the current integration tests should be ran locally (in the future, they might be added in the CI pipeline).

First, connect to the Google Cloud Platform as follows:

```
gcloud auth application-default login
gcloud config set project multiversx-blockchain-etl
gcloud auth application-default set-quota-project multiversx-blockchain-etl
```

Then, run the integration tests:

```
pytest -m "integration"
```

## Quickstart

This implementation copies the data from Elasticsearch in two parallel flows.

One flow copies the append-only indices (e.g. blocks, operations, events, receipts, etc.) into a staging BQ dataset. This process is incremental, i.e. it only copies the new data since the last run, and it's executed more often than the second flow (every 1 hour, by default). Once the staging database is loaded, the data is transferred to the main BQ dataset, using the _Big Query Data Transfers_ facility.

The second flow copies the mutable indices (e.g. tokens, accounts, etc.) into a staging BQ dataset. This process is not incremental. Tables are truncated and reloaded on each run. Once the staging database is loaded, the data is transferred to the main BQ dataset, using the _Big Query Data Transfers_ facility. This flow is executed less often than the first flow (every 4 hours, by default).

In order to invoke the two processes, you can either use the Docker setup (see next section) or explicitly invoke the following commands:

```
# First, set the following environment variables:
export GCP_PROJECT_ID=multiversx-blockchain-etl
export WORKSPACE=${HOME}/multiversx-etl/mainnet

# The first flow (for append-only indices):
python3 -m multiversxetl.app process-append-only-indices --workspace=${WORKSPACE} --sleep-between-iterations=3600

# The second flow (for mutable indices):
python3 -m multiversxetl.app process-mutable-indices --workspace=${WORKSPACE} --sleep-between-iterations=86400
```

### Rewinding

Sometimes, errors occur during the ETL process. For the append-only flow, it's recommended to rewind the BQ tables to the latest checkpoint (good state), and re-run the process only after that. This helps to de-duplicate the data beforehand, through a simple data removal. Otherwise, the full data de-duplication step would be employed (performed automatically, after each bulk of tasks, if the data counts from BQ and Elasticsearch do not match), which is more expensive.

To rewind the BQ tables corresponding to the append-only indices to the latest checkpoint, run the following command:

```
python3 -m multiversxetl.app rewind --workspace=${WORKSPACE}
```

If the checkpoint is not available or is assumed to be corrupted, one can find the latest good checkpoint by running the following command:

```
python3 -m multiversxetl.app find-latest-good-checkpoint --workspace=${WORKSPACE}
```

## Docker setup

Build the Docker image:

```
docker build --network host -f ./docker/Dockerfile -t multiversx-etl:latest .
```

Set up the containers:

```
# mainnet
docker compose --file ./docker/docker-compose.yml \
    --env-file ./docker/env/mainnet.env \
    --project-name multiversx-etl-mainnet up --detach

# devnet
docker compose --file ./docker/docker-compose.yml \
    --env-file ./docker/env/devnet.env \
    --project-name multiversx-etl-devnet up --detach

# testnet
docker compose --file ./docker/docker-compose.yml \
    --env-file ./docker/env/testnet.env \
    --project-name multiversx-etl-testnet up --detach
```

## Management (Google Cloud Console)

Below are a few links useful for managing the ETL process. They are only accessible to the MultiversX team.

- [BigQuery Workspace](https://console.cloud.google.com/bigquery?project=multiversx-blockchain-etl): inspect and manage the BigQuery datasets and tables.
- [Analytics Hub](https://console.cloud.google.com/bigquery/analytics-hub/exchanges?project=multiversx-blockchain-etl): create and publish data listings.
- [Logs Explorer](https://console.cloud.google.com/logs/query?project=multiversx-blockchain-etl): inspect logs.
- [Monitoring](https://console.cloud.google.com/bigquery/admin/monitoring?project=multiversx-blockchain-etl&region=eu): resource utilization and jobs explorer.
- [Data Transfers](https://console.cloud.google.com/bigquery/transfers?project=multiversx-blockchain-etl): inspect and manage the data transfers.
