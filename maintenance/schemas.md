
```
# Mainnet (staging)
export GCP_PROJECT_ID=multiversx-blockchain-etl
export BQ_DATASET_APPEND_ONLY=mainnet_append_only_indices_staging
export BQ_DATASET_MUTABLE=mainnet_mutable_indices_staging

# Mainnet
export GCP_PROJECT_ID=multiversx-blockchain-etl
export BQ_DATASET_APPEND_ONLY=crypto_multiversx_mainnet_eu
export BQ_DATASET_MUTABLE=crypto_multiversx_mainnet_eu

bq update --description "ESDTs of accounts (with history)." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.accountsesdthistory schema/accountsesdthistory.json
bq update --description "Accounts data (with history)." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.accountshistory schema/accountshistory.json
bq update --description "Blocks." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.blocks schema/blocks.json
bq update --description "Transaction events." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.events schema/events.json
bq update --description "Miniblocks (sub-components of blocks)." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.miniblocks schema/miniblocks.json
bq update --description "Operations (transactions and transactions results)." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.operations schema/operations.json
bq update --description "Transaction receipts." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.receipts schema/receipts.json
bq update --description "Rounds info." ${GCP_PROJECT_ID}:${BQ_DATASET_APPEND_ONLY}.rounds schema/rounds.json

bq update --description "Accounts data." ${GCP_PROJECT_ID}:${BQ_DATASET_MUTABLE}.accounts schema/accounts.json
bq update --description "ESDTs of accounts." ${GCP_PROJECT_ID}:${BQ_DATASET_MUTABLE}.accountsesdt schema/accountsesdt.json
bq update --description "Delegators." ${GCP_PROJECT_ID}:${BQ_DATASET_MUTABLE}.delegators schema/delegators.json
bq update --description "Epoch info." ${GCP_PROJECT_ID}:${BQ_DATASET_MUTABLE}.epochinfo schema/epochinfo.json
bq update --description "Smart contract deploys." ${GCP_PROJECT_ID}:${BQ_DATASET_MUTABLE}.scdeploys schema/scdeploys.json
bq update --description "ESDT tokens." ${GCP_PROJECT_ID}:${BQ_DATASET_MUTABLE}.tokens schema/tokens.json
bq update --description "Validators." ${GCP_PROJECT_ID}:${BQ_DATASET_MUTABLE}.validators schema/validators.json
```
