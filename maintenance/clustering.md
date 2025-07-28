
https://cloud.google.com/bigquery/docs/apply-partition-cluster-recommendations#apply_cluster_recommendations_directly

Apply a new clustering specification:

```
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.accountsesdthistory
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.accountshistory
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.blocks
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.events
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.miniblocks
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.operations
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.receipts
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.rounds

bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.accounts
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.accountsesdt
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.delegators
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.scdeploys
bq update --clustering_fields=timestamp mainnet_append_only_indices_staging.tokens
```

Cluster all rows according to the new clustering specification:

```
UPDATE mainnet_append_only_indices_staging.accountsesdthistory SET `timestamp`=`timestamp` WHERE true;
UPDATE mainnet_append_only_indices_staging.accountshistory SET `timestamp`=`timestamp` WHERE true;
UPDATE mainnet_append_only_indices_staging.blocks SET `timestamp`=`timestamp` WHERE true;
UPDATE mainnet_append_only_indices_staging.events SET `timestamp`=`timestamp` WHERE true;
UPDATE mainnet_append_only_indices_staging.miniblocks SET `timestamp`=`timestamp` WHERE true;
UPDATE mainnet_append_only_indices_staging.operations SET `timestamp`=`timestamp` WHERE true;
UPDATE mainnet_append_only_indices_staging.receipts SET `timestamp`=`timestamp` WHERE true;
UPDATE mainnet_append_only_indices_staging.rounds SET `timestamp`=`timestamp` WHERE true;
```
