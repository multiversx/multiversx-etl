Create transactions view:

```
CREATE VIEW
  `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.view_transactions` AS
SELECT
  *
FROM
  `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.operations`
WHERE
  type = 'normal'
```

Create scresults view:

```
CREATE VIEW
  `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.view_scresults` AS
SELECT
  *
FROM
  `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.operations`
WHERE
  type = 'unsigned'
```