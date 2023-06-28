-- Basic information about the most recent blocks

SELECT 
  `nonce`, 
  `timestamp`, 
  `shardId` `shard`,
  ARRAY_LENGTH(`miniBlocksHashes`) `num_miniblocks`,
  `txCount` `num_txs`,
FROM `multiversx.blocks`
ORDER BY `timestamp` DESC
LIMIT 1000
