-- Daily active users

SELECT 
  DATE(`timestamp`) `day`, 
  COUNT(DISTINCT `sender`) `num_users`, 
FROM `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.operations`
WHERE type = 'normal'
GROUP BY `day`
ORDER BY `day` DESC
LIMIT 1000

-- Number of interactions, per contract address

SELECT 
  DATE(`timestamp`) `day`,
  `receiver` `contract`,
  COUNT(*) `num_interactions`, 
FROM `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.operations`
WHERE `isScCall` = true
GROUP BY `day`, `contract`
HAVING `day` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY `day` DESC, `num_interactions` DESC

-- Number of unique users, per contract address

SELECT 
  DATE(`timestamp`) `day`,
  `receiver` `contract`,
  COUNT(DISTINCT `sender`) `num_users`, 
FROM `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.operations`
WHERE
  type = 'normal'
  AND `isScCall` = true
GROUP BY `day`, `contract`
HAVING `day` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY `day` DESC, `num_users` DESC

--
-- Volume transferred, per contract address; (native, custom tokens), (in, out)
--

-- Native (EGLD), input volume

WITH `contract_volumes_per_day` AS
(
  SELECT 
    DATE(`timestamp`) `day`,
    `receiver` `contract`,
    SUM(CAST(`value` AS BIGNUMERIC)) `native_volume`, 
  FROM `multiversx-blockchain-etl.crypto_multiversx_mainnet_eu.operations`
  WHERE
    type = 'normal'
    AND `isScCall` = true
  AND `status` = 'success'
  GROUP BY `day`, `contract`
)
SELECT `day`, `contract`, `native_volume`, `row_num` `top` FROM
(
  SELECT *, ROW_NUMBER() OVER (PARTITION BY `day` ORDER BY `native_volume` DESC) AS `row_num`
  FROM `contract_volumes_per_day`
)
WHERE `row_num` <= 3
ORDER BY `day` DESC, `top` ASC
