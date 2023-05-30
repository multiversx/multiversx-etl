-- Daily active users

SELECT 
  DATE(`timestamp`) `day`, 
  COUNT(DISTINCT `sender`) `num_users`, 
FROM `multiversx.transactions`
GROUP BY `day`
ORDER BY `day` DESC
LIMIT 1000

-- Number of interactions, per contract address

SELECT 
  DATE(`timestamp`) `day`,
  `receiver` `contract`,
  COUNT(*) `num_interactions`, 
FROM `multiversx.transactions`
WHERE `isScCall` = true
GROUP BY `day`, `contract`
HAVING `day` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY `day` DESC, `num_interactions` DESC

-- Number of unique users, per contract address

SELECT 
  DATE(`timestamp`) `day`,
  `receiver` `contract`,
  COUNT(DISTINCT `sender`) `num_users`, 
FROM `multiversx.transactions`
WHERE `isScCall` = true
GROUP BY `day`, `contract`
HAVING `day` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY `day` DESC, `num_users` DESC
