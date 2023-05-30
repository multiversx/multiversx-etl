-- Network rewards, by day

SELECT 
    DATE(`timestamp`) `day`, 
    SUM(CAST(value AS BIGNUMERIC)) `rewards` 
FROM `multiversx.transactions` 
WHERE `operation` = "reward"
GROUP BY `day`
ORDER BY `day` DESC

-- Number of transactions, by day

SELECT 
    DATE(`timestamp`) `day`, 
    COUNT(*) `transactions`
FROM `multiversx.transactions`
GROUP BY `day`
ORDER BY `day` DESC
