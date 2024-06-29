---------- Account for gap days

WITH time AS (
    SELECT time 
    FROM unnest(sequence(CAST('2022-08-18' AS DATE), CAST(NOW() AS DATE), INTERVAL '1' day)) AS s(time)
    )

---------- Calcualte aFXS SUPPLY

, net_deposits AS (
SELECT
    day
    ,SUM(amount / POWER(10,18)) AS net_afxs -- (net cvxFXS deposited)
FROM (
      SELECT 
        DATE_TRUNC('day', evt_block_time) AS day
        ,SUM(shares) AS amount
      FROM aladdin_dao_ethereum.AladdinFXS_evt_Deposit
      GROUP BY 1
      
      UNION ALL
      
       SELECT 
        DATE_TRUNC('day', evt_block_time) AS day
        ,-SUM(shares) AS amount
      FROM aladdin_dao_ethereum.AladdinFXS_evt_Withdraw
      GROUP BY 1
    )
GROUP BY 1
)

, cumulative_deposits AS (
SELECT
    a.time AS day
    ,b.net_afxs
    ,SUM(b.net_afxs) OVER (ORDER BY a.time) AS afxs_supply
FROM time a
LEFT JOIN net_deposits b ON a.time = b.day
)

SELECT * FROM cumulative_deposits
ORDER BY 1 DESC



-- SELECT * 
-- , assets + harvestBounty
-- FROM aladdin_dao_ethereum.AladdinFXS_evt_Harvest 
-- ORDER BY evt_block_time DESC