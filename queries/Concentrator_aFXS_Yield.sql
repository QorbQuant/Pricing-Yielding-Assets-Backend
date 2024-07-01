--- Account for gap days

WITH time AS (
    SELECT time 
    FROM unnest(sequence(CAST('2022-08-18' AS DATE), CAST(NOW() AS DATE), INTERVAL '1' day)) AS s(time)
    )

---------- CALCULATE cvxFXS Return

, net_deposits AS (
SELECT
    day
    ,SUM(amount / POWER(10,18)) AS net_cvxfxs -- (net cvxFXS deposited)
    ,SUM(shares /POWER(10,18)) AS net_shares
FROM (
      SELECT 
        DATE_TRUNC('day', evt_block_time) AS day
        ,SUM(assets) AS amount
        ,SUM(shares) AS shares
      FROM aladdin_dao_ethereum.AladdinFXS_evt_Deposit
      GROUP BY 1
      
      UNION ALL
      
       SELECT 
        DATE_TRUNC('day', evt_block_time) AS day
        ,-SUM(assets) AS amount
        ,-SUM(shares) AS shares
      FROM aladdin_dao_ethereum.AladdinFXS_evt_Withdraw
      GROUP BY 1
    )
GROUP BY 1
)

, cumulative_deposits AS (
SELECT
    a.time AS day
    ,b.net_cvxfxs
    ,b.net_cvxfxs / b.net_shares AS return
    ,SUM(b.net_cvxfxs) OVER (ORDER BY a.time) AS cvxfxs_staked
    ,LAST_VALUE(b.net_cvxfxs / b.net_shares) IGNORE NULLS OVER (
            PARTITION BY 1
            ORDER BY a.time
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS last_return
FROM time a
LEFT JOIN net_deposits b ON a.time = b.day
)

SELECT
    day,
    net_cvxfxs,
    COALESCE(return, last_return) AS return,
    cvxfxs_staked
FROM
    cumulative_deposits
ORDER BY
    day DESC




-- SELECT * 
-- , assets + harvestBounty
-- FROM aladdin_dao_ethereum.AladdinFXS_evt_Harvest 
-- ORDER BY evt_block_time DESC