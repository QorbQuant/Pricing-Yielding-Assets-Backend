---- stkcvxfxs Supply Data -----

    WITH supply_stkcvxfxn  AS (
        SELECT
          day,
          SUM(value) AS supply
        FROM (
            SELECT
              SUM(cast(value as double)) / CAST(1e18 AS DOUBLE) AS value,
              DATE_TRUNC('day', evt_block_time) AS day
            FROM erc20_ethereum.evt_Transfer
            WHERE contract_address = 0x49b4d1dF40442f0C31b1BbAEA3EDE7c38e37E31a --stkCvxFXS
             AND "from" = 0x0000000000000000000000000000000000000000
            GROUP BY 2
            
            UNION ALL
            
            SELECT
              - SUM(cast(value as double)) / CAST(1e18 AS DOUBLE) AS value,
              DATE_TRUNC('day', evt_block_time) AS day
            FROM erc20_ethereum.evt_Transfer
            WHERE contract_address = 0x49b4d1dF40442f0C31b1BbAEA3EDE7c38e37E31a -- StkCvxFXS
             AND "to" = 0x0000000000000000000000000000000000000000
            GROUP BY 2
          ) 
        GROUP BY day
        )

, balances_with_gap_days AS (
     SELECT 
        day
        ,SUM(supply) OVER (ORDER BY day ASC) AS supply -- balance per day with a transaction
        ,LEAD(day, 1, now()) OVER (ORDER BY day asc) AS next_day
     FROM supply_stkcvxfxn
     )

, days AS 
(
    WITH days_seq AS (
        SELECT
        sequence(
            (SELECT cast(min(date_trunc('day', day)) as timestamp) day FROM supply_stkcvxfxn)
            , date_trunc('day', cast(now() as timestamp))
            , interval '1' day) as day
    )
    
    SELECT 
        days.day
    FROM days_seq
    CROSS JOIN unnest(day) as days(day) --this is just doing explode like in spark sql
)

 , balance_all_days AS (
     SELECT
        d.day
        ,SUM(supply) AS supply
     FROM balances_with_gap_days b
     INNER JOIN days d ON b.day <= d.day
     AND d.day < b.next_day -- Yields an observation for every day after the first transfer until the next day with transfer
     GROUP BY 1
     ORDER BY 1 DESC
     )

----- pricing data for cvxFXS: -----

, weth_price AS (
  SELECT
    DATE_TRUNC('day', day) AS time,
    AVG(price) AS eth_price
  FROM prices.usd_daily
  WHERE contract_address = 0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0 -- fxs
  GROUP BY 1
), 
fxn_price AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS time,
    tokens_sold / CAST(tokens_bought AS DOUBLE) AS price,
    tokens_bought AS amount
  FROM curvefi_ethereum.cvxfxsfxs_swap_evt_TokenExchange
  WHERE sold_id = 0
  
  UNION
  SELECT
    DATE_TRUNC('day', evt_block_time) AS time,
    tokens_bought / CAST(tokens_sold AS DOUBLE) AS price,
    tokens_sold AS amount
  FROM curvefi_ethereum.cvxfxsfxs_swap_evt_TokenExchange
  WHERE sold_id = 1 AND tokens_bought > 0
), 
fin5 AS (
  SELECT
    time,
    SUM(price * amount) / CAST(SUM(amount) AS DOUBLE) AS price
  FROM fxn_price
  GROUP BY 1
), 

eth_price AS (
  SELECT
    f1.time AS day,
    f1.price,
    f.eth_price,
    f1.price,
    f1.price * f.eth_price AS cvxFXS_price,
    LEAD(f1.time, 1, now()) OVER (ORDER BY f1.time asc) AS next_day
  FROM fin5 AS f1
  JOIN weth_price AS f
    ON f1.time = f.time
)


, price_all_days AS (
     SELECT
        d.day
        ,AVG(b.eth_price) AS FXS_price
        ,AVG(b.cvxFXS_price) AS cvxFXS_price
     FROM eth_price b
     INNER JOIN days d ON b.day <= d.day
     AND d.day < b.next_day -- Yields an observation for every day after the first transfer until the next day with transfer
     GROUP BY 1
     ORDER BY 1 DESC
     )

, cvxfxn_mcap AS (
SELECT
    a.day
    ,a.supply AS staked_cvxfxs
    ,b.cvxFXS_price
    ,b.FXS_price
    ,a.supply * b.cvxFXS_price AS staked_mcap
FROM balance_all_days AS a
LEFT JOIN price_all_days AS b ON a.day = b.day
ORDER BY 1 DESC) 

-------- cvxFXN Rewards ----- 

, price AS (
SELECT 
    day
    ,price
    ,symbol
    ,decimals
    ,contract_address
FROM prices.usd_daily
WHERE contract_address IN (0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b -- CVX
                            ,0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0 -- FXS
                        )
)

, rewards AS (
SELECT 
    DATE_TRUNC('day', evt_block_time) AS day
    ,_rewardsToken
    ,SUM(_reward) AS reward
FROM convex_ethereum.cvxFxsStakingforConvex_evt_RewardPaid
GROUP BY 1,2
)

, rewards_final AS (
SELECT 
    a.day 
    ,a.reward AS reward_tokens_raw
    ,a._rewardsToken AS reward_token
    ,a.reward / POWER(10, b.decimals) AS reward_tokens_actual
    ,b.symbol
    ,b.price
    ,(a.reward / POWER(10, b.decimals)) * b.price AS reward_usd
FROM rewards AS a
LEFT JOIN price AS b ON a.day = b.day AND a._rewardsToken = b.contract_address
ORDER BY 1 DESC)

, rewards_total AS (
SELECT
    DATE_TRUNC('day', day) AS day
    ,SUM(reward_usd) AS total_usd
FROM rewards_final
GROUP BY 1
)

, yieldMultiplier AS (
SELECT
    a.day
    ,a.staked_cvxfxs
    ,a.cvxFXS_price
    ,a.FXS_price
    ,a.staked_mcap
    ,b.total_usd AS usd_rewards
    ,b.total_usd / staked_mcap AS rate
    ,b.total_usd / staked_mcap + 1 AS daily_yield_multiplier
    ,(b.total_usd / staked_mcap) * 365 AS rate_annualized
FROM cvxfxn_mcap AS a
LEFT JOIN rewards_total AS b ON a.day = b.day)

, cumulativeYield AS (
SELECT
    *
    ,AVG(rate_annualized) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7_day_avg_APR
    ,EXP(SUM(LN(daily_yield_multiplier)) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS cumulative_yield_multiplier
FROM yieldMultiplier
)

SELECT 
    *
    ,cumulative_yield_multiplier * cvxFXS_price AS adjusted_price
    ,(cumulative_yield_multiplier * cvxFXS_price) - cvxFXS_price AS earnings_per_token
    ,rolling_7_day_avg_APR * 100 AS rolling_7_day_avg_APR_percent
FROM cumulativeYield
ORDER BY day DESC

    
    
    