---- stkcvxfxn Supply Data -----

    WITH supply_stkcvxfxn  AS (
        SELECT
          day,
          SUM(value) AS supply
        FROM (
            SELECT
              SUM(cast(value as double)) / CAST(1e18 AS DOUBLE) AS value,
              DATE_TRUNC('day', evt_block_time) AS day
            FROM erc20_ethereum.evt_Transfer
            WHERE contract_address = 0xEC60Cd4a5866fb3B0DD317A46d3B474a24e06beF
             AND "from" = 0x0000000000000000000000000000000000000000
            GROUP BY 2
            
            UNION ALL
            
            SELECT
              - SUM(cast(value as double)) / CAST(1e18 AS DOUBLE) AS value,
              DATE_TRUNC('day', evt_block_time) AS day
            FROM erc20_ethereum.evt_Transfer
            WHERE contract_address = 0xEC60Cd4a5866fb3B0DD317A46d3B474a24e06beF
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

----- pricing data for cvxFXN: -----

, weth_price AS (
  SELECT
    DATE_TRUNC('day', minute) AS time,
    AVG(price) AS eth_price
  FROM prices.usd
  WHERE contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- weth
  GROUP BY 1
), 
fxn_price AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS time,
    tokens_sold / CAST(tokens_bought AS DOUBLE) AS price,
    tokens_bought AS amount
  FROM fx_protocol_ethereum.FXN_ETH_pool_evt_TokenExchange
  WHERE sold_id = 0
  
  UNION
  SELECT
    DATE_TRUNC('day', evt_block_time) AS time,
    tokens_bought / CAST(tokens_sold AS DOUBLE) AS price,
    tokens_sold AS amount
  FROM fx_protocol_ethereum.FXN_ETH_pool_evt_TokenExchange
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
    f1.price * f.eth_price AS FXN_price
  FROM fin5 AS f1
  JOIN weth_price AS f
    ON f1.time = f.time
)

, cnc_price AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS time,
    tokens_sold / CAST(tokens_bought AS DOUBLE) AS price,
    tokens_bought AS amount
  FROM fx_protocol_ethereum.cvxFXN_FXN_LP_evt_TokenExchange
  WHERE sold_id = 0
  
  UNION
  SELECT
    DATE_TRUNC('day', evt_block_time) AS time,
    tokens_bought / CAST(tokens_sold AS DOUBLE) AS price,
    tokens_sold AS amount
  FROM fx_protocol_ethereum.cvxFXN_FXN_LP_evt_TokenExchange
  WHERE sold_id = 1 AND tokens_bought > 0
), 
fin1 AS (
  SELECT
    time,
    SUM(price * amount) / CAST(SUM(amount) AS DOUBLE) AS price
  FROM cnc_price
  GROUP BY 1
), 

price_with_gap_days AS (
  SELECT
    f1.time AS day,
    f1.price,
    f.FXN_price,
    f1.price,
    f1.price * f.FXN_price AS cvxFXN_price,
    LEAD(f1.time, 1, now()) OVER (ORDER BY day asc) AS next_day
  FROM fin1 AS f1
  JOIN eth_price AS f
    ON f1.time = f.day
)

, price_all_days AS (
     SELECT
        d.day
        ,AVG(FXN_price) AS FXN_price
        ,AVG(cvxFXN_price) AS cvxFXN_price
     FROM price_with_gap_days b
     INNER JOIN days d ON b.day <= d.day
     AND d.day < b.next_day -- Yields an observation for every day after the first transfer until the next day with transfer
     GROUP BY 1
     ORDER BY 1 DESC
     )

, cvxfxn_mcap AS (
SELECT
    a.day
    ,a.supply AS staked_cvxfxn
    ,b.cvxFXN_price
    ,a.supply * b.cvxFXN_price AS staked_mcap
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
WHERE contract_address IN(0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 -- wsteth
                            ,0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b -- CVX
                        )
UNION ALL

SELECT 
    minute AS day
    ,FXN_price AS price
    ,'FXN' AS symbol
    ,18 AS decimals
    ,0x365accfca291e7d3914637abf1f7635db165bb09 AS contract_address
FROM query_3863432
)

, rewards AS (
SELECT 
    DATE_TRUNC('day', evt_block_time) AS day
    ,_rewardsToken
    ,SUM(_reward) AS reward
FROM fx_protocol_ethereum.cvxFxnStakingforConvex_evt_RewardPaid
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
    ,a.staked_cvxfxn
    ,a.cvxFXN_price
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
    ,cumulative_yield_multiplier * cvxFXN_price AS adjusted_price
    ,rolling_7_day_avg_APR * 100 AS rolling_7_day_avg_APR_percent
FROM cumulativeYield
ORDER BY day DESC
