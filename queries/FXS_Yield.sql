WITH fxs_yield AS (
SELECT 
DATE_TRUNC('day', evt_block_time) AS day
,SUM(yield / POWER(10,18)) AS FXS_yield
FROM ( 
    SELECT 
    evt_block_time
    ,yield
    FROM frax_ethereum.veFXSYieldDistributor_evt_YieldCollected
    
    UNION ALL 
    
    SELECT 
    evt_block_time 
    ,yield 
    FROM frax_ethereum.veFXSYieldDistributorV2_evt_YieldCollected
    
    UNION ALL
    
    SELECT 
    evt_block_time
    ,yield
    FROM frax_ethereum.veFXSYieldDistributorV3_evt_YieldCollected
    
    UNION ALL
    
    
    SELECT 
    evt_block_time
    ,yield
    FROM frax_ethereum.veFXSYieldDistributorV4_evt_YieldCollected
    ) t
    GROUP BY 1
)

, prices AS (
SELECT 
    DATE_TRUNC('day', minute) AS day
    ,AVG(price) AS price
FROM prices.usd
WHERE contract_address = 0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0
GROUP BY 1
)

, supply_frax AS (
    SELECT 
        txn_date AS day
        ,SUM(net_deposits) AS som 
    FROM (
    SELECT 
    DATE_TRUNC('day', CAST(evt_block_time AS timestamp)) AS txn_date
    ,SUM(CAST(value AS DOUBLE) / 1e18) AS net_deposits
FROM frax_ethereum.veFXS_evt_Deposit
GROUP BY 1

UNION ALL

SELECT  
DATE_TRUNC('day', CAST(evt_block_time AS timestamp)) AS txn_date
,-SUM(CAST(value AS DOUBLE) / 1e18) AS net_deposits
FROM frax_ethereum.veFXS_evt_Withdraw
GROUP BY 1
    
    ) t 
    GROUP BY 1
)


, balances_with_gap_days AS (
     SELECT 
        day
        ,SUM(som) OVER (ORDER BY day ASC) AS supply -- balance per day with a transaction
        ,LEAD(day, 1, now()) OVER (ORDER BY day asc) AS next_day
     FROM supply_frax
     )
     
, days AS 
(
    WITH days_seq AS (
        SELECT
        sequence(
            (SELECT cast(min(date_trunc('day', day)) as timestamp) day FROM supply_frax)
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

, YieldMultiplier AS (
SELECT
    a.day
    ,a.supply AS veFXS
    ,a.supply * b.price AS veFXS_mcap
    ,(c.FXS_yield / a.supply) + 1 AS daily_yield_multiplier
    ,(c.FXS_yield / a.supply) * b.price AS daily_yield_stablecoins
    ,(c.FXS_yield / a.supply) AS yield_percent
    ,c.FXS_yield
    ,c.FXS_yield * b.price AS USD_yield
    ,b.price
FROM balance_all_days AS a
LEFT JOIN prices AS b ON a.day = b.day
LEFT JOIN fxs_yield AS c ON a.day = c.day
),

CumulativeYield AS (
    SELECT
        day,
        veFXS,
        veFXS_mcap,
        yield_percent,
        yield_percent * 365 AS APR,
        FXS_yield,
        USD_yield,
        price,
        daily_yield_multiplier,
        SUM(daily_yield_stablecoins) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stablecoins,
        EXP(SUM(LN(daily_yield_multiplier)) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS cumulative_yield_multiplier
    FROM YieldMultiplier
)

SELECT
    day,
    veFXS,
    veFXS_mcap,
    yield_percent,
    POWER(1 + yield_percent, 365) - 1 AS annual_yield,
    APR,
    AVG(APR) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7_day_avg_APR,
    FXS_yield,
    USD_yield,
    price,
    cumulative_yield_multiplier,
    price + cumulative_stablecoins AS stablecoin_portfolio_value,
    price * cumulative_yield_multiplier AS adjusted_price
FROM CumulativeYield
ORDER BY day DESC


    