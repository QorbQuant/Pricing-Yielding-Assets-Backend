WITH days AS (
    SELECT time 
    FROM (unnest(sequence(CAST('2023-09-27' AS DATE), CAST(NOW() AS DATE), INTERVAL '1' day)) AS s(time)
    )
    )

--- VELO IN RELAY 
-----------------------------------
, velo AS ( 
SELECT 
    evt_block_date AS day
    ,SUM(deposits) AS net_velo
FROM (
SELECT 
    evt_block_date
    ,SUM(_weight) AS deposits
FROM velodrome_v2_optimism.VotingEscrow_evt_DepositManaged 
GROUP BY 1

UNION ALL 

SELECT 
    evt_block_date
    ,-SUM(_weight) AS withdraws
FROM velodrome_v2_optimism.VotingEscrow_evt_WithdrawManaged
GROUP BY 1)
GROUP BY 1
)

, velo_relay AS (
SELECT
    day
    ,net_velo
    ,SUM(net_velo) OVER (ORDER BY day) AS velo_in_relay
FROM (
    SELECT 
        a.time AS day
        ,(b.net_velo / POWER(10,18)) AS net_velo
    FROM days a
    LEFT JOIN velo b ON a.time = b.day
)
)

--- VELO COMPOUNDER
-----------------------------------

, comp_data AS (
SELECT
    evt_block_date
    ,SUM(balanceCompounded / POWER(10,18)) AS comp_amount
FROM velodrome_v2_optimism.AutoCompounder_evt_Compound
GROUP BY 1
)

, velo_combined AS (
SELECT 
    day
    --,velo_in_relay + comp_amount_cum AS velo_balance
    ,velo_in_relay AS velo_balance
    ,comp_amount
    ,comp_amount / (velo_in_relay + comp_amount_cum) AS rate
    ,(comp_amount / (velo_in_relay + comp_amount_cum)) * 365 AS rate_annualized
FROM (
    SELECT 
        a.day
        ,a.velo_in_relay
        ,b.comp_amount
        ,SUM(b.comp_amount) OVER (ORDER BY a.day) AS comp_amount_cum
    FROM velo_relay a
    LEFT JOIN comp_data b ON a.day = b.evt_block_date
    ORDER BY 1 ASC)
    )
    
    , prices AS (
    SELECT 
        a.day
        ,a.velo_balance
        ,a.comp_amount
        ,a.rate
        ,a.rate_annualized
        ,b.price
        ,a.rate + 1 AS daily_yield_multiplier
    FROM velo_combined a
    LEFT JOIN prices.usd_daily AS b ON a.day = b.day
    WHERE b.contract_address = 0x9560e827af36c94d2ac33a39bce1fe78631088db
    AND b.blockchain = 'optimism')
    
    SELECT 
        *
        ,rolling_7_day_avg_APR * 100 AS rolling_7_day_avg_APR_percent
        ,rolling_30_day_avg_APR * 100 AS rolling_30_day_avg_APR_percent
        ,cumulative_yield_multiplier * price AS adjusted_price
        ,price * velo_balance AS relay_tvl
        ,price * comp_amount AS comp_amount_usd
    FROM (
        SELECT 
            *
            ,AVG(rate_annualized) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7_day_avg_APR
            ,AVG(rate_annualized) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30_day_avg_APR
            ,EXP(SUM(LN(daily_yield_multiplier)) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS cumulative_yield_multiplier
        FROM prices)
    ORDER BY day DESC
    
