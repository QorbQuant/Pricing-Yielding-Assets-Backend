WITH r_tokens AS (
SELECT address, symbol, decimals, label
FROM (VALUES
    (0x12275DCB9048680c4Be40942eA4D92c74C63b844, 'eUSD', 18, 'rToken')
    ,(0x18C14C2D707b2212e17d1579789Fc06010cfca23, 'ETH+', 18, 'rToken')
   -- ,(0xCa5Ca9083702c56b481D1eec86F1776FDbd2e594, 'RSR', 18, 'rToken')
    ,(0x0BBF664D46becc28593368c97236FAa0fb397595, 'KNOX', 18, 'rToken')
    ,(0x96a993f06951B01430523D0D5590192d650EBf3e, 'rgUSD', 18, 'rToken')
    --,(0xaf88d065e77c8cC2239327C5EDb3A432268e5831, 'USDC', 6, 'Other')
    --,(0x498Bf2B1e120FeD3ad3D42EA2165E9b73f99C1e5, 'crvUSD', 18, 'Other')
    --,(0x82aF49447D8a07e3bd95BD0d56f35241523fBab1, 'WETH', 18, 'Other')
) AS temp_table (address, symbol, decimals, label)
)

, curve_pools AS (
SELECT address, label 
FROM (VALUES
    (0x93a416206b4ae3204cfe539edfee6bc05a62963e, 'eUSD/USDC')
    ,(0x67d11005af05bb1e9fdb1cfc261c23de3e1055a1, 'eUSD/crvUSD')
    ,(0x45B47fE1bed067de6B4b89e0285E6B571A64c57C, 'TriRSR (ETH+/eUSD/RSR')
    ,(0x6f33daf91d2acae10f5cd7bbe3f31716ed123f1d, 'KNOX/eUSD')
    ,(0x0acacb4f6db7708a5451b835acd39dfebac4eeb5, 'KNOX/rgUSD')
    ,(0x3dcb7b53b6177a04a2aece61d95bf577ecc02241, 'rgUSD/USDC')
    ,(0x0acacb4f6db7708a5451b835acd39dfebac4eeb5, 'ETH+/WETH')
) AS temp_table (address, label)
)

, time AS (
SELECT time 
FROM (unnest(sequence(CAST('2023-02-25' AS DATE), CAST(NOW() AS DATE), INTERVAL '1' DAY)))
AS s(time)
)

, combine AS (
SELECT t.time, rt.address
FROM time t
CROSS JOIN r_tokens rt
ORDER BY t.time)



, transfers AS (
SELECT 
    DATE_TRUNC('day', evt_block_time) AS time
    ,contract_address
    ,SUM(value) AS amount
FROM erc20_arbitrum.evt_transfer
WHERE contract_address IN (SELECT address FROM r_tokens)
AND to IN (SELECT address FROM curve_pools)
AND evt_block_time > DATE('2023-02-25')
GROUP BY 1,2

UNION ALL

SELECT 
     DATE_TRUNC('day', evt_block_time) AS time
    ,contract_address
    ,-SUM(value) AS amount
FROM erc20_arbitrum.evt_transfer
WHERE contract_address IN (SELECT address FROM r_tokens)
AND "from" IN (SELECT address FROM curve_pools)
AND evt_block_time > DATE('2023-02-25')
GROUP BY 1,2
)
    
    

SELECT
    a.time
    ,a.contract_address
    ,a.flow
    ,b.symbol
    ,SUM(a.flow) OVER (PARTITION BY a.contract_address, b.symbol ORDER BY time ) AS balance
FROM (
SELECT 
    c.time
    ,t.contract_address
    ,SUM(t.amount/ POWER(10,18)) AS flow
FROM combine c
LEFT JOIN transfers t ON c.time = t.time AND c.address = t.contract_address
GROUP BY 1,2) a
LEFT JOIN r_tokens b ON b.address = a.contract_address
ORDER BY time DESC