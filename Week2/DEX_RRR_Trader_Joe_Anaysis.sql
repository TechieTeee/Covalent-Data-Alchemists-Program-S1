--DEX RRR Trader Joe & WAVAX: USDC, BTC, ETH, Joe & Egg

--REACH
--Trader Count Avax: USDC, BTC, ETH, Joe & Egg
SELECT  [signed_at:aggregation] as date
  , uniq(sender) AS active_addresses
  , sum(active_addresses) OVER (ORDER BY date) as total_addresses
  , uniq(tx_hash) as tx_count
  , sum(tx_count) OVER (ORDER BY date) as total_tx
  , Case
      When aggregator_name = '' then 'No Aggregator' 
      Else aggregator_name
    End as aggregator
  , multiIf(
      lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
  
      , NULL
  ) as pair
FROM reports.dex
  WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
    )
    AND [signed_at:daterange]
    AND chain_name = 'avalanche_mainnet'
    AND protocol_name = 'traderjoe'
    AND event = 'swap'
    AND version = 1
GROUP BY date, pair, aggregator
ORDER BY date, pair desc


--# of Trades Avax: USDC, BTC, ETH, Joe & Egg
SELECT  [signed_at:aggregation] as date
  , uniq(sender) AS active_addresses
  , sum(active_addresses) OVER (ORDER BY date) as total_addresses
  , uniq(tx_hash) as tx_count
  , sum(tx_count) OVER (ORDER BY date) as total_tx
  , Case
      When aggregator_name = '' then 'No Aggregator' 
      Else aggregator_name
    End as aggregator
  , multiIf(
       lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
      , NULL
  ) as pair
FROM reports.dex
  WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
    )
    AND [signed_at:daterange]
    AND chain_name = 'avalanche_mainnet'
    AND protocol_name = 'traderjoe'
    AND event = 'swap'
    AND version = 1
GROUP BY date, pair, aggregator
ORDER BY date, pair desc


--# of New Trades: USDC, BTC, ETH, Joe & Egg
WITH active_addresses AS (
  SELECT signed_at
    , sender as address
    , multiIf(
       lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
        , NULL
    ) as pair
  FROM reports.dex
    WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
      )
      AND [signed_at:daterange]
      AND chain_name = 'avalanche_mainnet'
      AND protocol_name = 'traderjoe'
      AND event = 'swap'
      AND version = 1
)
SELECT date, uniq(address), pair
FROM (
    SELECT min([signed_at:aggregation]) AS date, address, pair
    FROM active_addresses 
    GROUP BY address, pair
)
WHERE [date:daterange]
GROUP BY date, pair
ORDER BY date, pair desc


--RETENTION

--Mint to Burn Ratio: Market Price (JOE/WAVAX)
with mint as (
  SELECT [signed_at:aggregation] as date, sum(amount0_unscaled) as amount
  FROM reports.dex
    WHERE pair_address = unhex('454e67025631c065d3cfad6d71e6892f74487a15')
      AND [signed_at:daterange]
      AND chain_name = 'avalanche_mainnet'
      AND protocol_name = 'traderjoe'
      AND event = 'add_liquidity'
  GROUP BY date
),

burn as (
  SELECT [signed_at:aggregation] as date, sum(amount0_unscaled) as amount
  FROM reports.dex
    WHERE pair_address = unhex('454e67025631c065d3cfad6d71e6892f74487a15')
      AND [signed_at:daterange]
      AND chain_name = 'avalanche_mainnet'
      AND protocol_name = 'traderjoe'
      AND event = 'remove_liquidity'
  GROUP BY date
),

price as (
  SELECT dt AS date, price_in_usd AS price
    FROM reports.token_prices
      WHERE [signed_at:daterange]
        AND contract_address = lower('6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd')
        AND chain_name = 'avalanche_mainnet'
)


--Cohort Retention (WAVAX/USDC)
with user_cohorts as (
    SELECT  sender as address
            , min(date_trunc('month', signed_at)) as cohortMonth
    FROM reports.dex
    WHERE pair_address = unhex('f4003f4efbe8691b60249e6afbd307abe7758adb')
        AND chain_name = 'avalanche_mainnet'
    GROUP BY address
),
following_months as (
    SELECT  sender as address
            , date_diff('month', uc.cohortMonth, date_trunc('month', signed_at))  as month_number
    FROM reports.dex
    LEFT JOIN user_cohorts uc ON address = uc.address
    WHERE pair_address = unhex('f4003f4efbe8691b60249e6afbd307abe7758adb')
        AND chain_name = 'avalanche_mainnet'
    GROUP BY address, month_number
),
cohort_size as (
    SELECT  uc.cohortMonth as cohortMonth
            , count(*) as num_users
    FROM user_cohorts uc
    GROUP BY cohortMonth
    ORDER BY cohortMonth
),
retention_table as (
    SELECT  c.cohortMonth as cohortMonth
            , o.month_number as month_number
            , count(*) as num_users
    FROM following_months o
    LEFT JOIN user_cohorts c ON o.address = c.address
    GROUP BY cohortMonth, month_number
)
SELECT  r.cohortMonth
        , s.num_users as new_users
        , r.month_number
        , r.num_users / s.num_users as retention
FROM retention_table r
LEFT JOIN cohort_size s 
	ON r.cohortMonth = s.cohortMonth
WHERE r.month_number != 0
ORDER BY r.cohortMonth, r.month_number

--Cohort Retention (JOE/WAVAX)
with user_cohorts as (
    SELECT  sender as address
            , min(date_trunc('month', signed_at)) as cohortMonth
    FROM reports.dex
    WHERE pair_address = unhex('454e67025631c065d3cfad6d71e6892f74487a15')
        AND chain_name = 'avalanche_mainnet'
    GROUP BY address
),
following_months as (
    SELECT  sender as address
            , date_diff('month', uc.cohortMonth, date_trunc('month', signed_at))  as month_number
    FROM reports.dex
    LEFT JOIN user_cohorts uc ON address = uc.address
    WHERE pair_address = unhex('454e67025631c065d3cfad6d71e6892f74487a15')
        AND chain_name = 'avalanche_mainnet'
    GROUP BY address, month_number
),
cohort_size as (
    SELECT  uc.cohortMonth as cohortMonth
            , count(*) as num_users
    FROM user_cohorts uc
    GROUP BY cohortMonth
    ORDER BY cohortMonth
),
retention_table as (
    SELECT  c.cohortMonth as cohortMonth
            , o.month_number as month_number
            , count(*) as num_users
    FROM following_months o
    LEFT JOIN user_cohorts c ON o.address = c.address
    GROUP BY cohortMonth, month_number
)
SELECT  r.cohortMonth

/*Stickiness Ratio AVAX, USDC, BTC, ETH, Joe & Egg*/
With daily_active_users as (
SELECT date_trunc('month', day) as date, avg(active_addresses) as avg_dau
FROM (
    SELECT date_trunc('day', signed_at) as day, uniq(sender) AS active_addresses
        , multiIf(
       lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
            , NULL
        ) as pair
    FROM reports.dex
      WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
        )
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
        AND protocol_name = 'traderjoe'
        AND event = 'swap'
        AND version = 1
        AND [signed_at:daterange]
    GROUP BY day, pair
		)
GROUP BY date
),
monthly_active_users as (
		SELECT date_trunc('month', signed_at) as date, uniq(sender) AS mau
          , multiIf(
       lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
            , NULL
        ) as pair
    FROM reports.dex
      WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
        )
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
        AND protocol_name = 'traderjoe'
        AND event = 'swap'
        AND version = 1
        AND [signed_at:daterange]
		GROUP BY date, pair
)
SELECT daily.date as date, (daily.avg_dau/monthly.mau) as stickiness_ratio, pair
FROM daily_active_users daily
  LEFT JOIN monthly_active_users monthly
  	ON daily.date = monthly.date
ORDER BY date, pair



--Revenue

--Pools Volume: Avax: USDC, BTC, ETH, Joe & Egg
SELECT sum(abs(amount0_unscaled)/power(10, prices.num_decimals)*prices.price_in_usd) as Volume
  , [signed_at:aggregation] as date
  --, sum(Volume) OVER (ORDER BY date) as cum_vol
  , multiIf(
       lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
      , NULL
  ) as pair
FROM reports.dex dex
LEFT JOIN (
    SELECT dt, contract_address, price_in_usd, num_decimals
    FROM reports.token_prices 
		WHERE chain_name = 'avalanche_mainnet'
		) prices
			ON hex(dex.token0_address) = upper(prices.contract_address)
				AND [dex.signed_at:aggregation] = prices.dt
    WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
    )
    AND chain_name = 'avalanche_mainnet'
    AND protocol_name = 'traderjoe'
    AND event = 'swap'
    AND [signed_at:daterange]
GROUP BY date, pair
ORDER BY date, pair asc

--Median Liquidity Added/Time Trader Joe
SELECT quantile(abs(amount0_unscaled)/power(10, prices0.num_decimals)*prices0.price_in_usd) as median_Token0
  , quantile(abs(amount1_unscaled)/power(10, prices1.num_decimals)*prices1.price_in_usd) as median_Token1
  , median_Token0 + median_Token1 as median_added_liquidity
  , avg(abs(amount0_unscaled)/power(10, prices0.num_decimals)*prices0.price_in_usd) as mean_Token0
  , avg(abs(amount1_unscaled)/power(10, prices1.num_decimals)*prices1.price_in_usd) as mean_Token1
  , mean_Token0 + mean_Token1 as mean_added_liquidity
  , [signed_at:aggregation] as date
  , multiIf(
       lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
      , NULL
  ) as pair
FROM reports.dex
  LEFT JOIN (
    SELECT dt, contract_address, price_in_usd, num_decimals
    FROM reports.token_prices 
		WHERE chain_name = 'avalanche_mainnet'
		) prices0
			ON hex(dex.token0_address) = upper(prices0.contract_address)
				AND [signed_at:aggregation] = prices0.dt
  LEFT JOIN (
    SELECT dt, contract_address, price_in_usd, num_decimals
    FROM reports.token_prices 
		WHERE chain_name = 'avalanche_mainnet'
		) prices1
			ON hex(dex.token1_address) = upper(prices1.contract_address)
				AND [signed_at:aggregation] = prices1.dt
  WHERE pair_address = unhex('f4003f4efbe8691b60249e6afbd307abe7758adb')
    AND chain_name = 'avalanche_mainnet'
    AND protocol_name = 'traderjoe'
    AND version = 1
    AND event = 'add_liquidity'
    AND [signed_at:daterange]
GROUP BY date, pair
ORDER BY date, pair

UNION ALL


--Millionaire Transfer Volume WAVAX
SELECT extract_address(hex(e.topic1)) as wallet
			 , sum(prices.price_in_usd * to_float64_raw(data0)/ pow(10, prices.num_decimals)) as amount_transfered
FROM blockchains.all_chains e
    INNER JOIN (
      SELECT contract_address, dt, price_in_usd, num_decimals
      FROM reports.token_prices prices
      WHERE chain_name = 'avalanche_mainnet'
       ) prices
            ON prices.contract_address = lower(hex(e.log_emitter))
                AND date_trunc('day', e.signed_at) = prices.dt
WHERE e.topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
AND e.chain_name = 'avalanche_mainnet'
AND e.signed_at > now() - interval '3 month'
AND (prices.price_in_usd * to_float64_raw(data0)/ pow(10, prices.num_decimals)) < 20000000000 -- Filter out all the transfers of shitcoins
GROUP BY wallet
HAVING amount_transfered >= 1000000
ORDER BY amount_transfered DESC


--Swap VLM Dominated Hvy Dex Traders JOE/WAVAX
WITH heavy_dex_traders AS (

SELECT 
('0x' || hex(sender)) AS user,  
  sender,
  count -- Final SELECT statement
FROM( -- Subquery 1
    SELECT count(tx_hash) as count, sender
    FROM reports.dex
    WHERE chain_name = 'avalanche_mainnet'
    GROUP BY sender
    ORDER BY count DESC
) x
INNER JOIN ( -- Subquery 2
    SELECT quantile(0.95)(count) as quartile
    FROM (
          SELECT count(tx_hash) as count, sender
          FROM reports.dex
          WHERE chain_name = 'avalanche_mainnet'
          GROUP BY sender
          ORDER BY count DESC
        )
) c
        ON 1=1
WHERE count > quartile

),

swap_vol_all AS (

  SELECT sum(abs(amount1_usd)) AS usd_volume_all
			 , [signed_at:aggregation] as date 
FROM reports.dex
WHERE pair_address = unhex('454e67025631c065d3cfad6d71e6892f74487a15')
AND chain_name = 'avalanche_mainnet'
AND protocol_name = 'traderjoe'
AND event = 'swap'
AND [signed_at:daterange]
GROUP BY date
),

swap_vol_heavy_dex_traders AS (
  
SELECT sum(abs(amount1_usd)) AS usd_volume_heavy_dex
			 , [signed_at:aggregation] as date 
FROM reports.dex
WHERE pair_address = unhex('454e67025631c065d3cfad6d71e6892f74487a15')
AND chain_name = 'avalanche_mainnet'
AND protocol_name = 'traderjoe'
AND event = 'swap'
AND sender IN (
  SELECT sender
  FROM heavy_dex_traders)
AND [signed_at:daterange]
GROUP BY date 

)

SELECT a.date,
usd_volume_heavy_dex,
usd_volume_all,
usd_volume_heavy_dex/usd_volume_all AS percentage_of_swap_vol
FROM swap_vol_heavy_dex_traders h
FULL JOIN swap_vol_all a
ON a.date = h.date
