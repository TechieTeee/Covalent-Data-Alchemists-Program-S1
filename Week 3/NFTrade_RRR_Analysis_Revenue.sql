--Revenue Analysis

--NFTrade Fee Revenue Growth Rate (USDC:AVAX)
--Contract Address Used for WAVAX/USDC Conversion
SELECT date
       , volume
       , (volume/previous)-1 as growth_rate
FROM (
      SELECT [signed_at:aggregation] as date
             , sum((fees_paid_unscaled/pow(10,18))*price_in_usd) as volume 
             , lagInFrame(volume) OVER (ORDER BY date) as previous
      FROM reports.nft_sales_all_chains sales
        LEFT JOIN (
          SELECT dt, contract_address, price_in_usd
          FROM reports.token_prices 
      		WHERE chain_name = 'avalanche_mainnet'
              and contract_address = lower('B31f66AA3C1e785363F0875A1B74E27b85FD66c7')
      ) prices
      				ON date_trunc('day', sales.signed_at) = prices.dt
WHERE chain_name = 'avalanche_mainnet'
AND market = 'nftrade'
AND [signed_at:daterange]
GROUP BY date
ORDER BY date ASC 
    )


--NFTrade Sales Volume in USD & Growth Rate
--Avalanche Main Net
SELECT date, volume, (volume/previous)-1 as growth_rate
FROM(
SELECT [signed_at:aggregation] as date
       , sum(nft_token_price_usd) as volume 
       , lagInFrame(volume) OVER (ORDER BY date) as previous
FROM reports.nft_sales_all_chains  
WHERE chain_name = 'avalanche_mainnet'
AND market = 'nftrade'
AND [signed_at:daterange]
GROUP BY date
ORDER BY date ASC 
)


--NFTrade Sales Vlme By Collection Last 30 Days
--Avalanche Main Net
SELECT sum(nft_token_price_usd) as volume
      , [signed_at:aggregation] as date 
      , collection_name
FROM reports.nft_sales_all_chains
where chain_name = 'avalanche_mainnet'
and market = 'nftrade'
AND signed_at > now() - interval '30 day'
and collection_name != ''  
GROUP BY date, collection_name


--NFTrade Sales Market Share (AVAX)
with cte1 as (
SELECT sum(nft_token_price_usd) as volume
      , [signed_at:aggregation] as date 
FROM reports.nft_sales_all_chains
WHERE chain_name = 'avalanche_mainnet'
AND market = 'nftrade'
AND [signed_at:daterange]
GROUP BY date
),
cte2 as (
SELECT sum(nft_token_price_usd) as volume
      , [signed_at:aggregation] as date 
FROM reports.nft_sales_all_chains
WHERE chain_name = 'avalanche_mainnet'
AND [signed_at:daterange]
GROUP BY date
)
SELECT cte2.date,
cte1.volume/cte2.volume AS sales_marketshare
FROM cte2 JOIN cte1 ON cte2.date=cte1.date


--NFTrade Revenue/User USDC & Growth Rate
--Avalanche Main Net
with cte1 as (      SELECT [signed_at:aggregation] as date, uniq(taker) as buyers
                    FROM reports.nft_sales_all_chains
                    WHERE chain_name = 'avalanche_mainnet'
                    AND market = 'nftrade'
                    AND [signed_at:daterange] 
                    GROUP BY date
                    ORDER BY date desc),
cte2 as (
SELECT date, volume
FROM(
SELECT [signed_at:aggregation] as date
       ,sum((fees_paid_unscaled/pow(10,18))*price_in_usd) as volume 
       ,lagInFrame(volume) OVER (ORDER BY date) as previous
FROM reports.nft_sales_all_chains sales
  LEFT JOIN (
    SELECT dt, contract_address, price_in_usd
    FROM reports.token_prices 
		WHERE chain_name = 'avalanche_mainnet'
        AND contract_address = lower('B31f66AA3C1e785363F0875A1B74E27b85FD66c7')
		) prices
				ON date_trunc('day', sales.signed_at) = prices.dt
WHERE chain_name = 'avalanche_mainnet'
AND market = 'nftrade'
AND [signed_at:daterange]
GROUP BY date
ORDER BY date ASC 
))

SELECT final_date, volume as revenue_per_buyer, (volume/previous)-1 as growth_rate
FROM(
SELECT cte2.date as final_date
       ,cte2.volume/cte1.buyers as volume 
       ,lagInFrame(volume) OVER (ORDER BY final_date) as previous
FROM cte2 JOIN cte1 ON cte1.date=cte2.date
GROUP BY final_date, volume
ORDER BY final_date ASC 
)


--NFTrade % of Sales from Whales (USDC:AVAX)
WITH wallet_list AS (
  SELECT count, taker as wallet
  FROM(
      SELECT count(tx_hash) as count, taker
      FROM reports.nft_sales_all_chains
      WHERE [chain_name:chainname]
      GROUP BY taker
      ORDER BY count DESC
  ) x
  INNER JOIN (
      SELECT quantile(0.99)(count) as quartile
      FROM (
            SELECT count(tx_hash) as count, taker
            FROM reports.nft_sales_all_chains
            WHERE [chain_name:chainname]
            GROUP BY taker
            ORDER BY count DESC
          )
  ) y
  ON 1=1
  WHERE count > quartile
),
market AS (
  SELECT [signed_at:aggregation] as date
         , sum(nft_token_price_usd) as volume
  FROM reports.nft_sales_all_chains  
  WHERE [chain_name:chainname]
  AND market = 'nftrade'
  AND [signed_at:daterange]
  GROUP BY date
),
wallet_volume AS (
  SELECT [signed_at:aggregation] as date
         ,sum(nft_token_price_usd) as volume
  FROM reports.nft_sales_all_chains
    INNER JOIN wallet_list
      ON wallet_list.wallet = nft_sales_all_chains.taker
  WHERE [chain_name:chainname]
  AND market = 'nftrade'
  AND [signed_at:daterange]
  GROUP BY date
)
SELECT market.date, wallet_volume.volume/market.volume as "Percentage"
FROM market
  INNER JOIN wallet_volume
    ON market.date = wallet_volume.date
