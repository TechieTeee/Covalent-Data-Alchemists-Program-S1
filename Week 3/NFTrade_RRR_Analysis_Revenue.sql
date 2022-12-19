/*Revenue Analysis*/

/*NFTrade Fee Revenue Growth Rate (USDC:AVAX)*/
/*Contract Address Used for WAVAX/USDC Conversion*/
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


/*NFTrade Sales Volume in USD & Growth Rate*/
/*Avalanche Main Net*/
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


/*NFTrade Sales Vlme By Collection Last 30 Days*/
/*Avalanche Main Net*/
SELECT sum(nft_token_price_usd) as volume
      , [signed_at:aggregation] as date 
      , collection_name
FROM reports.nft_sales_all_chains
where chain_name = 'avalanche_mainnet'
and market = 'nftrade'
AND signed_at > now() - interval '30 day'
and collection_name != ''  
GROUP BY date, collection_name


/*NFTrade Sales Market Share (AVAX)*/
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
