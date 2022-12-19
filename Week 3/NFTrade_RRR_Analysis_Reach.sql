/*REACH*/

/*Top NFT Marketplaces*/
SELECT count(*) as count, market 
FROM reports.nft_sales_all_chains 
GROUP BY market 
ORDER BY count DESC


/*NFTrade Sales By Collection (AVAX)*/
SELECT  [signed_at:aggregation] as date
        , count(tx_hash) as NFT_sales,
        collection_name
FROM reports.nft_sales_all_chains
where chain_name = 'avalanche_mainnet'
and market = 'nftrade'
and collection_name != ''
and [signed_at:daterange]
GROUP BY date, collection_name


/*NFTrade Sales By Collection (AVAX)*/
SELECT  [signed_at:aggregation] as date
        , count(tx_hash) as NFT_sales,
        collection_name
FROM reports.nft_sales_all_chains
where chain_name = 'avalanche_mainnet'
and market = 'nftrade'
and collection_name != ''
and [signed_at:daterange]
GROUP BY date, collection_name


/*NFTrade User Market Share (AVAX)*/
with market_users as ( 
        SELECT [signed_at:aggregation] as date
       , uniq(addresses) AS active_market_users
FROM  (
                SELECT signed_at, maker as addresses
                    FROM reports.nft_sales_all_chains
                    where chain_name = 'avalanche_mainnet'
                    and market = 'nftrade'
                    AND [signed_at:daterange]                     
                UNION ALL 
                    SELECT signed_at, taker as addresses
                    FROM reports.nft_sales_all_chains
                    where chain_name = 'avalanche_mainnet'
                    and market = 'nftrade'
                    AND [signed_at:daterange]) a
GROUP BY date
ORDER BY date
  ),
  
chain_users as (
  SELECT [signed_at:aggregation] as date
       , uniq(addresses) AS active_chain_users
FROM  (
                      SELECT signed_at, maker as addresses
                          FROM reports.nft_sales_all_chains
                          WHERE chain_name = 'avalanche_mainnet'
                          AND [signed_at:daterange]                     
                      UNION ALL 
                          SELECT signed_at, taker as addresses
                          FROM reports.nft_sales_all_chains
                          WHERE chain_name = 'avalanche_mainnet'
                          AND [signed_at:daterange]) b
GROUP BY date
ORDER BY date
  )
  
SELECT t2.date as date,
  t1.active_market_users as a,
  t2.active_chain_users as b,
  a/b as user_marketshare
FROM chain_users t2 JOIN market_users t1 ON t1.date=t2.date
GROUP BY date,a,b
ORDER BY date


/*NFTrade Sales Numbers Over Time*/
SELECT  [signed_at:aggregation] as date
        , count(tx_hash) as NFT_sales
FROM reports.nft_sales_all_chains
where chain_name = 'avalanche_mainnet'
and market = 'nftrade'
and [signed_at:daterange]
GROUP BY date
