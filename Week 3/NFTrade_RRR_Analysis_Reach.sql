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
