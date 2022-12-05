/*REACH*/

/*Top NFT Marketplaces*/
SELECT count(*) as count, market 
FROM reports.nft_sales_all_chains 
GROUP BY market 
ORDER BY count DESC
