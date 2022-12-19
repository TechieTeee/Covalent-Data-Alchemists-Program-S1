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
