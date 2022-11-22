#Active Addresses for Vending Machines NFT

SELECT  [signed_at:aggregation] as date
        , uniq(tx_sender) AS active_addresses
FROM blockchains.all_chains 
WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
GROUP BY date
ORDER BY date desc
