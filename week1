/*Active Addresses for Farmland*/
SELECT  [signed_at:aggregation] as date
        , uniq(tx_sender) AS active_addresses
FROM blockchains.all_chains 
WHERE tx_recipient = unhex('00f5D01D86008D14d04E29EFe88DffC75a9cAc47')
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
GROUP BY date
ORDER BY date desc
