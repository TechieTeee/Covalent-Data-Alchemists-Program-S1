SELECT  [signed_at:aggregation] as date
        , uniq(tx_hash) as transactions                                                              
FROM blockchains.all_chains
WHERE chain_name = 'avalanche_mainnet'
        AND tx_recipient = unhex('00f5D01D86008D14d04E29EFe88DffC75a9cAc47')
        AND successful = 1
        AND [signed_at:daterange]
GROUP BY date
