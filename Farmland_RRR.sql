/*Reach Farmland SQL Queries*/

/*Active Addresses for Farmland*/
SELECT  [signed_at:aggregation] as date
        , uniq(tx_sender) AS active_addresses
FROM blockchains.all_chains 
WHERE tx_recipient = unhex('00f5D01D86008D14d04E29EFe88DffC75a9cAc47')
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
GROUP BY date
ORDER BY date desc


/*Transactions for Farmland*/
SELECT  [signed_at:aggregation] as date
        , uniq(tx_hash) as transactions                                                              
FROM blockchains.all_chains
WHERE chain_name = 'avalanche_mainnet'
        AND tx_recipient = unhex('00f5D01D86008D14d04E29EFe88DffC75a9cAc47')
        AND successful = 1
        AND [signed_at:daterange]
GROUP BY date

/*Transactions Per Active Address Farmland*/
with t as(SELECT  [signed_at:aggregation] as date
        , uniq(tx_hash) as Transactions                                                              
FROM blockchains.all_chains
WHERE chain_name = 'avalanche_mainnet'
        AND tx_recipient = unhex('00f5D01D86008D14d04E29EFe88DffC75a9cAc47')
        AND successful = 1
        AND [signed_at:daterange]
and date > '2021-11-01'
GROUP BY date) , 
  

a as (SELECT  [signed_at:aggregation] as date
        , uniq(tx_sender) AS Active
FROM blockchains.all_chains 
WHERE tx_recipient = unhex('00f5D01D86008D14d04E29EFe88DffC75a9cAc47')
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
  and date > '2021-11-01'
GROUP BY date
ORDER BY date desc) 

select  t.date, t.Transactions, a.Active, cast(t.Transactions/a.Active as int) as transaction_per_active
  from t join a on t.date=a.date
