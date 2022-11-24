/*Vending Machines NFTs Reach, Retention and Revenue SQL Queries*/


/*#Reach for Vending Machines*/

/*#Active Addresses for Vending Machines NFTs*/
SELECT  [signed_at:aggregation] as date
        , uniq(tx_sender) AS active_addresses
FROM blockchains.all_chains 
WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
GROUP BY date
ORDER BY date desc


/*#Transaction Count for Vending Machines NFTs*/
SELECT  [signed_at:aggregation] as date
        , uniq(tx_hash) as transactions                                                              
FROM blockchains.all_chains
WHERE chain_name = 'avalanche_mainnet'
        AND tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND successful = 1
        AND [signed_at:daterange]
GROUP BY date

/*#TRX Per Active Address Vending Machines NFTs*/
with t as(SELECT  [signed_at:aggregation] as date
        , uniq(tx_hash) as Transactions                                                              
FROM blockchains.all_chains
WHERE chain_name = 'avalanche_mainnet'
        AND tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND successful = 1
        AND [signed_at:daterange]
and date > '2021-11-01'
GROUP BY date) , 
  

a as (SELECT  [signed_at:aggregation] as date
        , uniq(tx_sender) AS Active
FROM blockchains.all_chains 
WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
  and date > '2021-11-01'
GROUP BY date
ORDER BY date desc) 

select  t.date, t.Transactions, a.Active, cast(t.Transactions/a.Active as int) as transaction_per_active
  from t join a on t.date=a.date
