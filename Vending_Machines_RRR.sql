/*Vending Machines NFTs Reach, Retention and Revenue SQL Queries*/


/*#Reach for Vending Machine NFTs*/

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
 
 
 
 
 /*#Retention for Vending Machine NFTs*/
 
/*#Vending Machines NFTs Stickiness Ratio*/
With daily_active_users as (
SELECT date_trunc('month', day) as date, avg(active_addresses) as avg_dau
FROM (
    SELECT date_trunc('day', signed_at) as day, uniq(tx_sender) AS active_addresses
    FROM blockchains.all_chains
    WHERE chain_name = 'avalanche_mainnet'
    AND tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
    AND [signed_at:daterange]
    GROUP BY day
		)
GROUP BY date
),
monthly_active_users as (
		SELECT date_trunc('month', signed_at) as date, uniq(tx_sender) AS mau
    FROM blockchains.all_chains
    WHERE chain_name = 'avalanche_mainnet'
    AND tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
    AND [signed_at:daterange]
		GROUP BY date
)
SELECT daily.date as date, (daily.avg_dau/monthly.mau) as stickiness_ratio
FROM daily_active_users daily
  LEFT JOIN monthly_active_users monthly
  	ON daily.date = monthly.date
ORDER BY date
      
/*#Vending Machine New v.s. Existing Addresses*/
with user_cohorts as (
    SELECT  tx_sender as address
            , min([signed_at:aggregation]) as cohortDate
    FROM blockchains.all_chains
    WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
AND  chain_name = 'avalanche_mainnet'
    GROUP BY address
),
     new_users as (
    SELECT  cohortDate as date, uniq(address) as new_users_count
    FROM user_cohorts uc
    GROUP BY date
),
     all_users as (
    SELECT [signed_at:aggregation] as date
        ,uniq(tx_sender) as total_players
    FROM blockchains.all_chains
    WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
AND  chain_name = 'avalanche_mainnet'
 GROUP BY date
)
    SELECT  au.date
         , nu.new_users_count
         , au.total_players - nu.new_users_count AS Existing_Users
         , (nu.new_users_count/au.total_players)*100 as New_User_Percentage
    FROM all_users au
    LEFT JOIN new_users nu
        ON au.date = nu.date

/*#MoM Retention Vending Machine & New Users*/
with user_cohorts as (
    SELECT  tx_sender as address
            , min(date_trunc('month', signed_at)) as cohortMonth
    FROM blockchains.all_chains
    WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND chain_name = 'avalanche_mainnet'
        AND signed_at >  '2022-01-01'
        /*AND [signed_at:daterange]*/
    GROUP BY address
),
following_months as (
    SELECT  tx_sender as address
            , date_diff('month', uc.cohortMonth, date_trunc('month', signed_at))  as month_number
    FROM blockchains.all_chains
    LEFT JOIN user_cohorts uc ON address = uc.address
    WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND chain_name = 'avalanche_mainnet'
        AND signed_at >  '2022-01-01'
        /*AND [signed_at:daterange]*/
    GROUP BY address, month_number
),
cohort_size as (
    SELECT  uc.cohortMonth as cohortMonth
            , count(*) as num_users
    FROM user_cohorts uc
    GROUP BY cohortMonth
    ORDER BY cohortMonth
),
retention_table as (
    SELECT  c.cohortMonth as cohortMonth
            , o.month_number as month_number
            , count(*) as num_users
    FROM following_months o
    LEFT JOIN user_cohorts c ON o.address = c.address
    GROUP BY cohortMonth, month_number
)
SELECT  r.cohortMonth
        , s.num_users as new_users
        , r.month_number
        , r.num_users / s.num_users as retention
FROM retention_table r
LEFT JOIN cohort_size s 
	ON r.cohortMonth = s.cohortMonth
WHERE r.month_number != 0
ORDER BY r.cohortMonth, r.month_number



/*#Revenue Vending Machines NFTs*/

/*#Vending Machine NFTs Average Gas Prices*/
SELECT [signed_at:aggregation] as date
       , avg((tx.tx_gas_spent/pow(10, 18))* toFloat64(tx.tx_gas_price)) as average_gas_cost
       --, sum((tx.tx_gas_spent/pow(10, 18))* toFloat64(tx.tx_gas_price)) as aggregate_gas_cost
FROM ( 
            SELECT any(tx_hash), tx_gas_spent, tx_gas_price, signed_at
        FROM blockchains.all_chains 
          WHERE chain_name = 'avalanche_mainnet'
          and tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
          AND [signed_at:daterange]
            GROUP BY tx_gas_spent, tx_gas_price, signed_at
    ) tx

 GROUP BY date
 
 /*#Total Gas Weekly AVAX & Vending Machine NFTs*/
 SELECT [signed_at:aggregation] as date
       --, avg((tx.tx_gas_spent/pow(10, 18))* toFloat64(tx.tx_gas_price)) as average_gas_cost
       ,sum((tx.tx_gas_spent/pow(10, 18))* toFloat64(tx.tx_gas_price)) as aggregate_gas_cost
FROM ( 
            SELECT any(tx_hash), tx_gas_spent, tx_gas_price, signed_at
        FROM blockchains.all_chains 
          WHERE chain_name = 'avalanche_mainnet'
          and tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
          AND [signed_at:daterange]
            GROUP BY tx_gas_spent, tx_gas_price, signed_at
    ) tx

 GROUP BY date
 
 /*#Gas Per Active Address for Vending Machines*/
 with total as (SELECT [signed_at:aggregation] as date
       --, avg((tx.tx_gas_spent/pow(10, 18))* toFloat64(tx.tx_gas_price)) as average_gas_cost
       ,sum((tx.tx_gas_spent/pow(10, 18))* toFloat64(tx.tx_gas_price)) as aggregate_gas_cost
FROM ( 
            SELECT any(tx_hash), tx_gas_spent, tx_gas_price, signed_at
        FROM blockchains.all_chains 
          WHERE chain_name = 'avalanche_mainnet'
          and tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
          AND [signed_at:daterange]
            GROUP BY tx_gas_spent, tx_gas_price, signed_at
    ) tx
 GROUP BY date),

active as (SELECT  [signed_at:aggregation] as date
        , uniq(tx_sender) AS Active_Addresses
FROM blockchains.all_chains 
WHERE tx_recipient = unhex('BbD9786f178e2AEBb4b4329c41A821921ca05339')
        AND [signed_at:daterange]
        AND chain_name = 'avalanche_mainnet'
GROUP BY date
ORDER BY date desc)

select total.date, Active_Addresses, aggregate_gas_cost, aggregate_gas_cost/Active_Addresses as gas_per_active
from active join total on total.date=active.date
