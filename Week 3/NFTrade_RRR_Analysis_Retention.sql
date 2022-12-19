/*RETENTION ANALYSIS*/


/*NFTrade Buyer Retention (AVAX)*/
WITH user_cohorts as (
    SELECT  taker as address
            , min(date_trunc('month', signed_at)) as cohortMonth
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'avalanche_mainnet'
    AND market = 'nftrade'
    GROUP BY address
),
following_months as (
    SELECT  taker as address
            , date_diff('month', uc.cohortMonth, date_trunc('month', signed_at))  as month_number
    FROM reports.nft_sales_all_chains
    LEFT JOIN user_cohorts uc ON address = uc.address
    WHERE chain_name = 'avalanche_mainnet'
    AND market = 'nftrade'
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
AND [cohortMonth:daterange]
ORDER BY r.cohortMonth, r.month_number


/*NFTrade Seller Retention (AVAX)*/
WITH user_cohorts as (
    SELECT  maker as address
            , min(date_trunc('month', signed_at)) as cohortMonth
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'avalanche_mainnet'
    AND market = 'nftrade'
    GROUP BY address
),
following_months as (
    SELECT  maker as address
            , date_diff('month', uc.cohortMonth, date_trunc('month', signed_at))  as month_number
    FROM reports.nft_sales_all_chains
    LEFT JOIN user_cohorts uc ON address = uc.address
    WHERE chain_name = 'avalanche_mainnet'
    AND market = 'nftrade'
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
AND [cohortMonth:daterange]
ORDER BY r.cohortMonth, r.month_number


/*NFTrade Volume Distribution v.s. Competitors*/
with cte1 as (
SELECT maker as wallet
  FROM reports.nft_sales_all_chains
  WHERE chain_name = 'avalanche_mainnet'
  AND market = 'nftrade'
  AND [signed_at:daterange]
  
)
  SELECT 
  sum(nft_token_price_usd) as volume
      , [signed_at:aggregation] as date
      , market
FROM reports.nft_sales_all_chains sales INNER JOIN cte1 ON sales.maker=cte1.wallet
where chain_name = 'avalanche_mainnet'
AND [signed_at:daterange]
GROUP BY date, market
HAVING volume IS NOT NULL AND volume !=0
