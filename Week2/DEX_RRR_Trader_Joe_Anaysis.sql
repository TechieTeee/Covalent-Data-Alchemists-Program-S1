/*DEX RRR Trader Joe & WAVAX: USDC, BTC, ETH, Joe & Egg*/


/*Trader Count Avax: USDC, BTC, ETH, Joe & Egg*/
SELECT  [signed_at:aggregation] as date
  , uniq(sender) AS active_addresses
  , sum(active_addresses) OVER (ORDER BY date) as total_addresses
  , uniq(tx_hash) as tx_count
  , sum(tx_count) OVER (ORDER BY date) as total_tx
  , Case
      When aggregator_name = '' then 'No Aggregator' 
      Else aggregator_name
    End as aggregator
  , multiIf(
      lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
  
      , NULL
  ) as pair
FROM reports.dex
  WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
    )
    AND [signed_at:daterange]
    AND chain_name = 'avalanche_mainnet'
    AND protocol_name = 'traderjoe'
    AND event = 'swap'
    AND version = 1
GROUP BY date, pair, aggregator
ORDER BY date, pair desc


/*# of Trades Avax: USDC, BTC, ETH, Joe & Egg*/
SELECT  [signed_at:aggregation] as date
  , uniq(sender) AS active_addresses
  , sum(active_addresses) OVER (ORDER BY date) as total_addresses
  , uniq(tx_hash) as tx_count
  , sum(tx_count) OVER (ORDER BY date) as total_tx
  , Case
      When aggregator_name = '' then 'No Aggregator' 
      Else aggregator_name
    End as aggregator
  , multiIf(
       lower(hex(pair_address))= lower('f4003f4efbe8691b60249e6afbd307abe7758adb'), 'WAVAX/USDC'
      ,lower(hex(pair_address))= lower('2fd81391e30805cc7f2ec827013ce86dc591b806'), 'BTC.b/WAVAX'
      ,lower(hex(pair_address))= lower('fe15c2695f1f920da45c30aae47d11de51007af9'), 'WETH.e/WAVAX'
      ,lower(hex(pair_address))= lower('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'), 'EGG/WAVAX'
      ,lower(hex(pair_address))= lower('454e67025631c065d3cfad6d71e6892f74487a15'), 'JOE/WAVAX'
      , NULL
  ) as pair
FROM reports.dex
  WHERE pair_address IN  (
      unhex('f4003f4efbe8691b60249e6afbd307abe7758adb'),
      unhex('2fd81391e30805cc7f2ec827013ce86dc591b806'),
      unhex('fe15c2695f1f920da45c30aae47d11de51007af9'),
      unhex('3052a75dfd7a9d9b0f81e510e01d3fe80a9e7ec7'),
      unhex('454e67025631c065d3cfad6d71e6892f74487a15')
    )
    AND [signed_at:daterange]
    AND chain_name = 'avalanche_mainnet'
    AND protocol_name = 'traderjoe'
    AND event = 'swap'
    AND version = 1
GROUP BY date, pair, aggregator
ORDER BY date, pair desc


