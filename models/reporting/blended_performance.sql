{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH paid_data as
    (SELECT 'Meta' as channel, date, date_granularity,
      COALESCE(SUM(spend),0) as spend, COALESCE(SUM(link_clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as paid_purchases,
      COALESCE(SUM(revenue),0) as paid_revenue
    FROM {{ source('reporting','facebook_ad_performance') }}
    GROUP BY 1,2,3
    
    UNION ALL
    
    SELECT 'Google' as channel, date, date_granularity,
      COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as paid_purchases,
      COALESCE(SUM(revenue),0) as paid_revenue
    FROM {{ source('reporting','googleads_campaign_performance') }}
    GROUP BY 1,2,3
    
    UNION ALL
    
    SELECT 'Bing' as channel, date, date_granularity,
      COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(purchases),0) as paid_purchases,
      COALESCE(SUM(revenue),0) as paid_revenue
    FROM {{ source('reporting','bingads_campaign_performance') }}
    GROUP BY 1,2,3)

SELECT 
  channel,
  date,
  date_granularity,
  spend,
  clicks,
  impressions,
  paid_purchases,
  paid_revenue
FROM paid_data
