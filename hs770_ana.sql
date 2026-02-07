SELECT * FROM `hs770report.analytics_387639477.events_*` LIMIT 10;

--checking usage
SELECT
  creation_time,
  job_id,
  
  ROUND(total_bytes_processed / pow(10,9), 4) as gbs_processed,
  query
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT  
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
ORDER BY
  creation_time DESC;

select count(distinct user_pseudo_id) as users,
count(*) as total_events
from `hs770report.analytics_387639477.events_20260125`;

--view 1 session breakdown

WITH base_events AS (
  SELECT
    
    user_pseudo_id,

    
    CAST(
      (SELECT value.int_value
       FROM UNNEST(event_params)
       WHERE key = 'ga_session_id'
      ) AS STRING
    ) AS session_id,

    -- Date & time
    event_date,
    event_name,

    -- Traffic source (session-level attribution)
    traffic_source.source AS source,
    traffic_source.medium AS medium,

    -- Device
    device.category AS device_category

  FROM `hs770report.analytics_387639477.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
),

session_flags AS (
  SELECT
    user_pseudo_id,
    session_id,
    event_date,
    source,
    medium,
    device_category,

    
    MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS viewed_item,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS began_checkout,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchased,

   
    MAX(CASE WHEN event_name = 'generate_lead' THEN 1 ELSE 0 END) AS generated_lead

  FROM base_events
  WHERE session_id IS NOT NULL
  GROUP BY
    user_pseudo_id,
    session_id,
    event_date,
    source,
    medium,
    device_category
)

SELECT *
FROM session_flags;


--view 2 funnel view
CREATE OR REPLACE VIEW `hs770report.analytics_387639477.vw_funnel_diagnostics` AS

WITH funnel_counts AS (
  SELECT
    device_category,

    COUNT(DISTINCT session_id) AS sessions,

    SUM(viewed_item) AS viewed_item_sessions,
    SUM(added_to_cart) AS add_to_cart_sessions,
    SUM(began_checkout) AS begin_checkout_sessions,
    SUM(purchased) AS purchase_sessions

  FROM `hs770report.analytics_387639477.vw_sessions_summary`
  GROUP BY device_category
),

funnel_steps AS (
  SELECT
    device_category,
    'view_item' AS step,
    sessions AS sessions_at_step,
    viewed_item_sessions AS sessions_completed_step
  FROM funnel_counts

  UNION ALL

  SELECT
    device_category,
    'add_to_cart',
    viewed_item_sessions,
    add_to_cart_sessions
  FROM funnel_counts

  UNION ALL

  SELECT
    device_category,
    'begin_checkout',
    add_to_cart_sessions,
    begin_checkout_sessions
  FROM funnel_counts

  UNION ALL

  SELECT
    device_category,
    'purchase',
    begin_checkout_sessions,
    purchase_sessions
  FROM funnel_counts
)

SELECT
  device_category,
  step,
  sessions_at_step,
  sessions_completed_step,
  SAFE_DIVIDE(sessions_completed_step, sessions_at_step) AS step_conversion_rate,
  1 - SAFE_DIVIDE(sessions_completed_step, sessions_at_step) AS step_dropoff_rate
FROM funnel_steps;


--sanity check for funnel view
SELECT
  device_category,
  step,
  sessions_at_step,
  sessions_completed_step,
  step_dropoff_rate
FROM `hs770report.analytics_387639477.vw_funnel_diagnostics`
ORDER BY
  device_category,
  CASE step
    WHEN 'view_item' THEN 1
    WHEN 'add_to_cart' THEN 2
    WHEN 'begin_checkout' THEN 3
    WHEN 'purchase' THEN 4
  END;


  --view 3 for engagement segments
  CREATE OR REPLACE VIEW `hs770report.analytics_387639477.vw_engagement_segments` AS

SELECT
  device_category,
  source,
  medium,

  CASE
    WHEN purchased = 1 THEN 'Purchased'
    WHEN purchased = 0
         AND (viewed_item = 1 OR added_to_cart = 1 OR began_checkout = 1)
         THEN 'Engaged_No_Purchase'
    ELSE 'Low_Engagement'
  END AS engagement_segment,

  COUNT(DISTINCT session_id) AS sessions,
  SUM(generated_lead) AS leads_generated

FROM `hs770report.analytics_387639477.vw_sessions_summary`
GROUP BY
  device_category,
  source,
  medium,
  engagement_segment;


--sanity check
SELECT
  engagement_segment,
  SUM(sessions) AS total_sessions
FROM `hs770report.analytics_387639477.vw_engagement_segments`
GROUP BY engagement_segment;


--view 4 analytics
--Conversions by sessions
WITH base_events AS (
  SELECT
    user_pseudo_id,
    CAST(
      (SELECT value.int_value
       FROM UNNEST(event_params)
       WHERE key = 'ga_session_id'
      ) AS STRING
    ) AS session_id,
    event_name,
    event_timestamp -- Keep event_timestamp for ordering events within a session later if needed
  FROM `hs770report.analytics_387639477.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE -- Add date filter if using in Looker Studio
),

session_flags AS (
  SELECT
    user_pseudo_id,
    session_id,
    MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS viewed_item,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS began_checkout,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchased
  FROM base_events
  WHERE session_id IS NOT NULL
  GROUP BY
    user_pseudo_id,
    session_id
),

session_segments AS (
  SELECT
    session_id,
    CASE
      WHEN purchased = 1 THEN 'Purchased'
      WHEN purchased = 0
           AND (viewed_item = 1 OR added_to_cart = 1 OR began_checkout = 1)
           THEN 'Engaged_No_Purchase'
      ELSE 'Other'
    END AS segment
  FROM session_flags -- Now references the CTE above, not a view
),

events_in_sessions AS (
  SELECT DISTINCT
    CAST(
      (SELECT value.int_value
       FROM UNNEST(event_params)
       WHERE key = 'ga_session_id'
      ) AS STRING
    ) AS session_id,
    event_name
  FROM `hs770report.analytics_387639477.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE -- Apply date filter here as well
)

SELECT
  s.segment,
  e.event_name,
  COUNT(DISTINCT e.session_id) AS sessions_with_event
FROM events_in_sessions e
JOIN session_segments s
  ON e.session_id = s.session_id
WHERE s.segment IN ('Purchased', 'Engaged_No_Purchase')
GROUP BY
  s.segment,
  e.event_name
ORDER BY s.segment, sessions_with_event DESC;

-- waterfall
WITH base_events AS (
  SELECT
    user_pseudo_id,
    CAST(
      (SELECT value.int_value
       FROM UNNEST(event_params)
       WHERE key = 'ga_session_id'
      ) AS STRING
    ) AS session_id,
    device.category AS device_category,
    event_name
  FROM `hs770report.analytics_387639477.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
),

session_activity AS (
  SELECT
    session_id,
    device_category,
    MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS viewed_item,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS began_checkout,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchased
  FROM base_events
  WHERE session_id IS NOT NULL
  GROUP BY
    session_id,
    device_category
),

funnel_counts AS (
  SELECT
    device_category,
    COUNT(DISTINCT session_id) AS total_sessions_in_scope, -- Total sessions for this device
    SUM(viewed_item) AS viewed_item_sessions,
    SUM(added_to_cart) AS add_to_cart_sessions,
    SUM(began_checkout) AS begin_checkout_sessions,
    SUM(purchased) AS purchase_sessions
  FROM session_activity
  GROUP BY device_category
),

funnel_steps AS (
  SELECT
    device_category,
    '1_View_Item' AS step,
    -- Sessions that started the funnel (e.g., viewed an item)
    viewed_item_sessions AS sessions_at_step,
    -- Sessions that completed this step
    viewed_item_sessions AS sessions_completed_step
  FROM funnel_counts
  WHERE viewed_item_sessions > 0

  UNION ALL

  SELECT
    device_category,
    '2_Add_to_Cart',
    viewed_item_sessions, -- Sessions that were available for this step
    add_to_cart_sessions -- Sessions that completed this step
  FROM funnel_counts
  WHERE viewed_item_sessions > 0 -- Only consider sessions that entered the prior step

  UNION ALL

  SELECT
    device_category,
    '3_Begin_Checkout',
    add_to_cart_sessions, -- Sessions that were available for this step
    begin_checkout_sessions -- Sessions that completed this step
  FROM funnel_counts
  WHERE add_to_cart_sessions > 0 -- Only consider sessions that entered the prior step

  UNION ALL

  SELECT
    device_category,
    '4_Purchase',
    begin_checkout_sessions, -- Sessions that were available for this step
    purchase_sessions -- Sessions that completed this step
  FROM funnel_counts
  WHERE begin_checkout_sessions > 0 -- Only consider sessions that entered the prior step
)

SELECT
  device_category,
  step,
  sessions_at_step,
  sessions_completed_step,
  -- Calculate step-to-step conversion rate
  SAFE_DIVIDE(sessions_completed_step, sessions_at_step) AS step_conversion_rate,
  -- Calculate step-to-step drop-off rate
  1 - SAFE_DIVIDE(sessions_completed_step, sessions_at_step) AS step_dropoff_rate,
  -- Calculate cumulative conversion rate from the start of the funnel (view_item)
  SAFE_DIVIDE(sessions_completed_step, MAX(CASE WHEN step = '1_View_Item' THEN sessions_at_step ELSE 0 END) OVER (PARTITION BY device_category)) AS cumulative_conversion_rate
FROM funnel_steps
ORDER BY
  device_category,
  step;



-- ANALYTICAL DATA --
-- TOP 10 VIEWED PRODUCT --
SELECT item.item_name, COUNT(*) AS view_count
FROM `hs770report.analytics_387639477.events_*`, UNNEST(items) AS item
WHERE event_name = 'view_item'
GROUP BY item.item_name
ORDER BY view_count DESC
LIMIT 10;

SELECT item.item_name, COUNT(*) AS add_to_cart_count
FROM `hs770report.analytics_387639477.events_*`, UNNEST(items) AS item
WHERE event_name = 'add_to_cart'
GROUP BY item.item_name
ORDER BY add_to_cart_count DESC
LIMIT 10;

-- no, of people that add to cart but dont buy
WITH
  AddToCartItems AS (
    SELECT
      user_pseudo_id,
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = 'ga_session_id'
      ) AS ga_session_id,
      item.item_id,
      item.item_name
    FROM `hs770report.analytics_387639477.events_*`, UNNEST(items) AS item
    WHERE event_name = 'add_to_cart'
  ),
  PurchaseItems AS (
    SELECT
      user_pseudo_id,
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = 'ga_session_id'
      ) AS ga_session_id,
      item.item_id
    FROM `hs770report.analytics_387639477.events_*`, UNNEST(items) AS item
    WHERE event_name = 'purchase'
  ),
  AddedButNotPurchased AS (
    SELECT a.item_name, a.item_id
    FROM AddToCartItems AS a
    LEFT JOIN PurchaseItems AS p
      ON
        a.user_pseudo_id = p.user_pseudo_id
        AND a.ga_session_id = p.ga_session_id
        AND a.item_id = p.item_id
    WHERE p.item_id IS NULL
  )
SELECT item_name, COUNT(item_id) AS added_to_cart_no_purchase_count
FROM AddedButNotPurchased
GROUP BY item_name
ORDER BY added_to_cart_no_purchase_count DESC
LIMIT 10;


-- average order value
SELECT
  SUM(events.ecommerce.purchase_revenue_in_usd)
  / COUNT(DISTINCT events.ecommerce.transaction_id) AS average_order_value
FROM `hs770report.analytics_387639477.events_*` AS events
WHERE
  events.event_name = 'purchase'
  AND events.ecommerce.purchase_revenue_in_usd IS NOT NULL
  AND events.ecommerce.transaction_id IS NOT NULL;
  --AND  _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE;

-- 

-- conversion rate
WITH
  TrafficStats AS (
    SELECT
      COUNT(DISTINCT user_pseudo_id) AS total_visitors,
      COUNT(
        DISTINCT
          CASE
            WHEN event_name = 'purchase'
              THEN
                (
                  SELECT value.string_value
                  FROM UNNEST(event_params)
                  WHERE key = 'transaction_id'
                )
            ELSE NULL
            END)
        AS total_purchases
    FROM `hs770report.analytics_387639477.events_*`
     --WHERE _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
  )
SELECT
  total_visitors,
  total_purchases,
  SAFE_DIVIDE(total_purchases, total_visitors) * 100
    AS conversion_rate_percentage
FROM TrafficStats;
--usage
SELECT
  -- Sum up all bytes and convert to Gigabytes
  ROUND(SUM(total_bytes_processed) / pow(10,9), 2) AS total_gb_used_this_month,
  -- Calculate percentage of your 1TB (1000GB) limit
  ROUND((SUM(total_bytes_processed) / pow(10,12)) * 100, 2) AS percentage_of_limit
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  -- Only look at the current month
  EXTRACT(MONTH FROM creation_time) = EXTRACT(MONTH FROM CURRENT_TIMESTAMP())
  AND EXTRACT(YEAR FROM creation_time) = EXTRACT(YEAR FROM CURRENT_TIMESTAMP())
  AND job_type = 'QUERY';
-- sales by source
SELECT
  COALESCE(t.traffic_source.medium, 'Unknown') AS marketing_medium,
  COALESCE(t.traffic_source.source, 'Unknown') AS marketing_source,
  SUM(
    CASE
      WHEN ep.key = 'value'
        THEN COALESCE(ep.value.double_value, ep.value.int_value)
      ELSE 0
      END)
    AS total_sales_value
FROM
  `hs770report.analytics_387639477.events_*` AS t, UNNEST(t.event_params) AS ep
WHERE t.event_name IN ('purchase', 'ecommerce_purchase') 
--and _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
GROUP BY marketing_medium, marketing_source
ORDER BY total_sales_value DESC;

--user Journeys/Paths to Purchase
SELECT
  event_sequence,
  COUNT(DISTINCT user_pseudo_id || '_' || session_id) AS unique_user_sessions,
  COUNT(1) AS sequence_count
FROM
  (
    SELECT
      user_pseudo_id,
      session_id,
      STRING_AGG(event_name, ' -> ' ORDER BY event_timestamp) AS event_sequence
    FROM
      (
        SELECT
          user_pseudo_id,
          (
            SELECT value.int_value
            FROM UNNEST(event_params)
            WHERE key = 'ga_session_id'
          ) AS session_id,
          event_name,
          event_timestamp
        FROM `hs770report.analytics_387639477.events_*`
        WHERE
          _TABLE_SUFFIX
            BETWEEN FORMAT_DATE(
              '%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
          AND event_name IN (
            'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
      )
    WHERE session_id IS NOT NULL
    GROUP BY user_pseudo_id, session_id
    HAVING
      STRING_AGG(event_name, ' -> ' ORDER BY event_timestamp) LIKE '%purchase'
  )
GROUP BY event_sequence
ORDER BY sequence_count DESC
LIMIT 100;

-- bounce rate: Percentage of visitors who leave your site after viewing only one page

SELECT
  SAFE_DIVIDE(
    COUNT(
      CASE
        WHEN session_event_counts.event_count = 1
          THEN session_event_counts.session_id
        END),
    COUNT(session_event_counts.session_id))
  * 100 AS bounce_rate
FROM
  (
    SELECT
      CONCAT(
        t1.user_pseudo_id,
        '.',
        (
          SELECT value.int_value
          FROM UNNEST(t1.event_params)
          WHERE key = 'ga_session_id'
        ))
        AS session_id,
      COUNT(t1.event_name) AS event_count
    FROM `hs770report.analytics_387639477.events_*` AS t1
    WHERE
      -- Filter for valid ga_session_id to ensure a complete session identifier
      EXISTS(
        SELECT 1
        FROM UNNEST(t1.event_params)
        WHERE key = 'ga_session_id' AND value.int_value IS NOT NULL
      )
    GROUP BY session_id
  ) AS session_event_counts;

-- session time period: for how long did the avg user stay
SELECT AVG(session_duration_seconds) AS average_session_duration_seconds
FROM
  (
    SELECT
      user_pseudo_id,
      (
        SELECT ep.value.int_value
        FROM UNNEST(event_params) AS ep
        WHERE ep.key = 'ga_session_id'
      ) AS ga_session_id,
      (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000
        AS session_duration_seconds
    FROM `hs770report.analytics_387639477.events_*`
    WHERE
      _TABLE_SUFFIX
        BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
        AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
      AND (
        SELECT ep.value.int_value
        FROM UNNEST(event_params) AS ep
        WHERE ep.key = 'ga_session_id'
      ) IS NOT NULL
    GROUP BY user_pseudo_id, ga_session_id
  ) AS session_durations;


-- product added-to=cart but not purchaseed with their views
WITH
  AddToCartItems AS (
    SELECT
      user_pseudo_id,
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = 'ga_session_id'
      ) AS ga_session_id,
      item.item_id,
      item.item_name
    FROM `hs770report.analytics_387639477.events_*`, UNNEST(items) AS item
    WHERE event_name = 'add_to_cart'
  ),
  PurchaseItems AS (
    SELECT
      user_pseudo_id,
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = 'ga_session_id'
      ) AS ga_session_id,
      item.item_id
    FROM `hs770report.analytics_387639477.events_*`, UNNEST(items) AS item
    WHERE event_name = 'purchase'
  ),
  AddedButNotPurchased AS (
    SELECT a.item_name, a.item_id
    FROM AddToCartItems AS a
    LEFT JOIN PurchaseItems AS p
      ON
        a.user_pseudo_id = p.user_pseudo_id
        AND a.ga_session_id = p.ga_session_id
        AND a.item_id = p.item_id
    WHERE p.item_id IS NULL
  ),
  ViewedItems AS (
    SELECT item.item_name, COUNT(1) AS total_view_count
    FROM `hs770report.analytics_387639477.events_*`, UNNEST(items) AS item
    WHERE event_name = 'view_item'
    GROUP BY item.item_name
  )
SELECT
  abnp.item_name,
  COUNT(abnp.item_id) AS added_to_cart_no_purchase_count,
  COALESCE(vi.total_view_count, 0) AS total_view_count
FROM AddedButNotPurchased AS abnp
LEFT JOIN ViewedItems AS vi
  ON abnp.item_name = vi.item_name
GROUP BY abnp.item_name, vi.total_view_count
ORDER BY added_to_cart_no_purchase_count DESC
LIMIT 10;

-- Co purchase Analysis
SELECT
  device_category,
  step,
  sessions_at_step,
  sessions_completed_step,
  -- GA4 shows conversion, but BigQuery allows custom calculations for step drop-off.
  ROUND(step_conversion_rate * 100, 2) AS conversion_rate_percent,
  ROUND(step_dropoff_rate * 100, 2) AS dropoff_rate_percent
FROM
  `hs770report.analytics_387639477.vw_funnel_diagnostics`
ORDER BY
  device_category,
  CASE step
    WHEN 'view_item' THEN 1
    WHEN 'add_to_cart' THEN 2
    WHEN 'begin_checkout' THEN 3
    WHEN 'purchase' THEN 4
  END;

-- search keywords
SELECT
  (
    SELECT value.string_value
    FROM UNNEST(event_params)
    WHERE key = 'search_term'
  ) AS search_keyword,
  COUNT(1) AS search_count
FROM `hs770report.analytics_387639477.events_*`
WHERE
  event_name = 'view_search_results'
  AND (
    SELECT value.string_value
    FROM UNNEST(event_params)
    WHERE key = 'search_term'
  ) IS NOT NULL
  --AND _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
GROUP BY search_keyword
ORDER BY search_count DESC
LIMIT 10;

-- visitors by state
SELECT
  events.geo.region AS state,
  COUNT(DISTINCT events.user_pseudo_id) AS total_visitors
FROM `hs770report.analytics_387639477.events_*` AS events
WHERE
  events.geo.country = 'United States'
  --AND _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
GROUP BY state
ORDER BY total_visitors DESC
LIMIT 10;





-- search keywords
SELECT
  event_name,
  (
    SELECT value.string_value
    FROM UNNEST(event_params)
    WHERE key = 'search_term'
  ) AS search_term_param,
  event_params
FROM
  `hs770report.analytics_387639477.events_20260125`  -- Replace with a recent date table, e.g., events_YYYYMMDD
WHERE event_name LIKE '%search%'  -- Look for events that might contain 'search'
LIMIT 100;


