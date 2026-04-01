CREATE DATABASE marketing_analytics;
USE marketing_analytics;
CREATE TABLE user_funnel_data (
  user_id INT,
  source VARCHAR(50),
  campaign VARCHAR(50),
  stage VARCHAR(50),
  date DATE
);
SELECT * FROM user_funnel_data LIMIT 10;
SELECT COUNT(DISTINCT user_id) AS total_users
FROM user_funnel_data;
SELECT 
  stage,
  COUNT(DISTINCT user_id) AS users
FROM user_funnel_data
GROUP BY stage;
WITH funnel AS (
  SELECT 
    stage,
    COUNT(DISTINCT user_id) AS users
  FROM user_funnel_data
  GROUP BY stage
)

SELECT 
  MAX(CASE WHEN stage='Visitor' THEN users END) AS visitors,
  MAX(CASE WHEN stage='Signup' THEN users END) AS signups,
  MAX(CASE WHEN stage='Purchase' THEN users END) AS purchases
FROM funnel;
WITH funnel AS (
  SELECT 
    stage,
    COUNT(DISTINCT user_id) AS users
  FROM user_funnel_data
  GROUP BY stage
)

SELECT 
  ROUND(
    MAX(CASE WHEN stage='Signup' THEN users END) /
    MAX(CASE WHEN stage='Visitor' THEN users END) * 100, 2
  ) AS signup_conversion_rate,

  ROUND(
    MAX(CASE WHEN stage='Purchase' THEN users END) /
    MAX(CASE WHEN stage='Signup' THEN users END) * 100, 2
  ) AS purchase_conversion_rate
FROM funnel;

WITH funnel AS (
  SELECT 
    stage,
    COUNT(DISTINCT user_id) AS users
  FROM user_funnel_data
  GROUP BY stage
)

SELECT 
  ROUND(
    (1 - MAX(CASE WHEN stage='Signup' THEN users END) /
         MAX(CASE WHEN stage='Visitor' THEN users END)) * 100, 2
  ) AS visitor_to_signup_drop,

  ROUND(
    (1 - MAX(CASE WHEN stage='Purchase' THEN users END) /
         MAX(CASE WHEN stage='Signup' THEN users END)) * 100, 2
  ) AS signup_to_purchase_drop
FROM funnel;

SELECT 
  source,
  stage,
  COUNT(DISTINCT user_id) AS users
FROM user_funnel_data
GROUP BY source, stage
ORDER BY source;

WITH channel_data AS (
  SELECT 
    source,
    stage,
    COUNT(DISTINCT user_id) AS users
  FROM user_funnel_data
  GROUP BY source, stage
)

SELECT 
  source,
  MAX(CASE WHEN stage='Visitor' THEN users END) AS visitors,
  MAX(CASE WHEN stage='Signup' THEN users END) AS signups,
  MAX(CASE WHEN stage='Purchase' THEN users END) AS purchases
FROM channel_data
GROUP BY source;
SELECT 
  date,
  COUNT(DISTINCT user_id) AS users
FROM user_funnel_data
GROUP BY date
ORDER BY date;

SELECT 
  date,
  COUNT(DISTINCT CASE WHEN stage='Signup' THEN user_id END) AS signups,
  COUNT(DISTINCT CASE WHEN stage='Purchase' THEN user_id END) AS purchases
FROM user_funnel_data
GROUP BY date
ORDER BY date;
WITH stage_counts AS (
  SELECT 
    stage,
    COUNT(DISTINCT user_id) AS users
  FROM user_funnel_data
  GROUP BY stage
)

SELECT 
  stage,
  users,
  LAG(users) OVER (ORDER BY 
    CASE 
      WHEN stage='Visitor' THEN 1
      WHEN stage='Signup' THEN 2
      WHEN stage='Purchase' THEN 3
    END
  ) AS previous_stage_users,
  
  ROUND(
    users * 100.0 / LAG(users) OVER (ORDER BY 
      CASE 
        WHEN stage='Visitor' THEN 1
        WHEN stage='Signup' THEN 2
        WHEN stage='Purchase' THEN 3
      END
    ), 2
  ) AS step_conversion_rate
FROM stage_counts;

WITH user_journey AS (
  SELECT 
    user_id,
    MIN(CASE WHEN stage='Visitor' THEN date END) AS visit_date,
    MIN(CASE WHEN stage='Signup' THEN date END) AS signup_date,
    MIN(CASE WHEN stage='Purchase' THEN date END) AS purchase_date
  FROM user_funnel_data
  GROUP BY user_id
)

SELECT 
  AVG(DATEDIFF(purchase_date, visit_date)) AS avg_days_to_purchase
FROM user_journey
WHERE purchase_date IS NOT NULL;

WITH cohort AS (
  SELECT 
    user_id,
    MIN(date) AS first_visit_date
  FROM user_funnel_data
  WHERE stage='Visitor'
  GROUP BY user_id
)

SELECT 
  DATE_FORMAT(first_visit_date, '%Y-%m') AS cohort_month,
  COUNT(DISTINCT user_id) AS users
FROM cohort
GROUP BY cohort_month
ORDER BY cohort_month;

WITH first_visit AS (
  SELECT 
    user_id,
    MIN(date) AS first_date
  FROM user_funnel_data
  GROUP BY user_id
),

return_users AS (
  SELECT 
    f.user_id,
    COUNT(DISTINCT u.date) AS active_days
  FROM first_visit f
  JOIN user_funnel_data u 
    ON f.user_id = u.user_id
  GROUP BY f.user_id
)

SELECT 
  COUNT(*) AS total_users,
  SUM(CASE WHEN active_days > 1 THEN 1 ELSE 0 END) AS returning_users,
  ROUND(
    SUM(CASE WHEN active_days > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
  ) AS retention_rate
FROM return_users;
WITH campaign_data AS (
  SELECT 
    campaign,
    stage,
    COUNT(DISTINCT user_id) AS users
  FROM user_funnel_data
  GROUP BY campaign, stage
)

SELECT 
  campaign,
  MAX(CASE WHEN stage='Visitor' THEN users END) AS visitors,
  MAX(CASE WHEN stage='Purchase' THEN users END) AS purchases,
  ROUND(
    MAX(CASE WHEN stage='Purchase' THEN users END) * 100.0 /
    MAX(CASE WHEN stage='Visitor' THEN users END), 2
  ) AS conversion_rate
FROM campaign_data
GROUP BY campaign
ORDER BY conversion_rate DESC;

SELECT DISTINCT user_id
FROM user_funnel_data
WHERE user_id NOT IN (
  SELECT DISTINCT user_id
  FROM user_funnel_data
  WHERE stage='Purchase'
);
WITH daily_data AS (
  SELECT 
    date,
    COUNT(DISTINCT CASE WHEN stage='Visitor' THEN user_id END) AS visitors,
    COUNT(DISTINCT CASE WHEN stage='Purchase' THEN user_id END) AS purchases
  FROM user_funnel_data
  GROUP BY date
)

SELECT 
  date,
  visitors,
  purchases,
  ROUND(purchases * 100.0 / visitors, 2) AS daily_conversion_rate
FROM daily_data
ORDER BY date;

SELECT 
  source,
  stage,
  COUNT(DISTINCT user_id) AS users,
  RANK() OVER (PARTITION BY stage ORDER BY COUNT(DISTINCT user_id) DESC) AS rank_position
FROM user_funnel_data
GROUP BY source, stage;
WITH funnel AS (
  SELECT 
    COUNT(DISTINCT CASE WHEN stage='Visitor' THEN user_id END) AS visitors,
    COUNT(DISTINCT CASE WHEN stage='Signup' THEN user_id END) AS signups,
    COUNT(DISTINCT CASE WHEN stage='Purchase' THEN user_id END) AS purchases
  FROM user_funnel_data
)

SELECT 
  visitors,
  signups,
  purchases,
  ROUND(signups * 100.0 / visitors, 2) AS signup_rate,
  ROUND(purchases * 100.0 / signups, 2) AS purchase_rate
FROM funnel;

SELECT 
  source,
  COUNT(DISTINCT CASE WHEN stage='Purchase' THEN user_id END) AS purchases,
  RANK() OVER (ORDER BY COUNT(DISTINCT CASE WHEN stage='Purchase' THEN user_id END) DESC) AS rank_position
FROM user_funnel_data
GROUP BY source;
SELECT 
  source,
  COUNT(DISTINCT user_id) AS total_users,
  COUNT(DISTINCT CASE WHEN stage='Purchase' THEN user_id END) AS buyers
FROM user_funnel_data
GROUP BY source;