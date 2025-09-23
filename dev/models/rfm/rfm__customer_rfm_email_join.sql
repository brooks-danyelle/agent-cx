{{
  config({    
    "materialized": "table",
    "alias": "CustomerSegmentationDemo_rfm_mpfml",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH crm_customers AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'crm_customers') }}

),

ecom_orders AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

),

instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

customer_data_join AS (

  SELECT 
    ecom_orders.order_id AS ORDER_ID,
    ecom_orders.customer_id AS CUSTOMER_ID,
    ecom_orders.order_date AS ORDER_DATE,
    ecom_orders.order_amount AS ORDER_AMOUNT,
    instore_sales.transaction_id AS TRANSACTION_ID,
    instore_sales.transaction_date AS TRANSACTION_DATE,
    instore_sales.transaction_amount AS TRANSACTION_AMOUNT,
    crm_customers.signup_date AS SIGNUP_DATE,
    crm_customers.email AS EMAIL,
    crm_customers.zip_code AS ZIP_CODE,
    crm_customers.region AS REGION,
    crm_customers.preferred_channel AS PREFERRED_CHANNEL
  
  FROM ecom_orders
  LEFT JOIN instore_sales
     ON ecom_orders.customer_id = instore_sales.customer_id
  LEFT JOIN crm_customers
     ON ecom_orders.customer_id = crm_customers.customer_id

),

rfm_aggregation AS (

  SELECT 
    CUSTOMER_ID,
    MAX(ORDER_DATE) AS MOST_RECENT_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_data_join
  
  GROUP BY CUSTOMER_ID

),

email_events AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'email_events') }}

),

mask_email_id AS (

  SELECT 
    email_id AS EMAIL_ID,
    customer_id AS CUSTOMER_ID,
    event_type AS EVENT_TYPE,
    event_date AS EVENT_DATE,
    CONCAT(SUBSTRING(email_id, 1, 3), '****@****', SUBSTRING_INDEX(email_id, '@', -1)) AS MASKED_EMAIL_ID
  
  FROM email_events

),

rfm_calculation AS (

  SELECT 
    CUSTOMER_ID,
    MOST_RECENT_ORDER_DATE,
    DATEDIFF(DAY, MOST_RECENT_ORDER_DATE, CURRENT_DATE) AS RECENCY,
    FREQUENCY,
    MONETARY
  
  FROM rfm_aggregation

),

rfm_scores_calculation AS (

  SELECT 
    CUSTOMER_ID,
    MOST_RECENT_ORDER_DATE,
    RECENCY,
    FREQUENCY,
    MONETARY,
    NTILE(5) OVER (ORDER BY MONETARY DESC NULLS FIRST) AS MONETARY_SCORE,
    NTILE(5) OVER (ORDER BY FREQUENCY DESC NULLS FIRST) AS FREQUENCY_SCORE,
    NTILE(5) OVER (ORDER BY RECENCY ASC NULLS LAST) AS RECENCY_SCORE
  
  FROM rfm_calculation

),

rfm_with_segment AS (

  SELECT 
    CUSTOMER_ID,
    MOST_RECENT_ORDER_DATE,
    RECENCY,
    FREQUENCY,
    MONETARY,
    MONETARY_SCORE,
    FREQUENCY_SCORE,
    RECENCY_SCORE,
    CONCAT(RECENCY_SCORE, FREQUENCY_SCORE, MONETARY_SCORE) AS RFM_SEGMENT,
    'SEGMENT_FLAG' AS CUSTOMER_FLAG
  
  FROM rfm_scores_calculation

),

customer_rfm_email_join AS (

  SELECT 
    rfm_with_segment.CUSTOMER_ID,
    rfm_with_segment.MOST_RECENT_ORDER_DATE,
    rfm_with_segment.RECENCY,
    rfm_with_segment.FREQUENCY,
    rfm_with_segment.MONETARY,
    rfm_with_segment.MONETARY_SCORE,
    rfm_with_segment.FREQUENCY_SCORE,
    rfm_with_segment.RECENCY_SCORE,
    rfm_with_segment.RFM_SEGMENT,
    rfm_with_segment.CUSTOMER_FLAG,
    mask_email_id.EMAIL_ID,
    mask_email_id.EVENT_TYPE,
    mask_email_id.EVENT_DATE,
    mask_email_id.MASKED_EMAIL_ID
  
  FROM rfm_with_segment
  INNER JOIN mask_email_id
     ON rfm_with_segment.CUSTOMER_ID = mask_email_id.CUSTOMER_ID

)

SELECT *

FROM customer_rfm_email_join
