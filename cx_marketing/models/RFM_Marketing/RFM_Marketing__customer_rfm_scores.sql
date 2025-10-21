{{
  config({    
    "materialized": "ephemeral",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH crm_customers AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'crm_customers') }}

),

ecomm_orders AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'ecomm_orders') }}

),

instore_sales AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'instore_sales') }}

),

customer_data_join AS (

  SELECT 
    ecomm_orders.order_id AS ORDER_ID,
    ecomm_orders.customer_id AS CUSTOMER_ID,
    ecomm_orders.order_date AS ORDER_DATE,
    ecomm_orders.order_amount AS ORDER_AMOUNT,
    instore_sales.transaction_id AS TRANSACTION_ID,
    instore_sales.transaction_date AS TRANSACTION_DATE,
    instore_sales.transaction_amount AS TRANSACTION_AMOUNT,
    crm_customers.signup_date AS SIGNUP_DATE,
    crm_customers.email AS EMAIL,
    crm_customers.zip_code AS ZIP_CODE,
    crm_customers.region AS REGION,
    crm_customers.preferred_channel AS PREFERRED_CHANNEL
  
  FROM ecomm_orders
  LEFT JOIN instore_sales
     ON ecomm_orders.customer_id = instore_sales.customer_id
  LEFT JOIN crm_customers
     ON ecomm_orders.customer_id = crm_customers.customer_id

),

customer_aggregate_data AS (

  SELECT 
    CUSTOMER_ID,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    MAX(ORDER_DATE) AS MOST_RECENT_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_data_join
  
  GROUP BY 
    CUSTOMER_ID, EMAIL, ZIP_CODE, REGION, PREFERRED_CHANNEL

),

customer_rfm_analysis AS (

  SELECT 
    CUSTOMER_ID,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    MOST_RECENT_ORDER_DATE,
    FREQUENCY,
    MONETARY,
    DATEDIFF(DAY, MOST_RECENT_ORDER_DATE, CURRENT_DATE) AS RECENCY
  
  FROM customer_aggregate_data

),

customer_rfm_scores AS (

  SELECT 
    CUSTOMER_ID,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    MOST_RECENT_ORDER_DATE,
    FREQUENCY,
    MONETARY,
    RECENCY,
    CASE
      WHEN MONETARY >= 1000
        THEN 5
      WHEN MONETARY >= 500
        THEN 4
      WHEN MONETARY >= 250
        THEN 3
      WHEN MONETARY >= 100
        THEN 2
      ELSE 1
    END AS MONETARY_SCORE,
    CASE
      WHEN FREQUENCY >= 50
        THEN 5
      WHEN FREQUENCY >= 20
        THEN 4
      WHEN FREQUENCY >= 10
        THEN 3
      WHEN FREQUENCY >= 5
        THEN 2
      ELSE 1
    END AS FREQUENCY_SCORE,
    CASE
      WHEN RECENCY <= 7
        THEN 5
      WHEN RECENCY <= 14
        THEN 4
      WHEN RECENCY <= 30
        THEN 3
      WHEN RECENCY <= 60
        THEN 2
      ELSE 1
    END AS RECENCY_SCORE
  
  FROM customer_rfm_analysis

)

SELECT *

FROM customer_rfm_scores
