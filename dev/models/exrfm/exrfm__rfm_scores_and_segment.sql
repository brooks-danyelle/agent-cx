{{
  config({    
    "materialized": "ephemeral",
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

customer_order_join AS (

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

customer_order_aggregation AS (

  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_AMOUNT,
    TRANSACTION_ID,
    TRANSACTION_DATE,
    TRANSACTION_AMOUNT,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    MAX(ORDER_DATE) AS MAX_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_order_join
  
  GROUP BY 
    ORDER_ID, 
    CUSTOMER_ID, 
    ORDER_DATE, 
    ORDER_AMOUNT, 
    TRANSACTION_ID, 
    TRANSACTION_DATE, 
    TRANSACTION_AMOUNT, 
    SIGNUP_DATE, 
    EMAIL, 
    ZIP_CODE, 
    REGION, 
    PREFERRED_CHANNEL

),

customer_rfm_analysis AS (

  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_AMOUNT,
    TRANSACTION_ID,
    TRANSACTION_DATE,
    TRANSACTION_AMOUNT,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) AS RECENCY,
    FREQUENCY,
    MONETARY
  
  FROM customer_order_aggregation

),

rfm_scores_and_segment AS (

  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_AMOUNT,
    TRANSACTION_ID,
    TRANSACTION_DATE,
    TRANSACTION_AMOUNT,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    RECENCY,
    FREQUENCY,
    MONETARY,
    CASE
      WHEN RECENCY <= 30
        THEN 5
      WHEN RECENCY <= 60
        THEN 4
      WHEN RECENCY <= 90
        THEN 3
      WHEN RECENCY <= 120
        THEN 2
      ELSE 1
    END AS RECENCY_SCORE,
    CASE
      WHEN FREQUENCY >= 10
        THEN 5
      WHEN FREQUENCY >= 7
        THEN 4
      WHEN FREQUENCY >= 5
        THEN 3
      WHEN FREQUENCY >= 3
        THEN 2
      ELSE 1
    END AS FREQUENCY_SCORE,
    CASE
      WHEN MONETARY >= 1000
        THEN 5
      WHEN MONETARY >= 750
        THEN 4
      WHEN MONETARY >= 500
        THEN 3
      WHEN MONETARY >= 250
        THEN 2
      ELSE 1
    END AS MONETARY_SCORE,
    CONCAT(
      CAST(CASE
        WHEN RECENCY <= 30
          THEN 5
        WHEN RECENCY <= 60
          THEN 4
        WHEN RECENCY <= 90
          THEN 3
        WHEN RECENCY <= 120
          THEN 2
        ELSE 1
      END AS STRING), 
      CAST(CASE
        WHEN FREQUENCY >= 10
          THEN 5
        WHEN FREQUENCY >= 7
          THEN 4
        WHEN FREQUENCY >= 5
          THEN 3
        WHEN FREQUENCY >= 3
          THEN 2
        ELSE 1
      END AS STRING), 
      CAST(CASE
        WHEN MONETARY >= 1000
          THEN 5
        WHEN MONETARY >= 750
          THEN 4
        WHEN MONETARY >= 500
          THEN 3
        WHEN MONETARY >= 250
          THEN 2
        ELSE 1
      END AS STRING)) AS RFM_SEGMENT
  
  FROM customer_rfm_analysis

)

SELECT *

FROM rfm_scores_and_segment
