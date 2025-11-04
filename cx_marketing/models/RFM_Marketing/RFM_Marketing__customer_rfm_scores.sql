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

instore_sales AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'instore_sales') }}

),

ecomm_orders AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'ecomm_orders') }}

),

customer_data_joined AS (

  SELECT 
    crm_customers.customer_id AS CUSTOMER_ID,
    crm_customers.signup_date AS SIGNUP_DATE,
    crm_customers.email AS EMAIL,
    crm_customers.zip_code AS ZIP_CODE,
    crm_customers.region AS REGION,
    crm_customers.preferred_channel AS PREFERRED_CHANNEL,
    instore_sales.transaction_id AS INSTORE_TRANSACTION_ID,
    instore_sales.transaction_date AS INSTORE_TRANSACTION_DATE,
    instore_sales.transaction_amount AS INSTORE_TRANSACTION_AMOUNT,
    ecomm_orders.order_id AS ECOMM_ORDER_ID,
    ecomm_orders.order_date AS ECOMM_ORDER_DATE,
    ecomm_orders.order_amount AS ECOMM_ORDER_AMOUNT
  
  FROM crm_customers
  LEFT JOIN instore_sales
     ON crm_customers.customer_id = instore_sales.customer_id
  LEFT JOIN ecomm_orders
     ON crm_customers.customer_id = ecomm_orders.customer_id

),

customer_rfm_analysis AS (

  SELECT 
    CUSTOMER_ID,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    INSTORE_TRANSACTION_ID,
    INSTORE_TRANSACTION_DATE,
    INSTORE_TRANSACTION_AMOUNT,
    ECOMM_ORDER_ID,
    ECOMM_ORDER_DATE,
    ECOMM_ORDER_AMOUNT,
    DATEDIFF(
      DAY, 
      GREATEST(COALESCE(INSTORE_TRANSACTION_DATE, DATE'1900-01-01'), COALESCE(ECOMM_ORDER_DATE, DATE'1900-01-01')), 
      CURRENT_DATE) AS RECENCY,
    CASE
      WHEN INSTORE_TRANSACTION_ID IS NOT NULL
        THEN 1
      ELSE 0
    END
    + CASE
        WHEN ECOMM_ORDER_ID IS NOT NULL
          THEN 1
        ELSE 0
      END AS FREQUENCY,
    COALESCE(INSTORE_TRANSACTION_AMOUNT, 0) + COALESCE(ECOMM_ORDER_AMOUNT, 0) AS MONETARY
  
  FROM customer_data_joined

),

customer_rfm_scores AS (

  SELECT 
    CUSTOMER_ID,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    INSTORE_TRANSACTION_ID,
    INSTORE_TRANSACTION_DATE,
    INSTORE_TRANSACTION_AMOUNT,
    ECOMM_ORDER_ID,
    ECOMM_ORDER_DATE,
    ECOMM_ORDER_AMOUNT,
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
      WHEN FREQUENCY >= 4
        THEN 3
      WHEN FREQUENCY >= 2
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
    END AS MONETARY_SCORE
  
  FROM customer_rfm_analysis

)

SELECT *

FROM customer_rfm_scores
