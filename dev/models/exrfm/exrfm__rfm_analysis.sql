{{
  config({    
    "materialized": "ephemeral",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

crm_customers AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'crm_customers') }}

),

ecom_orders AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

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

customer_rfm_aggregation AS (

  SELECT 
    CUSTOMER_ID,
    MAX(ORDER_DATE) AS LAST_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_order_join
  
  GROUP BY CUSTOMER_ID

),

customer_rfm_details AS (

  SELECT 
    CUSTOMER_ID,
    LAST_ORDER_DATE,
    FREQUENCY,
    MONETARY,
    DATEDIFF(DAY, LAST_ORDER_DATE, CURRENT_DATE) AS RECENCY
  
  FROM customer_rfm_aggregation

),

rfm_analysis AS (

  SELECT 
    CUSTOMER_ID,
    LAST_ORDER_DATE,
    FREQUENCY,
    MONETARY,
    RECENCY,
    FREQUENCY + MONETARY + RECENCY AS RFM_SCORE,
    CASE
      WHEN RECENCY <= 30 AND FREQUENCY >= 10 AND MONETARY >= 1000
        THEN 'Champions      '
      WHEN RECENCY <= 30 AND FREQUENCY >= 5
        THEN 'Loyal Customers'
      WHEN RECENCY > 30 AND FREQUENCY < 5
        THEN 'At Risk        '
      ELSE 'Others         '
    END AS RFM_SEGMENT
  
  FROM customer_rfm_details

)

SELECT *

FROM rfm_analysis
