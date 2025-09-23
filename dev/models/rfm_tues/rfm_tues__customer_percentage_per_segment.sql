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
    ecom_orders.order_id,
    ecom_orders.customer_id,
    ecom_orders.order_date,
    ecom_orders.order_amount,
    instore_sales.transaction_id,
    instore_sales.transaction_date,
    instore_sales.transaction_amount,
    crm_customers.signup_date,
    crm_customers.email,
    crm_customers.zip_code,
    crm_customers.region,
    crm_customers.preferred_channel
  
  FROM ecom_orders
  INNER JOIN instore_sales
     ON ecom_orders.customer_id = instore_sales.customer_id
  INNER JOIN crm_customers
     ON ecom_orders.customer_id = crm_customers.customer_id

),

orders_2025_filter AS (

  SELECT * 
  
  FROM customer_order_join
  
  WHERE EXTRACT(YEAR FROM order_date) = 2025

),

customer_order_metrics AS (

  SELECT 
    order_id AS order_id,
    customer_id AS customer_id,
    order_date AS order_date,
    order_amount AS order_amount,
    transaction_id AS transaction_id,
    transaction_date AS transaction_date,
    transaction_amount AS transaction_amount,
    signup_date AS signup_date,
    email AS email,
    zip_code AS zip_code,
    region AS region,
    preferred_channel AS preferred_channel,
    DATEDIFF(DAY, order_date, CURRENT_DATE) AS RECENCY,
    COUNT(order_id) OVER (PARTITION BY customer_id) AS FREQUENCY,
    SUM(order_amount) OVER (PARTITION BY customer_id) AS MONETARY
  
  FROM orders_2025_filter

),

customer_rfm_score AS (

  SELECT 
    customer_id AS CUSTOMER_ID,
    RECENCY,
    FREQUENCY,
    MONETARY,
    RECENCY + FREQUENCY + MONETARY AS RFM_SCORE
  
  FROM customer_order_metrics

),

customer_count_per_segment AS (

  SELECT 
    RFM_SCORE,
    COUNT(CUSTOMER_ID) AS COUNT_CUSTOMER_ID
  
  FROM customer_rfm_score
  
  GROUP BY RFM_SCORE

),

customer_percentage_per_segment AS (

  SELECT 
    RFM_SCORE,
    COUNT_CUSTOMER_ID
    * 100.0
    / (SUM(COUNT_CUSTOMER_ID) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS PERCENTAGE_OF_CUSTOMERS
  
  FROM customer_count_per_segment

)

SELECT *

FROM customer_percentage_per_segment
