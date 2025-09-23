{{
  config({    
    "materialized": "ephemeral",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH ecom_orders AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

),

instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

crm_customers AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'crm_customers') }}

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

)

SELECT *

FROM orders_2025_filter
