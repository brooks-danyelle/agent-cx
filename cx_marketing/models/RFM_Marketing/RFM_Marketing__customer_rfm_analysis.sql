{{
  config({    
    "materialized": "ephemeral",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH ecomm_orders AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'ecomm_orders') }}

),

instore_sales AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'instore_sales') }}

),

crm_customers AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'crm_customers') }}

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

)

SELECT *

FROM customer_rfm_analysis
