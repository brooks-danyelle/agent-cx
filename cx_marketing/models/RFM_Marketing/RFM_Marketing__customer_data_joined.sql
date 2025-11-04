{{
  config({    
    "materialized": "ephemeral",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH instore_sales AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'instore_sales') }}

),

ecomm_orders AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'ecomm_orders') }}

),

crm_customers AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'crm_customers') }}

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

)

SELECT *

FROM customer_data_joined
