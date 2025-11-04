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

crm_customers AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'crm_customers') }}

),

instore_sales AS (

  SELECT * 
  
  FROM {{ source('danyelle.retail', 'instore_sales') }}

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

)

SELECT *

FROM customer_rfm_analysis
