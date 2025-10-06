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

customer_sales_orders_join AS (

  SELECT 
    crm_customers.customer_id AS CUSTOMER_ID,
    crm_customers.signup_date AS SIGNUP_DATE,
    crm_customers.email AS EMAIL,
    crm_customers.zip_code AS ZIP_CODE,
    crm_customers.region AS REGION,
    crm_customers.preferred_channel AS PREFERRED_CHANNEL,
    ecomm_orders.order_id AS ORDER_ID,
    ecomm_orders.order_date AS ORDER_DATE,
    ecomm_orders.order_amount AS ORDER_AMOUNT,
    instore_sales.transaction_id AS TRANSACTION_ID,
    instore_sales.transaction_date AS TRANSACTION_DATE,
    instore_sales.transaction_amount AS TRANSACTION_AMOUNT
  
  FROM crm_customers
  LEFT JOIN ecomm_orders
     ON crm_customers.customer_id = ecomm_orders.customer_id
  LEFT JOIN instore_sales
     ON crm_customers.customer_id = instore_sales.customer_id

),

customer_rfm_values AS (

  SELECT 
    CUSTOMER_ID,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    ANY_VALUE(ORDER_ID) AS ORDER_ID,
    ANY_VALUE(ORDER_DATE) AS ORDER_DATE,
    SUM(ORDER_AMOUNT) AS ORDER_AMOUNT,
    ANY_VALUE(TRANSACTION_ID) AS TRANSACTION_ID,
    ANY_VALUE(TRANSACTION_DATE) AS TRANSACTION_DATE,
    SUM(TRANSACTION_AMOUNT) AS TRANSACTION_AMOUNT,
    DATEDIFF(DAY, MAX(ORDER_DATE), CURRENT_DATE) AS recency,
    COUNT(DISTINCT ORDER_ID) AS frequency,
    SUM(ORDER_AMOUNT) AS monetary
  
  FROM customer_sales_orders_join
  
  GROUP BY 
    CUSTOMER_ID, SIGNUP_DATE, EMAIL, ZIP_CODE, REGION, PREFERRED_CHANNEL

)

SELECT *

FROM customer_rfm_values
