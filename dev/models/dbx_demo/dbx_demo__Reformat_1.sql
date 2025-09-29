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

instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

ecom_orders AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

),

ecommerce_instore_customer_join AS (

  SELECT 
    ecom_orders.order_id AS ORDER_ID,
    ecom_orders.customer_id AS ECOM_CUSTOMER_ID,
    ecom_orders.order_date AS ORDER_DATE,
    ecom_orders.order_amount AS ORDER_AMOUNT,
    instore_sales.transaction_id AS TRANSACTION_ID,
    instore_sales.customer_id AS INSTORE_CUSTOMER_ID,
    instore_sales.transaction_date AS TRANSACTION_DATE,
    instore_sales.transaction_amount AS TRANSACTION_AMOUNT,
    crm_customers.signup_date AS SIGNUP_DATE,
    crm_customers.email AS EMAIL,
    crm_customers.zip_code AS ZIP_CODE,
    crm_customers.region AS REGION,
    crm_customers.preferred_channel AS PREFERRED_CHANNEL
  
  FROM ecom_orders
  LEFT JOIN crm_customers
     ON ecom_orders.customer_id = crm_customers.customer_id
  LEFT JOIN instore_sales
     ON ecom_orders.customer_id = instore_sales.customer_id

),

Reformat_1 AS (

  SELECT 
    ORDER_ID AS ORDER_ID,
    ECOM_CUSTOMER_ID AS ECOM_CUSTOMER_ID,
    ORDER_DATE AS ORDER_DATE,
    ORDER_AMOUNT AS ORDER_AMOUNT,
    EMAIL AS EMAIL,
    ZIP_CODE AS ZIP_CODE,
    DATEDIFF(DAY, GREATEST(ORDER_DATE, TRANSACTION_DATE), CURRENT_DATE) AS RECENCY,
    (ORDER_AMOUNT + TRANSACTION_AMOUNT) AS MONETARY,
    (
      SELECT COUNT(DISTINCT ORDER_ID)
      
      FROM ecommerce_instore_customer_join AS e
      
      WHERE e.ECOM_CUSTOMER_ID = ecommerce_instore_customer_join.ECOM_CUSTOMER_ID
     )
    + (
        SELECT COUNT(DISTINCT TRANSACTION_ID)
        
        FROM ecommerce_instore_customer_join AS e
        
        WHERE e.ECOM_CUSTOMER_ID = ecommerce_instore_customer_join.ECOM_CUSTOMER_ID
       ) AS FREQUENCY
  
  FROM ecommerce_instore_customer_join

)

SELECT *

FROM Reformat_1
