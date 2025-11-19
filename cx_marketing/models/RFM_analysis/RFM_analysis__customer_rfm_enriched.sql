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

customer_order_sales_join AS (

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

customer_rfm_aggregation AS (

  SELECT 
    CUSTOMER_ID,
    MAX(TRANSACTION_DATE) AS LAST_TRANSACTION_DATE,
    COUNT(DISTINCT TRANSACTION_ID) AS FREQUENCY,
    SUM(TRANSACTION_AMOUNT) AS MONETARY
  
  FROM customer_order_sales_join
  
  GROUP BY CUSTOMER_ID

),

rfm_scoring AS (

  SELECT 
    CUSTOMER_ID,
    DATEDIFF(DAY, LAST_TRANSACTION_DATE, CURRENT_DATE) AS RECENCY,
    FREQUENCY,
    MONETARY,
    NTILE(5) OVER (ORDER BY DATEDIFF(DAY, LAST_TRANSACTION_DATE, CURRENT_DATE) NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RECENCY_SCORE,
    NTILE(5) OVER (ORDER BY FREQUENCY DESC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FREQUENCY_SCORE,
    NTILE(5) OVER (ORDER BY MONETARY DESC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MONETARY_SCORE
  
  FROM customer_rfm_aggregation

),

customer_rfm_enriched AS (

  SELECT 
    customer_order_sales_join.CUSTOMER_ID,
    customer_order_sales_join.SIGNUP_DATE,
    customer_order_sales_join.EMAIL,
    customer_order_sales_join.ZIP_CODE,
    customer_order_sales_join.REGION,
    customer_order_sales_join.PREFERRED_CHANNEL,
    customer_order_sales_join.ORDER_ID,
    customer_order_sales_join.ORDER_DATE,
    customer_order_sales_join.ORDER_AMOUNT,
    customer_order_sales_join.TRANSACTION_ID,
    customer_order_sales_join.TRANSACTION_DATE,
    customer_order_sales_join.TRANSACTION_AMOUNT,
    rfm_scoring.RECENCY,
    rfm_scoring.FREQUENCY,
    rfm_scoring.MONETARY,
    rfm_scoring.RECENCY_SCORE + rfm_scoring.FREQUENCY_SCORE + rfm_scoring.MONETARY_SCORE AS RFM_SCORE
  
  FROM customer_order_sales_join
  LEFT JOIN rfm_scoring
     ON customer_order_sales_join.CUSTOMER_ID = rfm_scoring.CUSTOMER_ID

)

SELECT *

FROM customer_rfm_enriched
