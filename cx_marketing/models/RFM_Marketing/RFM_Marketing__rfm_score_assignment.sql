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

customer_order_sales_data AS (

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

rfm_metrics AS (

  SELECT 
    CUSTOMER_ID,
    MAX(ORDER_DATE) AS LAST_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_order_sales_data
  
  GROUP BY CUSTOMER_ID

),

customer_rfm_details AS (

  SELECT 
    customer_order_sales_data.CUSTOMER_ID,
    customer_order_sales_data.SIGNUP_DATE,
    customer_order_sales_data.EMAIL,
    customer_order_sales_data.ZIP_CODE,
    customer_order_sales_data.REGION,
    customer_order_sales_data.PREFERRED_CHANNEL,
    customer_order_sales_data.ORDER_ID,
    customer_order_sales_data.ORDER_DATE,
    customer_order_sales_data.ORDER_AMOUNT,
    customer_order_sales_data.TRANSACTION_ID,
    customer_order_sales_data.TRANSACTION_DATE,
    customer_order_sales_data.TRANSACTION_AMOUNT,
    DATEDIFF(DAY, rfm_metrics.LAST_ORDER_DATE, CURRENT_DATE) AS RECENCY,
    rfm_metrics.FREQUENCY,
    rfm_metrics.MONETARY
  
  FROM customer_order_sales_data
  INNER JOIN rfm_metrics
     ON customer_order_sales_data.CUSTOMER_ID = rfm_metrics.CUSTOMER_ID

),

rfm_score_assignment AS (

  SELECT 
    CUSTOMER_ID,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    ORDER_ID,
    ORDER_DATE,
    ORDER_AMOUNT,
    TRANSACTION_ID,
    TRANSACTION_DATE,
    TRANSACTION_AMOUNT,
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
  
  FROM customer_rfm_details

)

SELECT *

FROM rfm_score_assignment
