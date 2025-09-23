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

customer_data_join AS (

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

rfm_aggregation AS (

  SELECT 
    CUSTOMER_ID,
    MAX(ORDER_DATE) AS MOST_RECENT_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_data_join
  
  GROUP BY CUSTOMER_ID

),

rfm_calculation AS (

  SELECT 
    CUSTOMER_ID,
    MOST_RECENT_ORDER_DATE,
    DATEDIFF(DAY, MOST_RECENT_ORDER_DATE, CURRENT_DATE) AS RECENCY,
    FREQUENCY,
    MONETARY
  
  FROM rfm_aggregation

),

customer_rfm_analysis AS (

  SELECT 
    CUSTOMER_ID,
    MOST_RECENT_ORDER_DATE,
    RECENCY,
    FREQUENCY,
    MONETARY,
    RECENCY + FREQUENCY + MONETARY AS RFM_SCORE,
    CASE
      WHEN RECENCY >= 4 AND FREQUENCY >= 4 AND MONETARY >= 4
        THEN 'Champions      '
      WHEN RECENCY >= 3 AND FREQUENCY >= 3 AND MONETARY >= 3
        THEN 'Loyal Customers'
      ELSE 'Others         '
    END AS RFM_SEGMENT
  
  FROM rfm_calculation

)

SELECT *

FROM customer_rfm_analysis
