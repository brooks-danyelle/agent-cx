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

rfm_analysis AS (

  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_AMOUNT,
    TRANSACTION_ID,
    TRANSACTION_DATE,
    TRANSACTION_AMOUNT,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    MAX(ORDER_DATE) AS MAX_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_order_join
  
  GROUP BY 
    ORDER_ID, 
    CUSTOMER_ID, 
    ORDER_DATE, 
    ORDER_AMOUNT, 
    TRANSACTION_ID, 
    TRANSACTION_DATE, 
    TRANSACTION_AMOUNT, 
    SIGNUP_DATE, 
    EMAIL, 
    ZIP_CODE, 
    REGION, 
    PREFERRED_CHANNEL

),

rfm_analysis_1 AS (

  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_AMOUNT,
    TRANSACTION_ID,
    TRANSACTION_DATE,
    TRANSACTION_AMOUNT,
    SIGNUP_DATE,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) AS RECENCY,
    FREQUENCY,
    MONETARY,
    CASE
      WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 30
        THEN 5
      WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 60
        THEN 4
      WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 90
        THEN 3
      WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 120
        THEN 2
      ELSE 1
    END
    + CASE
        WHEN FREQUENCY >= 10
          THEN 5
        WHEN FREQUENCY >= 8
          THEN 4
        WHEN FREQUENCY >= 6
          THEN 3
        WHEN FREQUENCY >= 4
          THEN 2
        ELSE 1
      END
    + CASE
        WHEN MONETARY >= 1000
          THEN 5
        WHEN MONETARY >= 750
          THEN 4
        WHEN MONETARY >= 500
          THEN 3
        WHEN MONETARY >= 250
          THEN 2
        ELSE 1
      END AS RFM_SCORE,
    CONCAT(
      CASE
        WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 30
          THEN 'R1'
        WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 60
          THEN 'R2'
        WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 90
          THEN 'R3'
        WHEN DATEDIFF(DAY, MAX_ORDER_DATE, CURRENT_DATE) <= 120
          THEN 'R4'
        ELSE 'R5'
      END, 
      CASE
        WHEN FREQUENCY >= 10
          THEN 'F1'
        WHEN FREQUENCY >= 8
          THEN 'F2'
        WHEN FREQUENCY >= 6
          THEN 'F3'
        WHEN FREQUENCY >= 4
          THEN 'F4'
        ELSE 'F5'
      END, 
      CASE
        WHEN MONETARY >= 1000
          THEN 'M1'
        WHEN MONETARY >= 750
          THEN 'M2'
        WHEN MONETARY >= 500
          THEN 'M3'
        WHEN MONETARY >= 250
          THEN 'M4'
        ELSE 'M5'
      END) AS RFM_SEGMENT
  
  FROM rfm_analysis

)

SELECT *

FROM rfm_analysis_1
