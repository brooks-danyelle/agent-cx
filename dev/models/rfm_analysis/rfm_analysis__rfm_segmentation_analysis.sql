{{
  config({    
    "materialized": "ephemeral",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

crm_customers AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'crm_customers') }}

),

ecom_orders AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

),

customer_order_sales_join AS (

  SELECT 
    crm_customers.customer_id AS CUSTOMER_ID,
    crm_customers.signup_date AS SIGNUP_DATE,
    crm_customers.email AS EMAIL,
    crm_customers.zip_code AS ZIP_CODE,
    crm_customers.region AS REGION,
    crm_customers.preferred_channel AS PREFERRED_CHANNEL,
    ecom_orders.order_id AS ORDER_ID,
    ecom_orders.order_date AS ORDER_DATE,
    ecom_orders.order_amount AS ORDER_AMOUNT,
    instore_sales.transaction_id AS TRANSACTION_ID,
    instore_sales.transaction_date AS TRANSACTION_DATE,
    instore_sales.transaction_amount AS TRANSACTION_AMOUNT
  
  FROM crm_customers
  LEFT JOIN ecom_orders
     ON crm_customers.customer_id = ecom_orders.customer_id
  LEFT JOIN instore_sales
     ON crm_customers.customer_id = instore_sales.customer_id

),

customer_rfm_analysis AS (

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
  
  FROM customer_order_sales_join
  
  GROUP BY 
    CUSTOMER_ID, SIGNUP_DATE, EMAIL, ZIP_CODE, REGION, PREFERRED_CHANNEL

),

rfm_segmentation_analysis AS (

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
    recency AS RECENCY,
    frequency AS FREQUENCY,
    monetary AS MONETARY,
    CASE
      WHEN recency <= 30
        THEN 5
      WHEN recency <= 60
        THEN 4
      WHEN recency <= 90
        THEN 3
      WHEN recency <= 120
        THEN 2
      ELSE 1
    END AS RECENCY_SCORE,
    CASE
      WHEN frequency >= 10
        THEN 5
      WHEN frequency >= 7
        THEN 4
      WHEN frequency >= 5
        THEN 3
      WHEN frequency >= 3
        THEN 2
      ELSE 1
    END AS FREQUENCY_SCORE,
    CASE
      WHEN monetary >= 1000
        THEN 5
      WHEN monetary >= 750
        THEN 4
      WHEN monetary >= 500
        THEN 3
      WHEN monetary >= 250
        THEN 2
      ELSE 1
    END AS MONETARY_SCORE,
    CONCAT(
      CASE
        WHEN recency <= 30
          THEN 'R5'
        WHEN recency <= 60
          THEN 'R4'
        WHEN recency <= 90
          THEN 'R3'
        WHEN recency <= 120
          THEN 'R2'
        ELSE 'R1'
      END, 
      CASE
        WHEN frequency >= 10
          THEN 'F5'
        WHEN frequency >= 7
          THEN 'F4'
        WHEN frequency >= 5
          THEN 'F3'
        WHEN frequency >= 3
          THEN 'F2'
        ELSE 'F1'
      END, 
      CASE
        WHEN monetary >= 1000
          THEN 'M5'
        WHEN monetary >= 750
          THEN 'M4'
        WHEN monetary >= 500
          THEN 'M3'
        WHEN monetary >= 250
          THEN 'M2'
        ELSE 'M1'
      END) AS RFM_SEGMENT
  
  FROM customer_rfm_analysis

)

SELECT *

FROM rfm_segmentation_analysis
