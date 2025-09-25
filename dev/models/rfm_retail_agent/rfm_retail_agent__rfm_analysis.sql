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

Reformat_1 AS (

  SELECT 
    ORDER_ID AS ORDER_ID,
    CUSTOMER_ID AS CUSTOMER_ID,
    EMAIL AS EMAIL,
    ZIP_CODE AS ZIP_CODE,
    REGION AS REGION,
    DATEDIFF(DAY, MAX(ORDER_DATE), CURRENT_DATE) AS RECENCY,
    COUNT(DISTINCT ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_data_join
  
  GROUP BY 
    ORDER_ID, CUSTOMER_ID, EMAIL, ZIP_CODE, REGION

),

rfm_analysis AS (

  SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    EMAIL,
    ZIP_CODE,
    REGION,
    RECENCY,
    FREQUENCY,
    MONETARY,
    CASE
      WHEN RECENCY >= 4
        THEN 'High  '
      WHEN RECENCY BETWEEN 2 AND 3
        THEN 'Medium'
      ELSE 'Low   '
    END AS RECENCY_SCORE,
    CASE
      WHEN FREQUENCY >= 4
        THEN 'High  '
      WHEN FREQUENCY BETWEEN 2 AND 3
        THEN 'Medium'
      ELSE 'Low   '
    END AS FREQUENCY_SCORE,
    CASE
      WHEN MONETARY >= 1000
        THEN 'High  '
      WHEN MONETARY BETWEEN 500 AND 999
        THEN 'Medium'
      ELSE 'Low   '
    END AS MONETARY_SCORE,
    CONCAT(
      CASE
        WHEN RECENCY >= 4
          THEN 'H'
        WHEN RECENCY BETWEEN 2 AND 3
          THEN 'M'
        ELSE 'L'
      END, 
      CASE
        WHEN FREQUENCY >= 4
          THEN 'H'
        WHEN FREQUENCY BETWEEN 2 AND 3
          THEN 'M'
        ELSE 'L'
      END, 
      CASE
        WHEN MONETARY >= 1000
          THEN 'H'
        WHEN MONETARY BETWEEN 500 AND 999
          THEN 'M'
        ELSE 'L'
      END) AS RFM_SEGMENT
  
  FROM Reformat_1

)

SELECT *

FROM rfm_analysis
