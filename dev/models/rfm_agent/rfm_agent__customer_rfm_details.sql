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

crm_customers AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'crm_customers') }}

),

instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

customer_id_join AS (

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
    ORDER_DATE AS ORDER_DATE,
    ORDER_AMOUNT AS ORDER_AMOUNT,
    TRANSACTION_ID AS TRANSACTION_ID,
    TRANSACTION_DATE AS TRANSACTION_DATE,
    TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT,
    SIGNUP_DATE AS SIGNUP_DATE,
    EMAIL AS EMAIL,
    ZIP_CODE AS ZIP_CODE,
    REGION AS REGION,
    PREFERRED_CHANNEL AS PREFERRED_CHANNEL,
    MAX(ORDER_DATE) AS LAST_PURCHASE,
    DATEDIFF(DAY, MAX(ORDER_DATE), CURRENT_DATE) AS RECENCY,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_id_join
  
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

rfm_scores_assignment AS (

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
    LAST_PURCHASE,
    RECENCY,
    FREQUENCY,
    MONETARY,
    NTILE(5) OVER (ORDER BY RECENCY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RECENCY_SCORE,
    NTILE(5) OVER (ORDER BY FREQUENCY DESC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FREQUENCY_SCORE,
    NTILE(5) OVER (ORDER BY MONETARY DESC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MONETARY_SCORE
  
  FROM Reformat_1

),

rfm_segment_with_scores AS (

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
    LAST_PURCHASE,
    RECENCY,
    FREQUENCY,
    MONETARY,
    RECENCY_SCORE,
    FREQUENCY_SCORE,
    MONETARY_SCORE,
    CONCAT(CAST(RECENCY_SCORE AS STRING), '-', CAST(FREQUENCY_SCORE AS STRING), '-', CAST(MONETARY_SCORE AS STRING)) AS RFM_SEGMENT
  
  FROM rfm_scores_assignment

),

customer_rfm_details AS (

  SELECT 
    ORDER_ID AS ORDER_ID,
    CUSTOMER_ID AS CUSTOMER_ID,
    ORDER_DATE AS ORDER_DATE,
    ORDER_AMOUNT AS ORDER_AMOUNT,
    TRANSACTION_ID AS TRANSACTION_ID,
    TRANSACTION_DATE AS TRANSACTION_DATE,
    TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT,
    SIGNUP_DATE AS SIGNUP_DATE,
    EMAIL AS EMAIL,
    ZIP_CODE AS ZIP_CODE,
    REGION AS REGION,
    PREFERRED_CHANNEL AS PREFERRED_CHANNEL,
    LAST_PURCHASE AS LAST_PURCHASE,
    RECENCY AS RECENCY,
    FREQUENCY AS FREQUENCY,
    MONETARY AS MONETARY,
    RECENCY_SCORE AS RECENCY_SCORE,
    FREQUENCY_SCORE AS FREQUENCY_SCORE,
    MONETARY_SCORE AS MONETARY_SCORE,
    RFM_SEGMENT AS RFM_SEGMENT,
    {{ segment_flag() }} AS CUSTOMER_FLAG
  
  FROM rfm_segment_with_scores

)

SELECT *

FROM customer_rfm_details
