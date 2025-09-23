{{
  config({    
    "materialized": "table",
    "alias": "CustomerSegmentationDemo_rfm1_p70pn",
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

customer_id_joined_data AS (

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

customer_rfm_analysis AS (

  SELECT 
    ANY_VALUE(ORDER_ID) AS ORDER_ID,
    CUSTOMER_ID,
    ANY_VALUE(ORDER_DATE) AS ORDER_DATE,
    ANY_VALUE(ORDER_AMOUNT) AS ORDER_AMOUNT,
    ANY_VALUE(TRANSACTION_ID) AS TRANSACTION_ID,
    ANY_VALUE(TRANSACTION_DATE) AS TRANSACTION_DATE,
    SUM(TRANSACTION_AMOUNT) AS TRANSACTION_AMOUNT,
    ANY_VALUE(SIGNUP_DATE) AS SIGNUP_DATE,
    ANY_VALUE(EMAIL) AS EMAIL,
    ANY_VALUE(ZIP_CODE) AS ZIP_CODE,
    ANY_VALUE(REGION) AS REGION,
    ANY_VALUE(PREFERRED_CHANNEL) AS PREFERRED_CHANNEL,
    DATEDIFF(DAY, MAX(TRANSACTION_DATE), CURRENT_DATE) AS recency,
    COUNT(DISTINCT TRANSACTION_ID) AS frequency,
    SUM(TRANSACTION_AMOUNT) AS monetary
  
  FROM customer_id_joined_data
  
  GROUP BY CUSTOMER_ID

),

rfm_segmentation_analysis AS (

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
    recency,
    frequency,
    monetary,
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
    END AS recency_score,
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
    END AS frequency_score,
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
    END AS monetary_score,
    CONCAT(
      CAST(CASE
        WHEN recency <= 30
          THEN 5
        WHEN recency <= 60
          THEN 4
        WHEN recency <= 90
          THEN 3
        WHEN recency <= 120
          THEN 2
        ELSE 1
      END AS STRING), 
      CAST(CASE
        WHEN frequency >= 10
          THEN 5
        WHEN frequency >= 7
          THEN 4
        WHEN frequency >= 5
          THEN 3
        WHEN frequency >= 3
          THEN 2
        ELSE 1
      END AS STRING), 
      CAST(CASE
        WHEN monetary >= 1000
          THEN 5
        WHEN monetary >= 750
          THEN 4
        WHEN monetary >= 500
          THEN 3
        WHEN monetary >= 250
          THEN 2
        ELSE 1
      END AS STRING)) AS RFM_segment,
    {{ segment_flag() }} AS CUSTOMER_FLAG
  
  FROM customer_rfm_analysis

)

SELECT *

FROM rfm_segmentation_analysis
