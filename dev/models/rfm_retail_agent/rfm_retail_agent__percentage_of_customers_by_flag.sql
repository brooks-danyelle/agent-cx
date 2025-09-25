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
      WHEN monetary >= 1000
        THEN 5
      WHEN monetary >= 500
        THEN 4
      WHEN monetary >= 250
        THEN 3
      WHEN monetary >= 100
        THEN 2
      ELSE 1
    END AS FREQUENCY_SCORE,
    CASE
      WHEN frequency >= 10
        THEN 5
      WHEN frequency >= 5
        THEN 4
      WHEN frequency >= 3
        THEN 3
      WHEN frequency >= 2
        THEN 2
      ELSE 1
    END AS MONETARY_SCORE
  
  FROM Reformat_1

),

rfm_segment_analysis AS (

  SELECT 
    ORDER_ID AS ORDER_ID,
    CUSTOMER_ID AS CUSTOMER_ID,
    EMAIL AS EMAIL,
    ZIP_CODE AS ZIP_CODE,
    REGION AS REGION,
    RECENCY AS RECENCY,
    FREQUENCY AS FREQUENCY,
    MONETARY AS MONETARY,
    RECENCY_SCORE AS RECENCY_SCORE,
    FREQUENCY_SCORE AS FREQUENCY_SCORE,
    MONETARY_SCORE AS MONETARY_SCORE,
    CONCAT(RECENCY_SCORE, FREQUENCY_SCORE, MONETARY_SCORE) AS RFM_SEGMENT,
    {{ segment_flag() }} AS FLAG
  
  FROM rfm_analysis

),

customer_count_by_flag AS (

  SELECT 
    FLAG,
    COUNT(DISTINCT CUSTOMER_ID) AS COUNT_DISTINCT_CUSTOMER_ID
  
  FROM rfm_segment_analysis
  
  GROUP BY FLAG

),

percentage_of_customers_by_flag AS (

  SELECT 
    FLAG,
    COUNT_DISTINCT_CUSTOMER_ID
    * 100.0
    / (SUM(COUNT_DISTINCT_CUSTOMER_ID) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS PERCENTAGE_OF_CUSTOMERS
  
  FROM customer_count_by_flag

)

SELECT *

FROM percentage_of_customers_by_flag
