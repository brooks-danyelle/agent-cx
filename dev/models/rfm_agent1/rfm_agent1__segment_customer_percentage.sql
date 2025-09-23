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

ecom_orders AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

),

instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

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

customer_data_aggregation AS (

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
  
  FROM customer_data_join
  
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

customer_rfm_metrics AS (

  SELECT 
    CUSTOMER_ID,
    ORDER_ID,
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
    MONETARY
  
  FROM customer_data_aggregation

),

rfm_segmentation AS (

  SELECT 
    CUSTOMER_ID,
    RECENCY,
    FREQUENCY,
    MONETARY,
    NTILE(5) OVER (ORDER BY RECENCY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RECENCY_SCORE,
    NTILE(5) OVER (ORDER BY FREQUENCY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FREQUENCY_SCORE,
    NTILE(5) OVER (ORDER BY MONETARY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MONETARY_SCORE,
    CONCAT(
      NTILE(5) OVER (ORDER BY RECENCY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
      NTILE(5) OVER (ORDER BY FREQUENCY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
      NTILE(5) OVER (ORDER BY MONETARY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS RFM_SEGMENT,
    {{ segment_flag() }} AS FLAG,
    EMAIL AS EMAIL,
    ZIP_CODE AS ZIP_CODE
  
  FROM customer_rfm_metrics

),

customer_count_by_segment AS (

  SELECT 
    FLAG,
    COUNT(CUSTOMER_ID) AS COUNT_CUSTOMER_ID
  
  FROM rfm_segmentation
  
  GROUP BY FLAG

),

segment_customer_percentage AS (

  SELECT 
    FLAG,
    COUNT_CUSTOMER_ID
    * 100.0
    / (SUM(COUNT_CUSTOMER_ID) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS PERCENTAGE_OF_CUSTOMERS
  
  FROM customer_count_by_segment

)

SELECT *

FROM segment_customer_percentage
