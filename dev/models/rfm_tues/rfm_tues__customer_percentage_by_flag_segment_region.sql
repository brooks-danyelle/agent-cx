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

customer_rfm_aggregation AS (

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
    MAX(TRANSACTION_DATE) AS MAX_TRANSACTION_DATE,
    COUNT(DISTINCT ORDER_ID) AS FREQUENCY_SCORE,
    SUM(ORDER_AMOUNT) AS MONETARY_SCORE
  
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

customer_rfm_scores AS (

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
    DATEDIFF(DAY, MAX_TRANSACTION_DATE, CURRENT_DATE) AS RECENCY_SCORE,
    FREQUENCY_SCORE,
    MONETARY_SCORE
  
  FROM customer_rfm_aggregation

),

rfm_segmented_customers AS (

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
    RECENCY_SCORE,
    FREQUENCY_SCORE,
    MONETARY_SCORE,
    CASE
      WHEN RECENCY_SCORE >= 4 AND FREQUENCY_SCORE >= 4 AND MONETARY_SCORE >= 4
        THEN 'Champions      '
      WHEN RECENCY_SCORE >= 3 AND FREQUENCY_SCORE >= 3 AND MONETARY_SCORE >= 3
        THEN 'Loyal Customers'
      ELSE 'Others         '
    END AS RFM_SEGMENT,
    {{ segment_flag() }} AS CUSTOMER_FLAG
  
  FROM customer_rfm_scores

),

customer_count_by_region_segment_flag AS (

  SELECT 
    REGION,
    RFM_SEGMENT,
    CUSTOMER_FLAG,
    COUNT(*) AS COUNT
  
  FROM rfm_segmented_customers
  
  GROUP BY 
    REGION, RFM_SEGMENT, CUSTOMER_FLAG

),

customer_percentage_by_flag_segment_region AS (

  SELECT 
    CUSTOMER_FLAG,
    RFM_SEGMENT,
    REGION,
    COUNT
    * 100.0
    / (SUM(COUNT) OVER (PARTITION BY CUSTOMER_FLAG RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS PERCENTAGE_OF_CUSTOMERS
  
  FROM customer_count_by_region_segment_flag

)

SELECT *

FROM customer_percentage_by_flag_segment_region
