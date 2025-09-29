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

instore_sales AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

ecom_orders AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

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

Reformat_1 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID,
    EMAIL AS EMAIL,
    DATEDIFF(DAY, MAX(ORDER_DATE), CURRENT_DATE) AS RECENCY,
    COUNT(DISTINCT ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM customer_order_join AS in0
  
  GROUP BY 
    CUSTOMER_ID, EMAIL

),

rfm_scores_calculation AS (

  SELECT 
    CUSTOMER_ID,
    EMAIL,
    RECENCY,
    FREQUENCY,
    MONETARY,
    NTILE(5) OVER (ORDER BY RECENCY DESC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RECENCY_SCORE,
    NTILE(5) OVER (ORDER BY FREQUENCY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FREQUENCY_SCORE,
    NTILE(5) OVER (ORDER BY MONETARY NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MONETARY_SCORE,
    {{ segment_flag() }} AS FLAG
  
  FROM Reformat_1

),

customer_count_per_segment AS (

  SELECT 
    FLAG,
    COUNT(CUSTOMER_ID) AS COUNT_CUSTOMER_ID
  
  FROM rfm_scores_calculation
  
  GROUP BY FLAG

),

percentage_of_customers_per_segment AS (

  SELECT 
    FLAG,
    COUNT_CUSTOMER_ID
    * 100.0
    / (SUM(COUNT_CUSTOMER_ID) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS PERCENTAGE_OF_CUSTOMERS
  
  FROM customer_count_per_segment

)

SELECT *

FROM percentage_of_customers_per_segment
