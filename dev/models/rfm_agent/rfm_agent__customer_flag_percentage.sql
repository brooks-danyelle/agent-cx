{{
  config({    
    "materialized": "table",
    "alias": "cx_demo",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH financial_transactions AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'instore_sales') }}

),

order_transactions AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'ecom_orders') }}

),

customer_insights AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'crm_customers') }}

),

customer_order_transaction_details AS (

  {#Consolidates customer transactions and profiles from online and in-store sales.#}
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
  
  FROM order_transactions AS ecom_orders
  LEFT JOIN financial_transactions AS instore_sales
     ON ecom_orders.customer_id = instore_sales.customer_id
  LEFT JOIN customer_insights AS crm_customers
     ON ecom_orders.customer_id = crm_customers.customer_id

),

customer_purchase_analysis AS (

  {#Compiles customer transaction details to assess purchase behavior, including last purchase date, frequency, and monetary value.#}
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
  
  FROM customer_order_transaction_details AS customer_id_join
  
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

rfm_scores AS (

  {#Assigns RFM scores to customers for targeted marketing based on recency, frequency, and monetary value of transactions.#}
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
  
  FROM customer_purchase_analysis AS Reformat_1

),

rfm_customer_analysis AS (

  {#Assigns RFM segments and scores to customers for targeted marketing strategies.#}
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
    CONCAT(RECENCY_SCORE, FREQUENCY_SCORE, MONETARY_SCORE) AS RFM_SEGMENT,
    {{ segment_flag() }} AS CUSTOMER_FLAG
  
  FROM rfm_scores AS rfm_scores_assignment

),

customer_flag_count AS (

  {#Counts customers by their assigned flags for segmentation analysis.#}
  SELECT 
    CUSTOMER_FLAG,
    COUNT(*) AS COUNT
  
  FROM rfm_customer_analysis AS rfm_segment_with_scores
  
  GROUP BY CUSTOMER_FLAG

),

customer_flag_percentage AS (

  {#Calculates the percentage distribution of customer flags.#}
  SELECT 
    CUSTOMER_FLAG,
    ROUND((COUNT / SUM(COUNT) OVER ()) * 100, 2) AS PERCENTAGE_OF_CUSTOMERS
  
  FROM customer_flag_count

)

SELECT *

FROM customer_flag_percentage
