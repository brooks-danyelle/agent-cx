{{
  config({    
    "materialized": "table",
    "alias": "rfm_analysis",
    "database": "danyelle",
    "schema": "retail"
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

ecommerce_instore_crm_join AS (

  {#Integrates e-commerce, in-store sales, and CRM data for comprehensive customer insights.#}
  SELECT 
    ecom_orders.order_id AS ORDER_ID,
    ecom_orders.customer_id AS ECOM_CUSTOMER_ID,
    ecom_orders.order_date AS ORDER_DATE,
    ecom_orders.order_amount AS ORDER_AMOUNT,
    instore_sales.transaction_id AS TRANSACTION_ID,
    instore_sales.customer_id AS INSTORE_CUSTOMER_ID,
    instore_sales.transaction_date AS TRANSACTION_DATE,
    instore_sales.transaction_amount AS TRANSACTION_AMOUNT,
    crm_customers.customer_id AS CRM_CUSTOMER_ID,
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

  {#Summarizes customer purchase behavior by last order date, frequency, and total spending.#}
  SELECT 
    CRM_CUSTOMER_ID,
    MAX(ORDER_DATE) AS LAST_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM ecommerce_instore_crm_join
  
  GROUP BY CRM_CUSTOMER_ID

),

rfm_calculation AS (

  {#Evaluates customer value by assessing recent activity, purchase frequency, and spending.#}
  SELECT 
    CRM_CUSTOMER_ID,
    DATEDIFF(DAY, LAST_ORDER_DATE, CURRENT_DATE) AS RECENCY,
    FREQUENCY,
    MONETARY
  
  FROM customer_rfm_aggregation

),

customer_rfm_details AS (

  {#Integrates customer contact and location data with RFM metrics for targeted marketing.#}
  SELECT 
    ecommerce_instore_crm_join.CRM_CUSTOMER_ID,
    ecommerce_instore_crm_join.EMAIL,
    ecommerce_instore_crm_join.ZIP_CODE,
    ecommerce_instore_crm_join.REGION,
    ecommerce_instore_crm_join.PREFERRED_CHANNEL,
    rfm_calculation.RECENCY,
    rfm_calculation.FREQUENCY,
    rfm_calculation.MONETARY
  
  FROM ecommerce_instore_crm_join
  INNER JOIN rfm_calculation
     ON ecommerce_instore_crm_join.CRM_CUSTOMER_ID = rfm_calculation.CRM_CUSTOMER_ID

),

rfm_scores_with_details AS (

  {#Assigns RFM scores to customers to identify high-value clients based on recency, frequency, and monetary spending.#}
  SELECT 
    CRM_CUSTOMER_ID,
    EMAIL,
    ZIP_CODE,
    REGION,
    PREFERRED_CHANNEL,
    RECENCY,
    FREQUENCY,
    MONETARY,
    CASE
      WHEN RECENCY <= 30
        THEN 5
      WHEN RECENCY <= 60
        THEN 4
      WHEN RECENCY <= 90
        THEN 3
      WHEN RECENCY <= 120
        THEN 2
      ELSE 1
    END AS RECENCY_SCORE,
    CASE
      WHEN FREQUENCY >= 10
        THEN 5
      WHEN FREQUENCY >= 7
        THEN 4
      WHEN FREQUENCY >= 5
        THEN 3
      WHEN FREQUENCY >= 3
        THEN 2
      ELSE 1
    END AS FREQUENCY_SCORE,
    CASE
      WHEN MONETARY >= 1000
        THEN 5
      WHEN MONETARY >= 750
        THEN 4
      WHEN MONETARY >= 500
        THEN 3
      WHEN MONETARY >= 250
        THEN 2
      ELSE 1
    END AS MONETARY_SCORE
  
  FROM customer_rfm_details

),

customer_rfm_segments AS (

  {#Segments customers based on recency, frequency, and monetary scores for targeted marketing.#}
  SELECT 
    CRM_CUSTOMER_ID AS CRM_CUSTOMER_ID,
    EMAIL AS EMAIL,
    ZIP_CODE AS ZIP_CODE,
    REGION AS REGION,
    PREFERRED_CHANNEL AS PREFERRED_CHANNEL,
    RECENCY AS RECENCY,
    FREQUENCY AS FREQUENCY,
    MONETARY AS MONETARY,
    RECENCY_SCORE AS RECENCY_SCORE,
    FREQUENCY_SCORE AS FREQUENCY_SCORE,
    MONETARY_SCORE AS MONETARY_SCORE,
    CONCAT(CAST(RECENCY_SCORE AS STRING), CAST(FREQUENCY_SCORE AS STRING), CAST(MONETARY_SCORE AS STRING)) AS RFM_SEGMENT
  
  FROM rfm_scores_with_details

)

SELECT *

FROM customer_rfm_segments
