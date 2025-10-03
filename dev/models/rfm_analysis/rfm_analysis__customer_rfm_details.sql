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

ecommerce_instore_crm_join AS (

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

  SELECT 
    CRM_CUSTOMER_ID,
    MAX(ORDER_DATE) AS LAST_ORDER_DATE,
    COUNT(ORDER_ID) AS FREQUENCY,
    SUM(ORDER_AMOUNT) AS MONETARY
  
  FROM ecommerce_instore_crm_join
  
  GROUP BY CRM_CUSTOMER_ID

),

rfm_calculation AS (

  SELECT 
    CRM_CUSTOMER_ID,
    DATEDIFF(DAY, LAST_ORDER_DATE, CURRENT_DATE) AS RECENCY,
    FREQUENCY,
    MONETARY
  
  FROM customer_rfm_aggregation

),

customer_rfm_details AS (

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

)

SELECT *

FROM customer_rfm_details
