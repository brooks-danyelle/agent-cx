{{
  config({    
    "materialized": "ephemeral",
    "database": "danyelle",
    "schema": "demo"
  })
}}

WITH email_events AS (

  SELECT * 
  
  FROM {{ source('itai.retail_analyst', 'email_events') }}

),

mask_email_id AS (

  SELECT 
    email_id AS EMAIL_ID,
    customer_id AS CUSTOMER_ID,
    event_type AS EVENT_TYPE,
    event_date AS EVENT_DATE,
    CONCAT(SUBSTRING(email_id, 1, 3), '****@****', SUBSTRING_INDEX(email_id, '@', -1)) AS MASKED_EMAIL_ID
  
  FROM email_events

)

SELECT *

FROM mask_email_id
