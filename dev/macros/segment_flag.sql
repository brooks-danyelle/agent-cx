{% macro segment_flag() %}
CASE
   -- Best customers: Recent, frequent, and high spenders
   WHEN recency_score = 5 AND frequency_score = 5 AND monetary_score = 5
        THEN 'Champion'


   -- Loyal customers: buy often, spend decently
   WHEN frequency_score >= 4 AND monetary_score >= 3
        THEN 'Loyal'


   -- Big spenders: high spenders but not frequent
   WHEN monetary_score = 5 AND frequency_score <= 3
        THEN 'High spend but not frequent'


   -- New customers: very recent, low frequency and spend
   WHEN recency_score = 5 AND frequency_score <= 2 AND monetary_score <= 2
        THEN 'New Customer'


   -- At risk: used to spend/buy often but haven’t bought recently
   WHEN recency_score <= 2 AND (frequency_score >= 4 OR monetary_score >= 4)
        THEN 'At Risk'


   -- Lost customers: inactive, low frequency, low spend
   WHEN recency_score = 1 AND frequency_score = 1 AND monetary_score = 1
        THEN 'Lost'


   -- Catch-all bucket
   ELSE 'Other'
   END
{% endmacro %}

 