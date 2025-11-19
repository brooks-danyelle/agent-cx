with DAG():
    RFM_analysis__customer_rfm_enriched = Task(
        task_id = "RFM_analysis__customer_rfm_enriched", 
        component = "Model", 
        modelName = "RFM_analysis__customer_rfm_enriched"
    )
