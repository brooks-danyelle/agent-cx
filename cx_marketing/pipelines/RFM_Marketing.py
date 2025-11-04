with DAG():
    RFM_Marketing__customer_rfm_analysis = Task(
        task_id = "RFM_Marketing__customer_rfm_analysis", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_rfm_analysis"
    )
