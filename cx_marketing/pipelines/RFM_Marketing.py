with DAG():
    RFM_Marketing__customer_rfm_values = Task(
        task_id = "RFM_Marketing__customer_rfm_values", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_rfm_values"
    )
