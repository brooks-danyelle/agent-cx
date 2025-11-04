with DAG():
    RFM_Marketing__customer_rfm_details = Task(
        task_id = "RFM_Marketing__customer_rfm_details", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_rfm_details"
    )
