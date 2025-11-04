with DAG():
    RFM_Marketing__customer_rfm_scores = Task(
        task_id = "RFM_Marketing__customer_rfm_scores", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_rfm_scores"
    )
