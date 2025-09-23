with DAG():
    rfm_agent1__segment_customer_percentage = Task(
        task_id = "rfm_agent1__segment_customer_percentage", 
        component = "Model", 
        modelName = "rfm_agent1__segment_customer_percentage"
    )
