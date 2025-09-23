with DAG():
    rfm_agent1__customer_rfm_metrics = Task(
        task_id = "rfm_agent1__customer_rfm_metrics", 
        component = "Model", 
        modelName = "rfm_agent1__customer_rfm_metrics"
    )
