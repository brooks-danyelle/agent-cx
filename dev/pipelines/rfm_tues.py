with DAG():
    rfm_tues__customer_rfm_score = Task(
        task_id = "rfm_tues__customer_rfm_score", 
        component = "Model", 
        modelName = "rfm_tues__customer_rfm_score"
    )
