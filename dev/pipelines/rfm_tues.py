with DAG():
    rfm_tues__customer_rfm_scores = Task(
        task_id = "rfm_tues__customer_rfm_scores", 
        component = "Model", 
        modelName = "rfm_tues__customer_rfm_scores"
    )
