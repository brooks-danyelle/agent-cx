with DAG():
    test__customer_rfm_scores = Task(
        task_id = "test__customer_rfm_scores", 
        component = "Model", 
        modelName = "test__customer_rfm_scores"
    )
