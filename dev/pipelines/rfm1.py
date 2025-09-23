with DAG():
    rfm1__rfm_score_and_segment = Task(
        task_id = "rfm1__rfm_score_and_segment", 
        component = "Model", 
        modelName = "rfm1__rfm_score_and_segment"
    )
