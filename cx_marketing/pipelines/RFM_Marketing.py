with DAG():
    RFM_Marketing__rfm_score_assignment = Task(
        task_id = "RFM_Marketing__rfm_score_assignment", 
        component = "Model", 
        modelName = "RFM_Marketing__rfm_score_assignment"
    )
