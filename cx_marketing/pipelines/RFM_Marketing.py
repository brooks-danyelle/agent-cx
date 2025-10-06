with DAG():
    RFM_Marketing__rfm_scores_with_columns = Task(
        task_id = "RFM_Marketing__rfm_scores_with_columns", 
        component = "Model", 
        modelName = "RFM_Marketing__rfm_scores_with_columns"
    )
