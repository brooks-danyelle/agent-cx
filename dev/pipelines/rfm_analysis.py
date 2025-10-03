with DAG():
    rfm_analysis__rfm_scores_with_details = Task(
        task_id = "rfm_analysis__rfm_scores_with_details", 
        component = "Model", 
        modelName = "rfm_analysis__rfm_scores_with_details"
    )
