with DAG():
    RFM_Marketing__rfm_analysis_with_scores = Task(
        task_id = "RFM_Marketing__rfm_analysis_with_scores", 
        component = "Model", 
        modelName = "RFM_Marketing__rfm_analysis_with_scores"
    )
