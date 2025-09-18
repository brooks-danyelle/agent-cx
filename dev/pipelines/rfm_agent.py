with DAG():
    rfm_agent__rfm_segment_with_scores = Task(
        task_id = "rfm_agent__rfm_segment_with_scores", 
        component = "Model", 
        modelName = "rfm_agent__rfm_segment_with_scores"
    )
