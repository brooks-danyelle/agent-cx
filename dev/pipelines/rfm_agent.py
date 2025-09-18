with DAG():
    rfm_agent__rfm_scores_assignment = Task(
        task_id = "rfm_agent__rfm_scores_assignment", 
        component = "Model", 
        modelName = "rfm_agent__rfm_scores_assignment"
    )
