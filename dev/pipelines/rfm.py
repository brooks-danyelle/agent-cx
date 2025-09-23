with DAG():
    rfm__rfm_scores_calculation = Task(
        task_id = "rfm__rfm_scores_calculation", 
        component = "Model", 
        modelName = "rfm__rfm_scores_calculation"
    )
