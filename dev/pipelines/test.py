with DAG():
    test__rfm_scores_calculation = Task(
        task_id = "test__rfm_scores_calculation", 
        component = "Model", 
        modelName = "test__rfm_scores_calculation"
    )
