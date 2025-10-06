with DAG():
    RFM_Marketing__rfm_calculation = Task(
        task_id = "RFM_Marketing__rfm_calculation", 
        component = "Model", 
        modelName = "RFM_Marketing__rfm_calculation"
    )
