with DAG():
    RFM_Marketing__rfm_metrics_calculation = Task(
        task_id = "RFM_Marketing__rfm_metrics_calculation", 
        component = "Model", 
        modelName = "RFM_Marketing__rfm_metrics_calculation"
    )
