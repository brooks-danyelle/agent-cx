with DAG():
    rfm_retail_agent__rfm_analysis = Task(
        task_id = "rfm_retail_agent__rfm_analysis", 
        component = "Model", 
        modelName = "rfm_retail_agent__rfm_analysis"
    )
