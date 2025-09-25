with DAG():
    rfm_retail_agent__rfm_segment_analysis = Task(
        task_id = "rfm_retail_agent__rfm_segment_analysis", 
        component = "Model", 
        modelName = "rfm_retail_agent__rfm_segment_analysis"
    )
