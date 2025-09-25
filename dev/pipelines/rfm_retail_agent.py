with DAG():
    rfm_retail_agent__customer_segment_percentage = Task(
        task_id = "rfm_retail_agent__customer_segment_percentage", 
        component = "Model", 
        modelName = "rfm_retail_agent__customer_segment_percentage"
    )
