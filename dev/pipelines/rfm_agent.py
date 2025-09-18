with DAG():
    rfm_agent__customer_percentage_by_flag_segment_region = Task(
        task_id = "rfm_agent__customer_percentage_by_flag_segment_region", 
        component = "Model", 
        modelName = "rfm_agent__customer_percentage_by_flag_segment_region"
    )
