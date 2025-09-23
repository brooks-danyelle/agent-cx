with DAG():
    rfm_tues__customer_percentage_by_flag_segment_region = Task(
        task_id = "rfm_tues__customer_percentage_by_flag_segment_region", 
        component = "Model", 
        modelName = "rfm_tues__customer_percentage_by_flag_segment_region"
    )
