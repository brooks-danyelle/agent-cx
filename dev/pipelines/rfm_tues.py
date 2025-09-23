with DAG():
    rfm_tues__customer_percentage_per_segment = Task(
        task_id = "rfm_tues__customer_percentage_per_segment", 
        component = "Model", 
        modelName = "rfm_tues__customer_percentage_per_segment"
    )
