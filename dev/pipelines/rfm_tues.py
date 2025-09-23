with DAG():
    rfm_tues__rfm_segmented_customers = Task(
        task_id = "rfm_tues__rfm_segmented_customers", 
        component = "Model", 
        modelName = "rfm_tues__rfm_segmented_customers"
    )
