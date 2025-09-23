with DAG():
    rfm_tues__orders_2025_filter = Task(
        task_id = "rfm_tues__orders_2025_filter", 
        component = "Model", 
        modelName = "rfm_tues__orders_2025_filter"
    )
