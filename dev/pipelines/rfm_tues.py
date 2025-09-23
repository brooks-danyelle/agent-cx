with DAG():
    rfm_tues__customer_data_join = Task(
        task_id = "rfm_tues__customer_data_join", 
        component = "Model", 
        modelName = "rfm_tues__customer_data_join"
    )
