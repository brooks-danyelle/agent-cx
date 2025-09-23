with DAG():
    rfm__customer_data_join = Task(
        task_id = "rfm__customer_data_join", 
        component = "Model", 
        modelName = "rfm__customer_data_join"
    )
