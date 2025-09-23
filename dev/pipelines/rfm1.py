with DAG():
    rfm1__customer_id_joined_data = Task(
        task_id = "rfm1__customer_id_joined_data", 
        component = "Model", 
        modelName = "rfm1__customer_id_joined_data"
    )
