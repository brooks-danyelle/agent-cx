with DAG():
    RFM_Marketing__customer_data_join = Task(
        task_id = "RFM_Marketing__customer_data_join", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_data_join"
    )
