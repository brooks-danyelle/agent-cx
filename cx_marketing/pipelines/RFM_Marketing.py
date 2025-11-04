with DAG():
    RFM_Marketing__customer_data_joined = Task(
        task_id = "RFM_Marketing__customer_data_joined", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_data_joined"
    )
