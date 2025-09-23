with DAG():
    rfm_agent1__customer_data_join = Task(
        task_id = "rfm_agent1__customer_data_join", 
        component = "Model", 
        modelName = "rfm_agent1__customer_data_join"
    )
