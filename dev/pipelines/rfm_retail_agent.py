with DAG():
    rfm_retail_agent__customer_data_join = Task(
        task_id = "rfm_retail_agent__customer_data_join", 
        component = "Model", 
        modelName = "rfm_retail_agent__customer_data_join"
    )
