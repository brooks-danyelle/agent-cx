with DAG():
    rfm_agent__customer_flag_percentage = Task(
        task_id = "rfm_agent__customer_flag_percentage", 
        component = "Model", 
        modelName = "rfm_agent__customer_flag_percentage"
    )
