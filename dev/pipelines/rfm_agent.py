with DAG():
    rfm_agent__customer_percentage_and_flag = Task(
        task_id = "rfm_agent__customer_percentage_and_flag", 
        component = "Model", 
        modelName = "rfm_agent__customer_percentage_and_flag"
    )
