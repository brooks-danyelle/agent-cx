with DAG():
    rfm_agent__customer_rfm_details = Task(
        task_id = "rfm_agent__customer_rfm_details", 
        component = "Model", 
        modelName = "rfm_agent__customer_rfm_details"
    )
