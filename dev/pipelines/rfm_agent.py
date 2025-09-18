with DAG():
    rfm_agent__customer_id_join = Task(
        task_id = "rfm_agent__customer_id_join", 
        component = "Model", 
        modelName = "rfm_agent__customer_id_join"
    )
