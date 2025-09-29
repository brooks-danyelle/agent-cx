with DAG():
    rfm_retail_agent__customer_flag_distribution = Task(
        task_id = "rfm_retail_agent__customer_flag_distribution", 
        component = "Model", 
        modelName = "rfm_retail_agent__customer_flag_distribution"
    )
