with DAG():
    rfm_agent__percentage_of_customers_per_flag = Task(
        task_id = "rfm_agent__percentage_of_customers_per_flag", 
        component = "Model", 
        modelName = "rfm_agent__percentage_of_customers_per_flag"
    )
