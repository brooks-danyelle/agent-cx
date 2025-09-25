with DAG():
    rfm_retail_agent__percentage_of_customers_by_flag = Task(
        task_id = "rfm_retail_agent__percentage_of_customers_by_flag", 
        component = "Model", 
        modelName = "rfm_retail_agent__percentage_of_customers_by_flag"
    )
