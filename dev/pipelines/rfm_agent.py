with DAG():
    rfm_agent__percentage_customers_at_risk = Task(
        task_id = "rfm_agent__percentage_customers_at_risk", 
        component = "Model", 
        modelName = "rfm_agent__percentage_customers_at_risk"
    )
