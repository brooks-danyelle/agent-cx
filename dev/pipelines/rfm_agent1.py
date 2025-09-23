with DAG():
    rfm_agent1__at_risk_customers = Task(
        task_id = "rfm_agent1__at_risk_customers", 
        component = "Model", 
        modelName = "rfm_agent1__at_risk_customers"
    )
