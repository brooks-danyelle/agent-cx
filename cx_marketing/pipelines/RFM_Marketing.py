with DAG():
    RFM_Marketing__customer_activity_overview = Task(
        task_id = "RFM_Marketing__customer_activity_overview", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_activity_overview"
    )
