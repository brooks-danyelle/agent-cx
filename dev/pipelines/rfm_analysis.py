with DAG():
    rfm_analysis__customer_rfm_analysis = Task(
        task_id = "rfm_analysis__customer_rfm_analysis", 
        component = "Model", 
        modelName = "rfm_analysis__customer_rfm_analysis"
    )
