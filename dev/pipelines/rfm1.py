with DAG():
    rfm1__customer_rfm_analysis = Task(
        task_id = "rfm1__customer_rfm_analysis", 
        component = "Model", 
        modelName = "rfm1__customer_rfm_analysis"
    )
