with DAG():
    rfm__customer_rfm_analysis = Task(
        task_id = "rfm__customer_rfm_analysis", 
        component = "Model", 
        modelName = "rfm__customer_rfm_analysis"
    )
