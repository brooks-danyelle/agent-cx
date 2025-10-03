with DAG():
    rfm_analysis__customer_rfm_details = Task(
        task_id = "rfm_analysis__customer_rfm_details", 
        component = "Model", 
        modelName = "rfm_analysis__customer_rfm_details"
    )
