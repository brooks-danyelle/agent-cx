with DAG():
    rfm_analysis__customer_rfm_segments = Task(
        task_id = "rfm_analysis__customer_rfm_segments", 
        component = "Model", 
        modelName = "rfm_analysis__customer_rfm_segments"
    )
