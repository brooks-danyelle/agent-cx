with DAG():
    RFM_Marketing__customer_rfm_segmentation = Task(
        task_id = "RFM_Marketing__customer_rfm_segmentation", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_rfm_segmentation"
    )
