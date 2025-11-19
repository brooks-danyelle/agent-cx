with DAG():
    RFM_analysis__customer_order_rfm_analysis = Task(
        task_id = "RFM_analysis__customer_order_rfm_analysis", 
        component = "Model", 
        modelName = "RFM_analysis__customer_order_rfm_analysis"
    )
