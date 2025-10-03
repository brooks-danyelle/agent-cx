with DAG():
    rfm_analysis__ecommerce_instore_crm_join = Task(
        task_id = "rfm_analysis__ecommerce_instore_crm_join", 
        component = "Model", 
        modelName = "rfm_analysis__ecommerce_instore_crm_join"
    )
