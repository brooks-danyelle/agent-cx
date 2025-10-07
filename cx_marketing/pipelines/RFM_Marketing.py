with DAG():
    RFM_Marketing__customer_orders_joined = Task(
        task_id = "RFM_Marketing__customer_orders_joined", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_orders_joined"
    )
