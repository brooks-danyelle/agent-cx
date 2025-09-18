with DAG():
    rfm_demo__customer_order_join = Task(
        task_id = "rfm_demo__customer_order_join", 
        component = "Model", 
        modelName = "rfm_demo__customer_order_join"
    )
