with DAG():
    rfm_tues__customer_order_metrics = Task(
        task_id = "rfm_tues__customer_order_metrics", 
        component = "Model", 
        modelName = "rfm_tues__customer_order_metrics"
    )
