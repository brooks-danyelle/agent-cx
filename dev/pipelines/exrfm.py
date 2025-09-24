with DAG():
    exrfm__customer_order_join = Task(
        task_id = "exrfm__customer_order_join", 
        component = "Model", 
        modelName = "exrfm__customer_order_join"
    )
