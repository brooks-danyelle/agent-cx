with DAG():
    test__customer_order_join = Task(
        task_id = "test__customer_order_join", 
        component = "Model", 
        modelName = "test__customer_order_join"
    )
