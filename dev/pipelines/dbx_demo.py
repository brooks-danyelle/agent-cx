with DAG():
    dbx_demo__ecommerce_instore_customer_join = Task(
        task_id = "dbx_demo__ecommerce_instore_customer_join", 
        component = "Model", 
        modelName = "dbx_demo__ecommerce_instore_customer_join"
    )
