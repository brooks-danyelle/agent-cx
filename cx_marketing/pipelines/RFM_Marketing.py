with DAG():
    RFM_Marketing__customer_sales_orders_join = Task(
        task_id = "RFM_Marketing__customer_sales_orders_join", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_sales_orders_join"
    )
