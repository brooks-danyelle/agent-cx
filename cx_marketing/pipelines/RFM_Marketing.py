with DAG():
    RFM_Marketing__customer_order_sales_join = Task(
        task_id = "RFM_Marketing__customer_order_sales_join", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_order_sales_join"
    )
