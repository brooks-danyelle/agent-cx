with DAG():
    RFM_Marketing__customer_order_sales_data = Task(
        task_id = "RFM_Marketing__customer_order_sales_data", 
        component = "Model", 
        modelName = "RFM_Marketing__customer_order_sales_data"
    )
