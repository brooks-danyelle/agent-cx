with DAG():
    RFM_analysis__customer_order_sales_join = Task(
        task_id = "RFM_analysis__customer_order_sales_join", 
        component = "Model", 
        modelName = "RFM_analysis__customer_order_sales_join"
    )
