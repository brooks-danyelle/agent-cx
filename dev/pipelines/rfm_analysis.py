with DAG():
    rfm_analysis__customer_order_sales_join = Task(
        task_id = "rfm_analysis__customer_order_sales_join", 
        component = "Model", 
        modelName = "rfm_analysis__customer_order_sales_join"
    )
