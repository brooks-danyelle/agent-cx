with DAG():
    crm_customers = Task(
        task_id = "crm_customers", 
        component = "Dataset", 
        table = {"name" : "crm_customers", "sourceType" : "Source", "sourceName" : "itai.retail_analyst"}
    )
    ecom_orders = Task(
        task_id = "ecom_orders", 
        component = "Dataset", 
        table = {"name" : "ecom_orders", "sourceType" : "Table", "sourceName" : "itai.retail_analyst"}
    )
    instore_sales = Task(
        task_id = "instore_sales", 
        component = "Dataset", 
        table = {"name" : "instore_sales", "sourceType" : "Table", "sourceName" : "itai.retail_analyst"}
    )
