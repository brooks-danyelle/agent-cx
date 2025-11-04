with DAG():
    instore_sales = Task(
        task_id = "instore_sales", 
        component = "Dataset", 
        table = {"name" : "instore_sales", "sourceType" : "Source", "sourceName" : "danyelle.retail"}
    )
    crm_customers = Task(
        task_id = "crm_customers", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "crm_customers", "sourceType" : "Table", "sourceName" : "danyelle.retail", "alias" : ""}
    )
    ecomm_orders = Task(
        task_id = "ecomm_orders", 
        component = "Dataset", 
        table = {"name" : "ecomm_orders", "sourceType" : "Source", "sourceName" : "danyelle.retail"}
    )
