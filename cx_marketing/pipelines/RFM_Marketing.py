with DAG():
    crm_customers = Task(
        task_id = "crm_customers", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "crm_customers", "sourceType" : "Table", "sourceName" : "danyelle.retail", "alias" : ""}
    )
    ecomm_orders = Task(
        task_id = "ecomm_orders", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "ecomm_orders", "sourceType" : "Table", "sourceName" : "danyelle.retail", "alias" : ""}
    )
    instore_sales = Task(
        task_id = "instore_sales", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "instore_sales", "sourceType" : "Table", "sourceName" : "danyelle.retail", "alias" : ""}
    )
