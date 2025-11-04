with DAG():
    crm_customers_1 = Task(
        task_id = "crm_customers_1", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "crm_customers", "sourceName" : "danyelle.retail", "sourceType" : "Table"}
    )
    instore_sales_1 = Task(
        task_id = "instore_sales_1", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "instore_sales", "sourceName" : "danyelle.retail", "sourceType" : "Table"}
    )
    instore_sales = Task(
        task_id = "instore_sales", 
        component = "Dataset", 
        table = {"name" : "instore_sales", "sourceType" : "Source", "sourceName" : "danyelle.retail"}
    )
    ecomm_orders_1 = Task(
        task_id = "ecomm_orders_1", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "ecomm_orders", "sourceName" : "danyelle.retail", "sourceType" : "Table"}
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
