with DAG():
    dbx_demo__frequent_customer_zip_codes = Task(
        task_id = "dbx_demo__frequent_customer_zip_codes", 
        component = "Model", 
        modelName = "dbx_demo__frequent_customer_zip_codes"
    )
