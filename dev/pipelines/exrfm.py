Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    ecom_orders = Task(
        task_id = "ecom_orders", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "ecom_orders", "sourceName" : "itai.retail_analyst", "sourceType" : "Table"}
    )
    instore_sales = Task(
        task_id = "instore_sales", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "instore_sales", "sourceName" : "itai.retail_analyst", "sourceType" : "Table"}
    )
    crm_customers = Task(
        task_id = "crm_customers", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "crm_customers", "sourceType" : "Table", "sourceName" : "itai.retail_analyst", "alias" : ""}
    )
