Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    hw_customers = Task(
        task_id = "hw_customers", 
        component = "Dataset", 
        table = {"name" : "hw_customers", "sourceType" : "Table", "sourceName" : "danyelle.helloworld", "alias" : ""}
    )
    hw_orders = Task(
        task_id = "hw_orders", 
        component = "Dataset", 
        table = {"name" : "hw_orders", "sourceType" : "Table", "sourceName" : "danyelle.helloworld", "alias" : ""}
    )
