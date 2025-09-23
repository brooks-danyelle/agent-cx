with DAG():
    send_cx_report = Task(
        task_id = "send_cx_report", 
        component = "Email", 
        body = "csv output", 
        subject = "CX segmentation report", 
        includeData = False, 
        fileName = "", 
        to = ["danyelle@prophecy.io"], 
        fileFormat = "", 
        hasTemplate = False
    )
    rfm_agent__customer_flag_percentage = Task(
        task_id = "rfm_agent__customer_flag_percentage", 
        component = "Model", 
        modelName = "rfm_agent__customer_flag_percentage"
    )
    rfm_agent__customer_flag_percentage.out_0 >> send_cx_report.customer_flag_percentage
