with DAG():
    rfm__rfm_with_segment = Task(
        task_id = "rfm__rfm_with_segment", 
        component = "Model", 
        modelName = "rfm__rfm_with_segment"
    )
    email_events = Task(
        task_id = "email_events", 
        component = "Dataset", 
        table = {"name" : "email_events", "sourceType" : "Source", "sourceName" : "itai.retail_analyst", "alias" : ""}
    )
