with DAG():
    rfm__customer_rfm_email_join = Task(
        task_id = "rfm__customer_rfm_email_join", 
        component = "Model", 
        modelName = "rfm__customer_rfm_email_join"
    )
