with DAG():
    exrfm__customer_rfm_details = Task(
        task_id = "exrfm__customer_rfm_details", 
        component = "Model", 
        modelName = "exrfm__customer_rfm_details"
    )
