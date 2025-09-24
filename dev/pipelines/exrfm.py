with DAG():
    exrfm__customer_rfm_analysis = Task(
        task_id = "exrfm__customer_rfm_analysis", 
        component = "Model", 
        modelName = "exrfm__customer_rfm_analysis"
    )
