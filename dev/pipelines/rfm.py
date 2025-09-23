with DAG():
    rfm__mask_email_id = Task(task_id = "rfm__mask_email_id", component = "Model", modelName = "rfm__mask_email_id")
    rfm__rfm_with_segment = Task(
        task_id = "rfm__rfm_with_segment", 
        component = "Model", 
        modelName = "rfm__rfm_with_segment"
    )
