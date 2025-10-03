with DAG():
    rfm_analysis__rfm_segmentation_analysis = Task(
        task_id = "rfm_analysis__rfm_segmentation_analysis", 
        component = "Model", 
        modelName = "rfm_analysis__rfm_segmentation_analysis"
    )
