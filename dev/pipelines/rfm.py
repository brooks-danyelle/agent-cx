with DAG():
    rfm__percentage_of_customers_per_segment = Task(
        task_id = "rfm__percentage_of_customers_per_segment", 
        component = "Model", 
        modelName = "rfm__percentage_of_customers_per_segment"
    )
