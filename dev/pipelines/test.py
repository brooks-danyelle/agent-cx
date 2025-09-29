with DAG():
    test__percentage_of_customers_per_segment = Task(
        task_id = "test__percentage_of_customers_per_segment", 
        component = "Model", 
        modelName = "test__percentage_of_customers_per_segment"
    )
