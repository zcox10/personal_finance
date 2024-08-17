class BqTable:
    def __init__(
        self,
        project_id,
        dataset_id,
        table_id,
        table_schema,
        table_description=None,
        labels=None,
        partition_field=None,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_schema = table_schema
        self.table_description = table_description
        self.labels = labels
        self.partition_field = partition_field

    @property
    def full_table_name(self):
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"
