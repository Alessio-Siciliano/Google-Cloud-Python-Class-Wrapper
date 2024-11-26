class ScheduledQuery:
    dataset_region           = None
    destination_dataset_name = None
    disabled                 = None
    display_name             = None
    name                     = None
    next_run_time            = None
    query                    = None
    partitioning_field       = None
    destination_table_name   = None
    write_disposition        = None
    schedule                 = None
    last_state               = None
    last_update              = None
    owner_email              = None
    
    def set_attribute(self, attribute_name: str, attribute_value):
        if hasattr(self, attribute_name) is not None:
            print(f"Setto {attribute_name} con il valore {attribute_value}")
            setattr(self, attribute_name, attribute_value)
    
    def get_attribute(self, attribute_name: str):
        return getattr(self, attribute_name)