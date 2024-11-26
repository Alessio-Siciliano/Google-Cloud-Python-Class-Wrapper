class Dataset:
    access_entries          = None
    created_on               = None
    dataset_id              = None
    description             = None
    friendly_name           = None
    full_dataset_id         = None
    is_case_insensitive     = None
    labels                  = None
    location                = None
    max_time_travel_hours   = None
    modified                = None
    project                 = None
    
    def set_attribute(self, attribute_name: str, attribute_value):
        if hasattr(self, attribute_name) is not None:
            setattr(self, attribute_name, attribute_value)
    
    def get_attribute(self, attribute_name: str):
        return getattr(self, attribute_name)