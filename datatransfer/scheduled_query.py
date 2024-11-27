""" Package that models the ScheduledQuery object. """

from utils.generic_object import Generic


class ScheduledQuery(Generic):
    """Class that identifies a ScheduledQuery object."""

    def __init__(self):
        self.dataset_region = None
        self.destination_dataset_name = None
        self.disabled = None
        self.display_name = None
        self.name = None
        self.next_run_time = None
        self.query = None
        self.partitioning_field = None
        self.destination_table_name = None
        self.write_disposition = None
        self.schedule = None
        self.last_state = None
        self.last_update = None
        self.owner_email = None
