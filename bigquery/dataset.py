""" Ths module contains the class to model the Dataset object. """

from utils.generic_object import Generic


class Dataset(Generic):
    """Object that represents the original Dataset object."""

    def __init__(self) -> None:
        super().__init__()
        self.access_entries = None
        self.created_on = None
        self.dataset_id = None
        self.description = None
        self.friendly_name = None
        self.full_dataset_id = None
        self.is_case_insensitive = None
        self.labels = None
        self.location = None
        self.max_time_travel_hours = None
        self.modified = None
        self.project = None
