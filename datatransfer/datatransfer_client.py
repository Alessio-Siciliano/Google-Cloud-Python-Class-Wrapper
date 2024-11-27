""" Module that contains functions to connect to BigQuery. """

import re
from google.cloud.bigquery_datatransfer import (
    DataTransferServiceClient,
    TransferConfig,
)
from utils.string import String
from utils.exceptions import ScheduledQueryIdWrongFormat
from datatransfer.scheduled_query import ScheduledQuery


class DatatransferClient:
    """Class with the methods to interact with DataTransfer on GCP."""

    # Matching rule for the format of a Scheduled Query ID.
    matching_rule = """
        projects\\/[a-zA-Z0-9-]+\\/locations\\/[a-zA-Z-]+\\/transferConfigs\\/[a-zA-Z0-9-]+
    """

    def __init__(
        self,
        project_id: str,
        location: str,
        service_account_key_file_path: str = str(),
    ) -> None:
        """Create the object.

        Parameters
        ----------
        project_id : dict
            Project of the data

        location: str
            Location of the data

        service_account_key_file_path: str
            Location of the service account key file

        Returns
        -------
        None
        """
        self.project_id = project_id
        self.location = location
        self.string_util = String()
        self.list_of_scheduled_queries_objects: list[ScheduledQuery] = []

        if service_account_key_file_path == "":
            self.client = DataTransferServiceClient()
        else:
            self.client = DataTransferServiceClient.from_service_account_file(
                filename=service_account_key_file_path
            )

    def get_scheduled_query_by_id(
        self, scheduled_query_id: str
    ) -> "ScheduledQuery":
        """Get a scheduled query object by its ID.

        Parameters
        ----------
        scheduled_query_id: str
            ID of the scheduled query

        Returns
        -------
        ScheduledQuery:
            An object for the given query (if exists)
        """

        # Check if ID match the format rule to avoid useless requests.
        matching_result = re.match(self.matching_rule, scheduled_query_id)
        if matching_result is None:
            raise ScheduledQueryIdWrongFormat()

        # If the data format is correct, let's try with the request
        transfer_config = self.client.get_transfer_config(
            name=scheduled_query_id
        )

        # Create and return the object instance for the given query
        scheduled_query = ScheduledQuery()
        scheduled_query.set_attribute(
            "dataset_region", transfer_config.dataset_region
        )
        scheduled_query.set_attribute(
            "destination_dataset_name", transfer_config.destination_dataset_id
        )
        scheduled_query.set_attribute("disabled", transfer_config.disabled)
        scheduled_query.set_attribute(
            "display_name", transfer_config.display_name
        )
        scheduled_query.set_attribute("name", transfer_config.name)
        scheduled_query.set_attribute(
            "next_run_time", transfer_config.next_run_time
        )
        scheduled_query.set_attribute(
            "query", dict(transfer_config.params).get("query")
        )
        scheduled_query.set_attribute(
            "partitioning_field",
            dict(transfer_config.params).get("partitioning_field"),
        )
        scheduled_query.set_attribute(
            "destination_table_name",
            dict(transfer_config.params).get(
                "destination_table_name_template"
            ),
        )
        scheduled_query.set_attribute(
            "write_disposition",
            dict(transfer_config.params).get("write_disposition"),
        )
        scheduled_query.set_attribute("schedule", transfer_config.schedule)
        scheduled_query.set_attribute("last_state", transfer_config.state)
        scheduled_query.set_attribute(
            "last_update", transfer_config.update_time
        )
        scheduled_query.set_attribute(
            "owner_email", transfer_config.owner_info.email
        )

        return scheduled_query

    def get_all_scheduled_queries(self) -> list["ScheduledQuery"]:
        """Get ALL schedule queries of the project.
        We save the list for future calls so the data are already available.

        Parameters
        ----------
        None

        Returns
        -------
        List[ScheduledQuery]
            List of the object (if found) of the Scheduled Query

        """
        transfer_configs_request_response = self.client.list_transfer_configs(
            parent="projects/"
            + self.project_id
            + "/locations/"
            + self.location
        )
        for scheduled_query_object in transfer_configs_request_response:
            # For the moment we filter for only the scheduled queries.
            if scheduled_query_object.data_source_id != "scheduled_query":
                continue

            scheduled_query = ScheduledQuery()
            # The owner_email is populated only for GET method
            # We send a request for each scheduled query to get the data.
            owner_email = self.get_scheduled_query_by_id(
                scheduled_query_object.name
            ).owner_email
            scheduled_query.set_attribute("owner_email", owner_email)

            # Append the object to the list.
            self.list_of_scheduled_queries_objects.append(scheduled_query)
        return self.list_of_scheduled_queries_objects

    def get_scheduled_queries_by_owner_email(
        self, owner_email: str
    ) -> list[ScheduledQuery]:
        """Get all schedule queries of a given user email.

        Parameters
        ----------
        owner_email: str
            The owner email.

        Returns
        -------
        List[ScheduledQuery]
            List of the object (if found) of the Scheduled Query for the owner

        """
        if len(self.list_of_scheduled_queries_objects) == 0:
            self.get_all_scheduled_queries()
        return (
            list(
                filter(
                    lambda x: x.owner_email == owner_email,
                    self.list_of_scheduled_queries_objects,
                )
            )
            if len(self.list_of_scheduled_queries_objects) > 0
            else []
        )

    def update_scheduled_query(
        self, scheduled_query_id: str, changes: dict
    ) -> None:
        """Update a scheduled query by its ID

        Parameters
        ----------
        scheduled_query_id: str
            The ID of the scheduled query

        changes: dict
            Eeach key is an attribute to change and the value its new value.

        Returns
        -------
        None
        """
        scheduled_query_object = TransferConfig(name=scheduled_query_id)
        for key, value in changes.items():
            if not hasattr(scheduled_query_object, key):
                print("ERRORE")
            setattr(scheduled_query_object, key, value)

        # scheduled_query_object.disabled = disabled
        # scheduled_query_object.display_name = 'sched_DB_INITIAL_TARGET_2023'
        # transfer_config = self.client.update_transfer_config(
        #    {
        #        "transfer_config": scheduled_query_object,
        #        "update_mask": {"paths": list(set(changes.keys()))},
        #    }
        # )
        # TODO:magari ritornare uno stato?
