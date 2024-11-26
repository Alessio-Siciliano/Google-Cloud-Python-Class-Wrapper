from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetReference
from utils.Exceptions import InvalidArgumentToFunction
from bigquery.Dataset import Dataset

class BigqueryClient:
    
    def __init__(self, project_id: str = str(), location: str = str(), service_account_key_file_path: str = str()) -> None:
        """
        Return a singleton instance of BigQuery Client.
        
        Parameters
        ----------
        
        project_id: str
            The Bigquery project ID, optional
        
        location: str
            The location of the project (e.g 'eu'), optional
            
        service_account_key_file_path: str
            The JSON key associated with the service account, optional.
        """
        if (project_id == '' or location == '') and service_account_key_file_path == '':
            raise InvalidArgumentToFunction()

        self.project_id = project_id
        self.location   = location        
        if service_account_key_file_path == '':
            self.client = bigquery.Client(project=project_id, location=location)
        else:
            self.client = bigquery.Client(project=project_id, location=location).from_service_account_json(service_account_key_file_path)

    def query(self, query, dry_run: bool = False, return_pandas: bool = False):
        """
        Execute a query for real or in dry_mode
        
        Parameters
        ----------
        query: str
            string with the query
        dry_run: bool
            Flag dry_run mode
        return_pandas: bool
            Flag to return the result as pandas dataframe
        
        Returns
        -------
        result
            An iterator with the result
        """
        job_config = bigquery.QueryJobConfig()
        job_config.dry_run = dry_run
        result = self.client.query_and_wait(query, job_config = job_config)

        return result.to_dataframe() if return_pandas else result

    def get_list_of_datasets(self) -> list[dict]:
        """
        Return the entire list of datasets of the project. These datasets are children of the class DatasetListItem.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        List[dict()]
            A list of dictionary, where each dict() is a dataset with a short list of informations
        """
        datasets = list(self.client.list_datasets())
        datasets_dict = [{'dataset_id': x.dataset_id, 'friendly_name': x.friendly_name, 'labels': x.labels} for x in datasets]
        return datasets_dict

    def get_dataset_by_name(self, dataset_name: str) -> 'Dataset':
        """
        Return a wrapped instance of a given dataset.
        
        Parameters
        ----------
        
        dataset_name: str
            The name of the dataset to look for
        
        Returns
        -------
        Dataset
            An object that identifies a dataset.
        """
        
        dataset = self.client.get_dataset(DatasetReference.from_string(default_project = self.project_id, dataset_id = dataset_name))
        dataset_object = Dataset()
        
        """ Set attributes for my wrapper class Dataset """ 
        dataset_object.set_attribute('access_entries', dataset.access_entries)
        dataset_object.set_attribute('created_on', dataset.created)
        dataset_object.set_attribute('dataset_id', dataset.dataset_id)
        dataset_object.set_attribute('description', dataset.description)
        dataset_object.set_attribute('friendly_name', dataset.friendly_name)
        dataset_object.set_attribute('full_dataset_id', dataset.full_dataset_id)
        dataset_object.set_attribute('is_case_insensitive', dataset.is_case_insensitive)
        dataset_object.set_attribute('labels', dataset.labels)
        dataset_object.set_attribute('location', dataset.location)
        dataset_object.set_attribute('max_time_travel_hours', dataset.max_time_travel_hours)
        dataset_object.set_attribute('modified', dataset.modified)
        dataset_object.set_attribute('project', dataset.project)

        return dataset_object