from google.cloud import bigquery

class BigqueryUtils:
    def __init__(self):
        self.__bigquery_client = bigquery.Client()

    def create_dataset(self, datasetId: str):
        #
        # Make sure the target dataset exists, or create it.
        try:
            assert datasetId in [dataset.dataset_id
                                  for dataset in list(self.__bigquery_client.list_datasets())]
        except AssertionError:  # dataset doesn't exist
            datasetReference = self.__bigquery_client.dataset(datasetId)
            dataset = bigquery.Dataset(datasetReference)
            dataset.location = 'US'
            dataset = self.__bigquery_client.create_dataset(dataset)

        return

    def execute_query_to_df(self, query):
        query_job = self.__bigquery_client.query(query)
        ret_df = query_job.result().to_dataframe().fillna('')
        return ret_df

    def execute_query_with_schema_and_target(self, query, targetDataset, targetTable, schemaDataset=None, schemaTable=None):

        #
        # Reset the table based on schema
        writeDisposition = 'WRITE_TRUNCATE'
        if schemaDataset is not None and schemaTable is not None:
            copyJobConfig = bigquery.CopyJobConfig()
            copyJobConfig.write_disposition = 'WRITE_TRUNCATE'
            copy_job = self.__bigquery_client.copy_table(
                self.__bigquery_client.dataset(schemaDataset).table(schemaTable),
                self.__bigquery_client.dataset(targetDataset).table(targetTable),
                job_config=copyJobConfig
            )
            writeDisposition = 'WRITE_APPEND'

        #
        # Set job configuration & execute
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.__bigquery_client.dataset(targetDataset).table(targetTable)
        job_config.write_disposition = writeDisposition
        metrics_query_job = self.__bigquery_client.query(query, job_config=job_config)
        nRows = metrics_query_job.result().total_rows
        return nRows

    #def write_dataset_to_bigquery(self, pdDataset, targetTableId):


#import os
#os.chdir('~/PycharmProjects')
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../sportsight-tests.json"
#bqu = BigqueryUtils()
#bqu.execute_query_with_schema_and_target('SELECT lastUpdatedOn  FROM `sportsight-tests.Baseball1.pbp_playoffs_2018` LIMIT 100', 'test_dataset', 'test_table')