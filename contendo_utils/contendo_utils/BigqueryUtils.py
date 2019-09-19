from google.cloud import bigquery
from google.cloud import storage
from gcsfs import GCSFileSystem

class BigqueryUtils:
    def __init__(self):
        self.__bigquery_client = bigquery.Client()
        self.__storage_client  = storage.Client()

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

    def upload_file_to_gcp(self, bucketName, inFileName, targerFileName):
        bucket = self.__storage_client.get_bucket(bucketName)
        blob = bucket.blob(targerFileName)
        res = blob.upload_from_filename(inFileName)
        return 'gs://{}/{}'.format(bucketName, targerFileName)

    def upload_string_to_gcp(self, data, bucketName, targetFileName):
        bucket = self.__storage_client.get_bucket(bucketName)
        blob = bucket.blob(targetFileName)
        res = blob.upload_from_string(data)
        return 'gs://{}/{}'.format(bucketName, targetFileName)

    def read_string_from_gcp(self, bucketName, fromFileName):
        data = None
        try:
            bucket = self.__storage_client.get_bucket(bucketName)
            blob = bucket.blob(fromFileName)
            data = blob.download_as_string()
        #except storage. NotFound as e:
        #    print('Info: File not found in BigqueryUtils.read_string_from_gcp({}, {})', bucketName, fromFileName)
        except Exception as e:
            print ('Error in BigqueryUtils.read_string_from_gcp({}, {}) {}, trace {}', bucketName, fromFileName, e, type(e))

        return data

    def create_table_from_gcp_file(self, gcpFileURI, datasetId, tableId, writeDisposition='WRITE_APPEND'):
        datasetRef = self.__bigquery_client.dataset(datasetId)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.write_disposition = writeDisposition
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        load_job = self.__bigquery_client.load_table_from_uri(
            gcpFileURI,
            datasetRef.table(tableId),
            job_config=job_config
        )
        return load_job.result()

    def create_table_from_local_file(self, localFile, datasetId, tableId, writeDisposition='WRITE_TRUNCATE', fileType=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON):
        datasetRef = self.__bigquery_client.dataset(datasetId)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.write_disposition = writeDisposition
        job_config.source_format = fileType
        fileObj = open(localFile, 'rb')
        load_job = self.__bigquery_client.load_table_from_file(
            fileObj,
            datasetRef.table(tableId),
            job_config=job_config
        )
        fileObj.close()
        return load_job.result()

    def get_table_schema(self, datasetId, tableId):

        datasetRef = self.__bigquery_client.dataset(datasetId)
        tableRef = datasetRef.table(tableId)
        table = self.__bigquery_client.get_table(tableRef)  # API Request

        schema = []
        for schemaField in table.schema:
            schema.append(schemaField.to_api_repr())

        return schema

    def execute_query(self, query):
        query_job = self.__bigquery_client.query(query)
        result = query_job.result()
        return result

    def execute_query_to_df(self, query, fillna=''):
        query_job = self.__bigquery_client.query(query)
        ret_df = query_job.result().to_dataframe().fillna(fillna)
        return ret_df

    def execute_query_to_dict(self, query, fillna=''):
        query_job = self.__bigquery_client.query(query)
        ret_df = query_job.result().to_dataframe().fillna(fillna)
        retDict = {}
        retDict['nRows'] = ret_df.shape[0]
        retDict['Columns'] = list(ret_df.columns)
        rows = []
        for i,row in ret_df.iterrows():
            rows.append(dict(row))
        retDict['Rows'] = rows
        return retDict

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