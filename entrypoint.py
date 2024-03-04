from mescobrad_edge.plugins.mri_upload_to_cloud_plugin.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata

class GenericPlugin(EmptyPlugin):

    def execute_sql_on_trino(self, sql, conn):
        """Generic function to execute a SQL statement"""

        # Get a cursor from the connection object
        cur = conn.cursor()

        # Execute sql statement
        cur.execute(sql)

        # Get the results from the cluster
        rows = cur.fetchall()

        # Return the results
        return rows

    def transform_input_data(self, source_name, workspace_id):
        """Transform input data into table suitable for creating query. Currently only
        filename and workspace are tracked in Trino table. In case that additional
        information should be tracked, also those data should be sent and this function
        should be updated accordingly."""

        import pandas as pd

        columns = ['source', 'rowid', 'variable', 'value', 'workspace_id']
        d = {'source': source_name, 'rowid': 0, 'variable': "None", 'value': "None",
             'workspace_id': workspace_id}
        df = pd.DataFrame(data=d, index=[0], columns=columns)

        return df

    def upload_data_on_trino(self, schema_name, table_name, data, conn):
        """Create sql statement for inserting data and update
        the table with data"""

        # Iterate through pandas dataframe to extract each row values
        data_list = []
        for row in data.itertuples(index=False):
            data_list.append(str(tuple(row)))
        data_to_insert = ", ".join(data_list)

        # Insert data into the table
        sql_statement = "INSERT INTO iceberg.{schema_name}.{table_name} VALUES {data}"\
            .format(schema_name=schema_name, table_name=table_name, data=data_to_insert)

        self.execute_sql_on_trino(sql=sql_statement, conn=conn)

    def upload_depersonalized_MRI_data(self, file, path_to_file, workspace_id):
        """Upload to cloud defaced and anonymized DICOM data."""
        import os
        import boto3
        from botocore.config import Config

        from trino.dbapi import connect
        from trino.auth import BasicAuthentication

        s3 = boto3.resource('s3',
                            endpoint_url= self.__OBJ_STORAGE_URL__,
                            aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID__,
                            aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET__,
                            config=Config(signature_version='s3v4'),
                            region_name=self.__OBJ_STORAGE_REGION__)

        s3_local = boto3.resource('s3',
                                  endpoint_url= self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        # Initialize the connection with Trino
        conn = connect(
            host=self.__TRINO_HOST__,
            port=self.__TRINO_PORT__,
            http_scheme="https",
            auth=BasicAuthentication(self.__TRINO_USER__, self.__TRINO_PASSWORD__)
        )

        # Get the schema name, schema in Trino is an equivalent to a bucket in MinIO
        # Trino doesn't allow to have "-" in schema name so it needs to be replaced
        # with "_"
        schema_name = self.__OBJ_STORAGE_BUCKET__.replace("-", "_")

        # Get the table name
        table_name = self.__OBJ_STORAGE_TABLE__.replace("-", "_")

        path_to_download = os.path.join(path_to_file, os.path.basename(file))

        # Download defaced and anonymized file
        s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).download_file(file,
                                                                         path_to_download)

        # Upload metadata information to Trino
        data = self.transform_input_data(os.path.basename(file), workspace_id)
        self.upload_data_on_trino(schema_name, table_name, data, conn)

        # Upload output zip file with defaced and anonymized data
        s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_file(path_to_download, file)


        print("File is uploaded on the cloud storage.")


    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        import os
        import shutil

        path_to_download_data = "mescobrad_edge/plugins/mri_upload_to_cloud_plugin/anonymize_files/"

        # create temporary folder for storing downloaded files
        os.makedirs(path_to_download_data, exist_ok=True)

        files = input_meta.file_name
        for file in files:
            self.upload_depersonalized_MRI_data(file, path_to_download_data,
                                                input_meta.workspace_id)

        shutil.rmtree(path_to_download_data)

        return PluginActionResponse()
