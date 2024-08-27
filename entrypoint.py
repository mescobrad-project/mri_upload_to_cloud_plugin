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

    def transform_input_data(self, source_name, personal_id, workspace_id, MRN,
                             metadata_file_name):
        """Transform input data into table suitable for creating query. Currently only
        filename and workspace are tracked in Trino table. In case that additional
        information should be tracked, also those data should be sent and this function
        should be updated accordingly."""

        import pandas as pd
        start_data = {
            "PID": [personal_id]
        }
        data_df = pd.DataFrame(data=start_data)


        data_df["pseudoMRN"] = self.calculate_pseudoMRN(MRN, workspace_id)

        if metadata_file_name is not None:
            data_df["metadata_file_name"] = metadata_file_name

        # Add rowid column representing id of the row in the file
        data_df["rowid"] = data_df.index + 1

         # Insert source column representing name of the source file
        data_df.insert(0, "source", source_name)

        # Transform table into table with 5 columns: source,
        # rowid, variable_name, variable_value, workspace_id
        data_df = data_df.melt(id_vars=["source","rowid"])
        data_df = data_df.sort_values(by=['rowid'])

        # As a variable values type string is expected
        data_df = data_df.astype({"value":"str"})

        # Add workspace id into workspace column of the table
        data_df.insert(4, "workspace_id", workspace_id)

        return data_df

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

    def calculate_pseudoMRN(self, mrn, workspace_id):
        import hashlib

        if mrn is None:
            pseudoMRN = None
        else:
            personalMRN = [mrn, workspace_id]
            personal_mrn = "".join(str(data) for data in personalMRN)

            # Generate ID
            pseudoMRN = hashlib.sha256(bytes(personal_mrn, "utf-8")).hexdigest()

        return pseudoMRN

    def generate_personal_id(self, personal_data):
        """Based on the identity, full_name and date of birth."""

        import hashlib

        personal_id = "".join(str(data) for data in personal_data)

        # Remove all whitespaces characters
        personal_id = "".join(personal_id.split())

        # Generate ID
        id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
        return id

    def create_personal_identifier(self, data_info):
        import pandas as pd
        # Generate personal id
        if all(param is not None for param in [data_info['name'],
                                               data_info['surname'],
                                               data_info['date_of_birth'],
                                               data_info['unique_id']]):

            # Make unified dates, so that different formats of date doesn't change the
            # final id
            data_info["date_of_birth"] = pd.to_datetime(data_info["date_of_birth"],
                                                        dayfirst=True)

            data_info["date_of_birth"] = data_info["date_of_birth"].strftime("%d-%m-%Y")

            # Personal id is made based on name, surname, date date of birth, and national
            # unique id
            personal_data = [data_info["name"], data_info["surname"],
                             data_info["date_of_birth"], data_info["unique_id"]]

            personal_id = self.generate_personal_id(personal_data)
        else:
            # TO DO - what is the flow if the data is not provided
            personal_data = []
            personal_id = self.generate_personal_id(personal_data)

        return personal_id

    def upload_depersonalized_MRI_data(self, file, path_to_file, data_info):
        """Upload to cloud defaced and anonymized DICOM data."""
        import os
        import boto3
        from botocore.config import Config
        from io import BytesIO

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

        # Metadata file name
        metadata_file_template = "{name}.json"
        if data_info["metadata_json_file"] is not None:
            metadata_file_name = metadata_file_template.format(
                name=os.path.splitext(os.path.basename(file))[0])
        else:
            metadata_file_name = None

        # Download defaced and anonymized file
        s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).download_file(file,
                                                                         path_to_download)
        # Create personal id
        personal_id = self.create_personal_identifier(data_info)

        # Upload metadata information to Trino
        data = self.transform_input_data(os.path.basename(file), personal_id,
                                         data_info['workspace_id'],
                                         data_info['MRN'],
                                         metadata_file_name)
        self.upload_data_on_trino(schema_name, table_name, data, conn)

        # Upload output zip file with defaced and anonymized data
        s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_file(path_to_download, file)

        # Upload metadata file
        if metadata_file_name is not None:
            obj_name_metadata = f"metadata_files/{metadata_file_name}"
            s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_fileobj(
                BytesIO(data_info["metadata_json_file"]), obj_name_metadata,
                ExtraArgs={'ContentType': "text/json"})


        print("File is uploaded on the cloud storage.")


    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        import os
        import shutil

        path_to_download_data = "mescobrad_edge/plugins/mri_upload_to_cloud_plugin/anonymize_files/"

        # create temporary folder for storing downloaded files
        os.makedirs(path_to_download_data, exist_ok=True)

        files = input_meta.file_name
        for file in files:
            if file is not None:
                self.upload_depersonalized_MRI_data(file, path_to_download_data,
                                                    input_meta.data_info)
            else:
                continue

        shutil.rmtree(path_to_download_data)

        return PluginActionResponse()
