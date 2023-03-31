from mescobrad_edge.plugins.mri_upload_to_cloud_plugin.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata

class GenericPlugin(EmptyPlugin):

    def upload_depersonalized_MRI_data(self, file, path_to_file):
        """Upload to cloud defaced and anonymized DICOM data."""
        import os
        import boto3
        from botocore.config import Config

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

        path_to_download = os.path.join(path_to_file, os.path.basename(file))
        # Download defaced and anonymized file
        s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).download_file(file, path_to_download)

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
            self.upload_depersonalized_MRI_data(file, path_to_download_data)

        shutil.rmtree(path_to_download_data)

        return PluginActionResponse()
