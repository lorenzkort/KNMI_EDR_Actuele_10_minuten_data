import os
import sys
import requests
from src.logging_config import setup_logging

logger = setup_logging(__name__)

class OpenDataAPI:
    def __init__(self, api_token: str):
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.headers = {"Authorization": api_token}
        logger.info("Initialized OpenDataAPI client")

    def __get_data(self, url, params=None):
        logger.debug(f"Making API request to: {url}")
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def list_files(self, dataset_name: str, dataset_version: str, params: dict):
        logger.info(f"Listing files for dataset {dataset_name} version {dataset_version}")
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
            params=params,
        )

    def get_file_url(self, dataset_name: str, dataset_version: str, file_name: str):
        logger.info(f"Getting download URL for file: {file_name}")
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{file_name}/url"
        )

def download_file_from_temporary_download_url(download_url, filename):
    try:
        logger.info(f"Starting download of file: {filename}")
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            block_size = 8192
            downloaded = 0
            
            # Create directory if it doesn't exist
            os.makedirs("./dataset-download", exist_ok=True)
            output_path = f"./dataset-download/{filename}"
            
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=block_size):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:  # Only show progress if we know the total size
                        progress = (downloaded / total_size) * 100
                        logger.info(f"Download progress: {progress:.1f}%")
                        
            logger.info(f"Successfully downloaded {downloaded:,} bytes to {output_path}")
            return output_path
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {str(e)}")
        raise

def main():
    try:
        api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImQ1ZjE0YzUxZTYyMTQ4MmI5ZDM1YWYwMmE4NDE5OTVkIiwiaCI6Im11cm11cjEyOCJ9"
        dataset_name = "Actuele10mindataKNMIstations"
        dataset_version = "2"
        logger.info(f"Starting download process for {dataset_name} version {dataset_version}")

        api = OpenDataAPI(api_token=api_key)

        # sort the files in descending order and only retrieve the first file
        params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
        response = api.list_files(dataset_name, dataset_version, params)
        if "error" in response:
            error_msg = f"Unable to retrieve list of files: {response['error']}"
            logger.error(error_msg)
            raise Exception(error_msg)

        latest_file = response["files"][0].get("filename")
        logger.info(f"Found latest file: {latest_file}")

        # fetch the download url and download the file
        response = api.get_file_url(dataset_name, dataset_version, latest_file)
        download_file_from_temporary_download_url(response["temporaryDownloadUrl"], latest_file)
        logger.info("Download step completed successfully")
        
    except Exception as e:
        logger.error(f"Download step failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()