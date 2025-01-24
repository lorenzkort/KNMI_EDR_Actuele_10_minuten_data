import os
import sys
import requests
from src.logging_config import setup_logging

# Set up logging for the application
logger = setup_logging(__name__)

class OpenDataAPI:
    """
    A client for interacting with the KNMI Open Data API.
    
    Attributes:
        api_token (str): The API token used for authentication.
        base_url (str): Base URL for the KNMI Open Data API.
        headers (dict): HTTP headers for authentication.
    """
    def __init__(self, api_token: str):
        """
        Initializes the OpenDataAPI client with the provided API token.

        Args:
            api_token (str): The API token for authenticating requests.
        """
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.headers = {"Authorization": api_token}
        logger.info("Initialized OpenDataAPI client")

    def __get_data(self, url, params=None):
        """
        Makes an authenticated GET request to the specified API endpoint.

        Args:
            url (str): The endpoint URL to send the request to.
            params (dict, optional): Query parameters to include in the request.

        Returns:
            dict: The JSON response from the API.

        Raises:
            HTTPError: If the HTTP request returned an unsuccessful status code.
        """
        logger.debug(f"Making API request to: {url}")
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def list_files(self, dataset_name: str, dataset_version: str, params: dict):
        """
        Retrieves a list of files for a specific dataset version.

        Args:
            dataset_name (str): Name of the dataset.
            dataset_version (str): Version of the dataset.
            params (dict): Query parameters for filtering or sorting the files.

        Returns:
            dict: The JSON response containing the list of files.
        """
        logger.info(f"Listing files for dataset {dataset_name} version {dataset_version}")
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
            params=params,
        )

    def get_file_url(self, dataset_name: str, dataset_version: str, file_name: str):
        """
        Retrieves the download URL for a specific file in a dataset.

        Args:
            dataset_name (str): Name of the dataset.
            dataset_version (str): Version of the dataset.
            file_name (str): Name of the file to retrieve.

        Returns:
            dict: The JSON response containing the temporary download URL.
        """
        logger.info(f"Getting download URL for file: {file_name}")
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{file_name}/url"
        )


def download_file_from_temporary_download_url(download_url, filename):
    """
    Downloads a file from a temporary download URL.

    Args:
        download_url (str): The temporary URL from which to download the file.
        filename (str): Name of the file to save locally.

    Returns:
        str: The local path where the downloaded file is saved.

    Raises:
        RequestException: If an error occurs during the download.
    """
    try:
        logger.info(f"Starting download of file: {filename}")
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            block_size = 8192  # 8 KB blocks for downloading
            downloaded = 0

            # Create the download directory if it doesn't exist
            os.makedirs("./dataset-download", exist_ok=True)
            output_path = f"./dataset-download/{filename}"

            # Write the file to disk in chunks
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=block_size):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:  # Display progress if file size is known
                        progress = (downloaded / total_size) * 100
                        logger.info(f"Download progress: {progress:.1f}%")

            logger.info(f"Successfully downloaded {downloaded:,} bytes to {output_path}")
            return output_path

    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {str(e)}")
        raise


def main():
    """
    Main function to handle the end-to-end process of interacting with the KNMI Open Data API.
    It retrieves the latest file from a specific dataset and downloads it.
    """
    try:
        # API token for authentication
        api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImQ1ZjE0YzUxZTYyMTQ4MmI5ZDM1YWYwMmE4NDE5OTVkIiwiaCI6Im11cm11cjEyOCJ9"
        dataset_name = "Actuele10mindataKNMIstations"  # Name of the dataset
        dataset_version = "2"  # Version of the dataset
        logger.info(f"Starting download process for {dataset_name} version {dataset_version}")

        # Initialize the API client
        api = OpenDataAPI(api_token=api_key)

        # Define query parameters to retrieve the latest file
        params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
        response = api.list_files(dataset_name, dataset_version, params)

        # Check for errors in the response
        if "error" in response:
            error_msg = f"Unable to retrieve list of files: {response['error']}"
            logger.error(error_msg)
            raise Exception(error_msg)

        # Extract the filename of the latest file
        latest_file = response["files"][0].get("filename")
        logger.info(f"Found latest file: {latest_file}")

        # Fetch the download URL and download the file
        response = api.get_file_url(dataset_name, dataset_version, latest_file)
        download_file_from_temporary_download_url(response["temporaryDownloadUrl"], latest_file)
        logger.info("Download step completed successfully")

    except Exception as e:
        logger.error(f"Download step failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
