import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import tempfile
import shutil
import logging
from datetime import datetime, timedelta
import earthaccess

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestOpenFlowCron(unittest.TestCase):

    def setUp(self):
        # Create temporary directories for logs and DB
        self.temp_dir = tempfile.mkdtemp()
        self.log_path = os.path.join(self.temp_dir, 'openflow_cron.log')
        self.db_path = os.path.join(self.temp_dir, 'data.db')
        
        # Set environment variables for paths
        os.environ['OPENFLOW_LOG_PATH'] = self.log_path
        os.environ['OPENFLOW_DB_PATH'] = self.db_path
        
      # Set environment variables for authentication
        # These will be overwritten by GitHub Actions secrets if running in CI
        if 'EARTHDATA_USERNAME' not in os.environ:
            os.environ['EARTHDATA_USERNAME'] = 'mock_username'
        if 'EARTHDATA_PASSWORD' not in os.environ:
            os.environ['EARTHDATA_PASSWORD'] = 'mock_password'
       
        global openflow_cron
        import openflow_cron
        # Authenticate with earthaccess
        self.auth = earthaccess.login(strategy="environment")


    def tearDown(self):
        # Close all logging handlers
        for handler in logging.root.handlers[:]:
            handler.close()
            logging.root.removeHandler(handler)
        
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Clear environment variables
        for var in ['OPENFLOW_LOG_PATH', 'OPENFLOW_DB_PATH', 'NASA_USERNAME', 'NASA_PASSWORD']:
            if var in os.environ:
                del os.environ[var]

    @patch('openflow_cron.earthaccess.login')
    def test_authentication(self):
        logging.info("Testing authentication...")
        try:
            auth = earthaccess.login(strategy="environment")
            self.assertIsNotNone(auth)
            logging.info("Authentication successful")
            logging.info(f"Authentication type: {type(auth).__name__}")
            logging.info(f"Is authenticated: {auth.authenticated}")
        except Exception as e:
            logging.error(f"Authentication failed: {str(e)}")
            logging.error(f"Error type: {type(e).__name__}")
            logging.error(f"Error args: {e.args}")
            raise

    @patch('openflow_cron.earthaccess.login')
    def test_authenticate_failure(self, mock_login):
        # Setup
        mock_login.side_effect = Exception("Authentication failed")

        # Execute and Assert
        with self.assertRaises(Exception):
            openflow_cron.authenticate()

    @patch('openflow_cron.earthaccess.search_datasets')
    def test_search_vegdri_dataset(self):
        results = openflow_cron.search_vegdri_dataset()
        self.assertIsNotNone(results)
        self.assertTrue(len(results) > 0)
        print(f"Found {len(results)} VegDRI dataset results")
        
        # Print details of the first few results
        for i, result in enumerate(results[:5]):  # Print details of first 5 results
            print(f"\nDataset {i+1}:")
            print(f"Short Name: {result.short_name}")
            print(f"Version: {result.version}")
            print(f"Time Start: {result.time_start}")
            print(f"Time End: {result.time_end}")

    @patch('openflow_cron.earthaccess.search_datasets')
    def test_search_vegdri_dataset(self, mock_search_datasets):
        # Mock the search_datasets method to return some dummy results
        mock_result = MagicMock()
        mock_result.short_name = "VegDRI"
        mock_result.version = "1"
        mock_result.time_start = "2020-01-01T00:00:00.000Z"
        mock_result.time_end = "2023-12-31T23:59:59.999Z"
        mock_search_datasets.return_value = [mock_result]

        results = openflow_cron.search_vegdri_dataset()
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 1)
        print(f"Found {len(results)} VegDRI dataset results")
        
        if len(results) > 0:
            # Print details of the first few results
            for i, result in enumerate(results[:5]):  # Print details of first 5 results
                print(f"\nDataset {i+1}:")
                print(f"Short Name: {result.short_name}")
                print(f"Version: {result.version}")
                print(f"Time Start: {result.time_start}")
                print(f"Time End: {result.time_end}")
        else:
            print("No VegDRI datasets found. Please check your search criteria and authentication.")

        # Assert that the search_datasets method was called with the correct parameters
        mock_search_datasets.assert_called_once_with(
            short_name="VegDRI",
            cloud_hosted=True
        )

    @patch('openflow_cron.earthaccess.search_datasets')
    def test_search_any_dataset(self, mock_search_datasets):
        logging.info("Searching for any available dataset...")
        mock_result = MagicMock()
        mock_result.short_name = "SampleDataset"
        mock_result.version = "1"
        mock_result.time_start = "2020-01-01T00:00:00.000Z"
        mock_result.time_end = "2023-12-31T23:59:59.999Z"
        mock_search_datasets.return_value = [mock_result]

        results = earthaccess.search_datasets(cloud_hosted=True)
        self.assertIsNotNone(results)
        self.assertGreater(len(results), 0)
        logging.info(f"Found {len(results)} datasets")
        if len(results) > 0:
            for i, result in enumerate(results[:5]):
                logging.info(f"Dataset {i+1}:")
                logging.info(f"  Short Name: {result.short_name}")
                logging.info(f"  Version: {result.version}")
                logging.info(f"  Time Start: {result.time_start}")
                logging.info(f"  Time End: {result.time_end}")
        else:
            logging.warning("No datasets found. This may indicate an issue with the search functionality or authentication.")

        mock_search_datasets.assert_called_once_with(cloud_hosted=True)
        
    def test_find_date_range_vegdri(self):
        logging.info("Starting test_find_date_range_vegdri")
        results = openflow_cron.search_vegdri_dataset()
        logging.info(f"search_vegdri_dataset returned {len(results)} results")
        start_date, end_date = openflow_cron.find_date_range(results)
        
        logging.info(f"VegDRI dataset date range: {start_date} to {end_date}")
        
        if start_date is None or end_date is None:
            logging.warning("No date range found. This could be due to no datasets being returned.")
            return
        
        # Check for data 8 days ago
        eight_days_ago = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d")
        has_recent_data = self._data_exists_for_date(results, eight_days_ago)
        logging.info(f"Data exists for 8 days ago ({eight_days_ago}): {has_recent_data}")
        
        # Check for data 10 years ago
        ten_years_ago = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
        has_old_data = self._data_exists_for_date(results, ten_years_ago)
        logging.info(f"Data exists for 10 years ago ({ten_years_ago}): {has_old_data}")

        # Only assert if we actually found data
        if len(results) > 0:
            self.assertTrue(has_recent_data or has_old_data, "Expected to find data either 8 days ago or 10 years ago")
        else:
            logging.warning("No results found, skipping assertions")
            
    def _data_exists_for_date(self, results, target_date):
        target_date = datetime.strptime(target_date, "%Y-%m-%d")
        for result in results:
            start_date = datetime.strptime(result.time_start, "%Y-%m-%dT%H:%M:%S.%fZ")
            end_date = datetime.strptime(result.time_end, "%Y-%m-%dT%H:%M:%S.%fZ")
            if start_date <= target_date <= end_date:
                return True
        return False


if __name__ == '__main__':
    unittest.main()