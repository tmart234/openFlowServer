import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import tempfile
import shutil
import logging

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
        
        
        # Now import openflow_cron after setting the environment variables
        global openflow_cron
        import openflow_cron

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
    def test_authenticate(self, mock_login):
        # Setup
        mock_auth = MagicMock()
        mock_login.return_value = mock_auth

        # Execute
        result = openflow_cron.authenticate()

        # Assert
        mock_login.assert_called_once_with(strategy="environment")
        self.assertEqual(result, mock_auth)

    @patch('openflow_cron.earthaccess.login')
    def test_authenticate_failure(self, mock_login):
        # Setup
        mock_login.side_effect = Exception("Authentication failed")

        # Execute and Assert
        with self.assertRaises(Exception):
            openflow_cron.authenticate()

    @patch('openflow_cron.earthaccess.search_datasets')
    def test_search_vegdri_dataset(self, mock_search_datasets):
        # Setup
        mock_results = [MagicMock(), MagicMock()]
        mock_search_datasets.return_value = mock_results

        # Execute
        result = openflow_cron.search_vegdri_dataset()

        # Assert
        mock_search_datasets.assert_called_once_with(
            short_name="VegDRI",
            cloud_hosted=True
        )
        self.assertEqual(result, mock_results)

    @patch('openflow_cron.earthaccess.search_datasets')
    def test_search_vegdri_dataset_failure(self, mock_search_datasets):
        # Setup
        mock_search_datasets.side_effect = Exception("Search failed")

        # Execute and Assert
        with self.assertRaises(Exception):
            openflow_cron.search_vegdri_dataset()

if __name__ == '__main__':
    unittest.main()