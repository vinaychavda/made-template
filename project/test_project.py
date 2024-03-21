import unittest
import os


class TestDataSqliteIsExistOrNot(unittest.TestCase):
    def test_data_sqlite_existence(self):
        # Get the current directory of the script
        current_directory = os.path.dirname(os.path.abspath(__file__))

        # Construct the path to the data.sqlite file
        sqlite_file_path = os.path.join(current_directory, '..', 'data', 'data.sqlite')

        # Check if the file exists
        self.assertTrue(os.path.isfile(sqlite_file_path), "data.sqlite does not exist in the data folder.")
        if os.path.isfile(sqlite_file_path):
            print('data.sqlite exists.')


if __name__ == "__main__":
    unittest.main()
