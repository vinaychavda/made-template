import os


def test_exercise(expected_output_file):
    if os.path.isfile(expected_output_file):
        print(f"\t[SUCCESS] Found output file {expected_output_file}")
    else:
        print(f"\t[ERROR] Can not find expected output file: {expected_output_file}.")
        return


current_directory = os.path.dirname(os.path.abspath(__file__))
file_name = "./data/data.sqlite"
file_path = os.path.join(current_directory, file_name)
if __name__ == "__main__":
    test_exercise(file_path)
