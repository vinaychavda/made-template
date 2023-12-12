import os


def test_exercise(expected_output_file):
    if os.path.isfile(expected_output_file):
        print(f"\t[SUCCESS] Found output file {expected_output_file}")
    else:
        print(f"\t[ERROR] Can not find expected output file: {expected_output_file}.")
        return


test_exercise('data/co2_emission_test1.db')
test_exercise('data/population_data.db')
