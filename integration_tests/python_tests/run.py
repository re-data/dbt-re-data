import subprocess
import sys

database = sys.argv[1]
list_test_cmd = 'pytest --collect-only'

result = subprocess.check_output(list_test_cmd, shell=True)
result = result.split(b'\n')
for line in result:
    line = line.decode('UTF-8').strip()
    if '<Module ' in line:
        test_file_name = line.replace('<Module ', '').replace('>', '')
        test_cmd = f"pytest --db {database} -k '{test_file_name}'"
        print(f'Running tests in the {test_file_name} module')
        subprocess.run(test_cmd, shell=True).check_returncode()