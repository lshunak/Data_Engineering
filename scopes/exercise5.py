import os
import stat
import sys

def check_and_modify_permissions(file_name):
    if not os.path.exists(file_name):
        print(f"File '{file_name}' not found.")
        return

    # Check if the file is executable
    if os.access(file_name, os.X_OK):
        print(f"'{file_name}' is already executable.")
    else:
        print(f"'{file_name}' is not executable. Changing permissions...")

        os.chmod(file_name, os.stat(file_name).st_mode | stat.S_IXUSR )
        print(f"Execute permissions added for owner")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a file name.")
    else:
        file_name = sys.argv[1]
        check_and_modify_permissions(file_name)
