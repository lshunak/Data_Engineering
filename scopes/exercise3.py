import os

def os_info():
    os_name = os.name
    logged_user = os.getlogin()
    workin_dir = os.getcwd()

    print(f'OS Name: {os_name}')
    print(f'Logged User: {logged_user}')
    print(f'Working Directory: {workin_dir}')


if __name__ == '__main__':
    os_info()
