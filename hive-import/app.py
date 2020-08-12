import paramiko
import json 
import sys
from config import Config
from scp import SCPClient

class Connection(object):
    # This class is for estabilishing SSH connection

    def __init__(self, host, username, password, port=22):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.username = username
        self.password = password
        self.port = port
        self.host = host
        self.ssh_connection = ssh

    def connect(self):
        self.ssh_connection.connect(self.host, self.port, self.username, self.password)
    
    def execute(self, command):
        return self.ssh_connection.exec_command(command)

    def get_transport(self):
        return self.ssh_connection.get_transport()

    def close(self):
        self.ssh_connection.close()

def construct_hive_import_command(atlas_home_location, remote_sources_file_location):
    print("INFO: Atlas home is set to: {0}".format(atlas_home_location))
    import_hive_location_command = "{0}/hook-bin/import-hive.sh".format(atlas_home_location.rstrip("/"))
    return "{0} -f {1}".format(import_hive_location_command, remote_sources_file_location)
    # return "{0} -t test:jeff_table".format(import_hive_location_command)

def load_config(config_file_path):
    with open(config_file_path, 'r') as config:
        config = json.load(config)
    return config

def scp_file_to_node(connection, local_file_location, remote_file_location):
    with SCPClient(connection.get_transport()) as scp:
        scp.put(local_file_location, remote_file_location)
        scp.close()

def get_renew_kerberos_ticket_command(keytab_location=None):
    if keytab_location is None:
        print("INFO: Headless keytab is being used")
        return "kinit"
    else:
        print("INFO: Renewing Kerberos ticket using this keytab {}".format(keytab_location))
        return "kinit -kt {0}".format(keytab_location)
    
if __name__ == "__main__":

    config_dict = load_config("../config.json")
    print("INFO: Loaded config file")

    host = config_dict[Config.HOSTNAME]
    port = config_dict[Config.PORT]
    username = config_dict[Config.USERNAME]
    password = config_dict[Config.PASSWORD]

    # print(config_dict)

    ssh_connection = Connection(host, username, password, port)
    ssh_connection.connect()
    print("INFO: SSH connection established with remote host {0} for user {1}".format(host, username))

    print("INFO: Copying {0} file to remote node".format(config_dict[Config.SOURCES_FILE_KEY]))
    scp_file_to_node(ssh_connection, config_dict[Config.SOURCES_FILE_KEY], config_dict[Config.TEMP_WRITE_PATH])
    print("INFO: File {0} has been copied successfully to remote node in this location: {1}".format(config_dict[Config.SOURCES_FILE_KEY], config_dict[Config.TEMP_WRITE_PATH]))

    kerberos_enabled_flag = config_dict["kerberos_flag"]
    kerberos_headless_keytab_flag = config_dict["keytab_headless"]
    keytab_location = config_dict["keytab_location"]

    command = construct_hive_import_command(config_dict["atlas_home_location"], config_dict[Config.TEMP_WRITE_PATH])
    print("INFO: Import hive location to be executed on remote node: {0}".format(command))

    if kerberos_enabled_flag:
        if config_dict["keytab_headless"]:
            kerberos_renew_command = get_renew_kerberos_ticket_command()
        else:
            kerberos_renew_command = get_renew_kerberos_ticket_command(config_dict["keytab_location"])
        # command = "{0} \n cat {1}".format(kerberos_renew_command, config_dict[Config.TEMP_WRITE_PATH])
        command = "{0} \n {1}".format(kerberos_renew_command, command)
    
    print("INFO: Executing following command: {0}".format(command))
    stdin, stdout, stderr = ssh_connection.execute(command)

    atlas_user = config_dict[Config.ATLAS_USERNAME]
    atlas_password = config_dict[Config.ATLAS_PASSWORD]

    if kerberos_headless_keytab_flag:
        stdin.write(password + "\n" + atlas_user + "\n" + atlas_password + "\n")
    else:
        stdin.write(atlas_user + "\n" + atlas_password + "\n")
    stdin.flush()

    while True:
        line = stdout.readline()
        if not line:
            break
        print(line)

    return_code = stdout.channel.recv_exit_status()
    ssh_connection.close()

    if return_code == 0:
        print("INFO: Script ran successfully as exit code is 0")
        print("Exiting...")
        sys.exit(0)
    else:
        print("ERROR: Issue occured as exit code is non-zero, exiting...")
        sys.exit(1)
    