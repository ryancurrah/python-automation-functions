from bash_client import bash_client
from ssh_client.ssh_client import SshClient
from datetime import datetime
import os.path
import re


class Automation(SshClient):

    def __init__(self, *args, **kwargs):
        super(Automation, self).__init__(*args, **kwargs)
        # Set timeouts and sleeps in seconds
        self.connection_timeout = 60
        self.command_timeout = 60
        self.command_sleep = 3

    def remote_copy_files(self, files, destination_directory):
        """
        :param files: tuple or list of files with there directory paths to be copied over to the destination server (String)
        :param destination_directory: the directory the files will be copied to on the destination server (String)
        :return: Tuple of command execution status (Bool), Result Message (String)
        """
        if not isinstance(destination_directory, (str, basestring, unicode)):
            raise TypeError('destination_directory input variable is not a string or unicode type.')

        for index, file_name in enumerate(files):
            if not os.path.isfile(file_name):
                raise IOError('files input variable at index {0} value {1} does not point to a valid file. '
                              'The location or filename maybe incorrect.'.format(index, file_name))

        command = ['scp',
                   '-o',
                   'UserKnownHostsFile=/dev/null',
                   '-o',
                   'StrictHostKeyChecking=no',
                   '-i',
                   '{SSH_KEY}'.format(SSH_KEY=self.private_key_file),
                   '{USERNAME}@{HOSTNAME}:{DESTINATION}'.format(USERNAME=self.username,
                                                                HOSTNAME=self.hostname,
                                                                DESTINATION=destination_directory)]

        # Insert files into command after ssh_key
        for file in files:
            command.insert(7, file)

        stdout, stderr, returncode = bash_client.execute_command(command=command,
                                                                 command_timeout=self.command_timeout,
                                                                 command_sleep=self.command_sleep)

        bash_client.log_execute_command_results(command=command,
                                                datetime_executed=datetime.now(),
                                                action='SCP Files to remote server',
                                                returncode=returncode,
                                                stdout=stdout,
                                                stderr=stderr,
                                                log_directory='/tmp/',
                                                server=self.hostname)

        if returncode == 0:
            command_execution_status = True
            result_message = u"Successfully scp files to {HOSTNAME}.".format(HOSTNAME=self.hostname)
        else:
            command_execution_status = False
            result_message = u"Failed to scp files to {HOSTNAME}.".format(HOSTNAME=self.hostname)
        return command_execution_status, result_message

    def execute_shell_script(self, script_file_name, su_as='root'):
        """
        :param script_file_name: The full path and name of the script (String)
        :param su_as: The name of the user su as to run/execute the command (String)
        :return: Tuple of command execution status (Bool), Result Message (String)
        """
        if not isinstance(script_file_name, (str, basestring, unicode)):
            raise TypeError('destination_directory input variable is not a string or unicode type.')

        script_file_name = script_file_name.rstrip('/')

        status, file_exists = self._check_if_file_or_directory_exists(script_file_name)
        if status and not file_exists:
            message = u"The script '{SCRIPT}' on {HOSTNAME} " \
                      u"does not exist, cannot execute it.".format(SCRIPT=script_file_name,
                                                                   HOSTNAME=self.hostname)
            return True, message, False
        elif not status:
            message = u"Unable to determine if the script '{SCRIPT}' on {HOSTNAME} " \
                      u"exists, cannot execute it.".format(SCRIPT=script_file_name,
                                                           HOSTNAME=self.hostname)
            return False, message, None

        sudo_su = self._sudo_su_command(su_as)
        command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                   'sh {SCRIPT}'.format(SCRIPT=script_file_name)]

        # execute command
        result = self.execute_remote_command(command)
        return self._parse_ssh_client_result(result,
                                             u"Successfully executed script {0}.".format(script_file_name),
                                             u"Failed to execute script {0}.".format(script_file_name))

    def move_files(self, files, destination_directory, su_as='root'):
        """
        :param files: tuple or list of the full path and name of the files to move (String)
        :param destination_directory: the directory the files will be copied to on the destination server (String)
        :param su_as: The name of the user su as to run/execute the command (String)
        :return: A tuple of ssh connection status (Bool), Result Message (String), command execution status (Bool)

        Move a list of files from directory to another; Does not move directories.
        """
        if not isinstance(destination_directory, (str, basestring, unicode)):
            raise TypeError('destination_directory input variable is not a string or unicode type.')

        for index, file_name in enumerate(files):
            status, file_exists = self._check_if_file_or_directory_exists(file_name)
            if status and not file_exists:
                message = u"The source file '{SOURCE_FILE}' on {HOSTNAME} " \
                          u"does not exist, cannot move it.".format(SOURCE_FILE=file_name,
                                                                    HOSTNAME=self.hostname)
                return True, message, False
            elif not status:
                message = u"Unable to determine if the source file '{SOURCE_FILE}' on {HOSTNAME} " \
                          u"exists, cannot move it.".format(SOURCE_FILE=file_name,
                                                            HOSTNAME=self.hostname)
                return False, message, None

        # build up command
        source_files_with_path = self._flatten_list_for_command(files)

        sudo_su = self._sudo_su_command(su_as)
        command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                   'mv {SOURCE_FILES} {DESTINATION_DIRECTORY}'.format(SOURCE_FILES=source_files_with_path,
                                                                      DESTINATION_DIRECTORY=destination_directory)]

        # execute command
        result = self.execute_remote_command(command)
        return self._parse_ssh_client_result(result,
                                             u"Successfully moved files to folder {0}.".format(destination_directory),
                                             u"Failed to moved files to folder {0}.".format(destination_directory))

    def mount_file_system(self, source_file_system, mount_directory, su_as='root'):
        """
        :param source_file_system: The mount source either a network location or local drive (String)
        :param mount_directory: The destination directory where the files system will be mounted to (String)
        :param su_as: The name of the user su as to run/execute the command (String)
        :return: A tuple of ssh connection status (Bool), Result Message (String), command execution status (Bool)
        """
        if not isinstance(source_file_system, (str, basestring, unicode)):
            raise TypeError('source_file_system input variable is not a string or unicode type.')
        if not isinstance(mount_directory, (str, basestring, unicode)):
            raise TypeError('mount_directory input variable is not a string or unicode type.')

        source_file_system = source_file_system.rstrip('/')
        mount_directory = mount_directory.rstrip('/')

        status, file_exists = self._check_if_file_or_directory_exists(mount_directory)
        if status and not file_exists:
            message = u"The mount directory '{MOUNT_DIR}' on {HOSTNAME} " \
                      u"does not exist, cannot mount to it.".format(MOUNT_DIR=mount_directory,
                                                                    HOSTNAME=self.hostname)
            return True, message, False
        elif not status:
            message = u"Unable to determine if the mount directory '{MOUNT_DIR}' on {HOSTNAME} " \
                      u"exists, cannot mount to it.".format(MOUNT_DIR=mount_directory,
                                                            HOSTNAME=self.hostname)
            return False, message, None

        sudo_su = self._sudo_su_command(su_as)
        command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                   'mount {SOURCE_FS} {MOUNT_DIR}'.format(SOURCE_FS=source_file_system,
                                                          MOUNT_DIR=mount_directory)]

        result = self.execute_remote_command(command)
        return self._parse_ssh_client_result(result,
                                             success_message=u"Successfully mounted file system: '{0}' to the mount "
                                                             u"folder: '{1}'.".format(source_file_system,
                                                                                      mount_directory),
                                             error_message=u"Failed to mounted file system: '{0}' to the "
                                                           u"mount folder: '{1}'.".format(source_file_system,
                                                                                          mount_directory))

    def unmount_file_system(self, mount_directory, su_as='root'):
        """
        :param mount_directory: The destination directory where the files system will be mounted to (String)
        :param su_as: The name of the user su as to run/execute the command (String)
        :return: A tuple of ssh connection status (Bool), Result Message (String), command execution status (Bool)
        """
        if not isinstance(mount_directory, (str, basestring, unicode)):
            raise TypeError('mount_directory input variable is not a string or unicode type.')

        mount_directory = mount_directory.rstrip('/')

        status, file_exists = self._check_if_file_or_directory_exists(mount_directory)
        if status and not file_exists:
            message = u"The mount directory '{MOUNT_DIR}' on {HOSTNAME} " \
                      u"does not exist, cannot unmount it.".format(MOUNT_DIR=mount_directory,
                                                                   HOSTNAME=self.hostname)
            return True, message, False
        elif not status:
            message = u"Unable to determine if the mount directory '{MOUNT_DIR}' on {HOSTNAME} " \
                      u"exists, cannot unmount it.".format(MOUNT_DIR=mount_directory,
                                                           HOSTNAME=self.hostname)
            return False, message, None

        sudo_su = self._sudo_su_command(su_as)
        command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                   'umount -f {MOUNT_DIR}'.format(MOUNT_DIR=mount_directory)]

        result = self.execute_remote_command(command)
        return self._parse_ssh_client_result(result,
                                             success_message=u"Successfully unmounted file system from mount "
                                                             u"folder: '{0}'.".format(mount_directory),
                                             error_message=u"Failed to unmounted file system mount "
                                                           u"folder: '{0}'.".format(mount_directory))

    def change_file_ownership(self, chown_username, files, chown_groupname='', recursive=False, su_as='root'):
        """
        :param chown_username: The username to change the ownership to (String)
        :param chown_groupname: The groupname to change the ownership to optional (String)
        :param files: A tuple or list of file names as strings to change the ownership on (Tuple or List)
        :param recursive: change ownership recursively if True, do not change ownership recursively if False (Bool)
        :param su_as: The name of the user su as to run/execute the command (String)
        :return: A tuple of ssh connection status (Bool), Result Message (String), command execution status (Bool)

        Change the owner and group of a set of files
        """
        if not isinstance(chown_username, (str, basestring, unicode)):
            raise TypeError('chown_username input variable is not a string or unicode type.')
        if not isinstance(chown_groupname, (str, basestring, unicode)):
            raise TypeError('chown_groupname input variable is not a string or unicode type.')
        if not isinstance(files, (tuple, list)):
            raise TypeError('files input variable is not a list or tuple.')

        for index, file_name in enumerate(files):
            status, file_exists = self._check_if_file_or_directory_exists(file_name)
            if status and not file_exists:
                message = u"The file '{FILE}' on {HOSTNAME} " \
                          u"does not exist, cannot change ownership on it.".format(FILE=file_name,
                                                                                   HOSTNAME=self.hostname)
                return True, message, False
            elif not status:
                message = u"Unable to determine if the file '{FILE}' on {HOSTNAME} " \
                          u"exists, cannot change ownership on it.".format(FILE=file_name,
                                                                           HOSTNAME=self.hostname)
                return False, message, None

        # build up command
        files_flattened = self._flatten_list_for_command(files)

        # Get sudo su command
        sudo_su = self._sudo_su_command(su_as)

        # Set chown command options
        if recursive:
            chown_command = 'chown -R'
        else:
            chown_command = 'chown'

        if not chown_groupname:
            command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                       '{CHOWN} {USERNAME} {SOURCE_FILES}'.format(CHOWN=chown_command,
                                                                  USERNAME=chown_username,
                                                                  SOURCE_FILES=files_flattened)]
        else:
            command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                       '{CHOWN} {USERNAME}.{GROUPNAME} {SOURCE_FILES}'.format(CHOWN=chown_command,
                                                                              USERNAME=chown_username,
                                                                              GROUPNAME=chown_groupname,
                                                                              SOURCE_FILES=files_flattened)]

        # execute command
        result = self.execute_remote_command(command)
        return self._parse_ssh_client_result(result,
                                             success_message=u"Successfully changed ownership "
                                                             u"on files {0} to the username {1}.".format(chown_username,
                                                                                                         files_flattened),
                                             error_message=u"Failed to change ownership on "
                                                           u"files {0} to the username {1}.".format(files_flattened,
                                                                                                    chown_username))

    def create_directory(self, folder_path_name, su_as='root'):
        """
        :param folder_path_name: The full path of the folder with the folder name (String)
        :param su_as: The name of the user su as to run/execute the command (String)
        :return: A tuple of ssh connection status (Bool), Result Message (String), command execution status (Bool)

        Creates directories with -p flag on; No error if folder exists already, makes parent directories as needed.
        """
        if not isinstance(folder_path_name, (str, basestring, unicode)):
            raise TypeError('folder_path_name input variable is not a string or unicode type.')

        folder_path_name = folder_path_name.rstrip('/')

        sudo_su = self._sudo_su_command(su_as)
        command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                   'mkdir -p {DIRECTORY}'.format(DIRECTORY=folder_path_name)]

        result = self.execute_remote_command(command)
        return self._parse_ssh_client_result(result,
                                             success_message=u"Successfully created "
                                                             u"folder {0}.".format(folder_path_name),
                                             error_message=u"Failed to create folder {0}.".format(folder_path_name))

    def execute_oracle_sql_script(self, script_file, script_parameters, su_as='oracle'):
        """
        :param script_file: A string of the full path and file name of the sql script (String)
        :param script_parameters: A list or tuple of the parameters for the script (String)
        :param su_as: The name of the user su as to run/execute the command (String)
        :return: A tuple of ssh connection status (Bool), Result Message (String), command execution status (Bool)

        Executes an SQL script as the oracle user
        """
        if not isinstance(script_file, (str, basestring, unicode)):
            raise TypeError('script_file input variable is not a string or unicode type.')
        if not isinstance(script_parameters, (list, tuple)):
            raise TypeError('script_parameters input variable is not a list or tuple.')

        status, file_exists = self._check_if_file_or_directory_exists(script_file)
        if status and not file_exists:
            message = u"The script file '{SCRIPT_FILE}' on {HOSTNAME} " \
                      u"does not exist, cannot execute it.".format(SCRIPT_FILE=script_file,
                                                                   HOSTNAME=self.hostname)
            return True, message, False
        elif not status:
            message = u"Unable to determine if the script file '{SCRIPT_FILE}' on {HOSTNAME} " \
                      u"exists, cannot execute it.".format(SCRIPT_FILE=script_file,
                                                           HOSTNAME=self.hostname)
            return False, message, None

        # build up command
        script_parameters_flattened = self._flatten_list_for_command(script_parameters)

        sudo_su = self._sudo_su_command(su_as)
        if script_parameters:
            command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                       'sqlplus / as sysdba @{SCRIPT_FILE} {PARAMETERS}'.format(SCRIPT_FILE=script_file,
                                                                                PARAMETERS=script_parameters_flattened)]
        else:
            command = ['{SUDO_SU}'.format(SUDO_SU=sudo_su),
                       'sqlplus / as sysdba @{SCRIPT_FILE}'.format(SCRIPT_FILE=script_file)]

        # execute command
        result = self.execute_remote_command(command)

        if result.get('status') and result.get('exit_code') == 0:
            if re.search('ORA-01543', result.get('stdout')):
                return True, u"Error ORA-01543: Oracle Tablespace already exists. Unable to create Tablespace.", False
            if re.search('ORA-01920', result.get('stdout')):
                return True, u"Error ORA-01920: Oracle User already exists. Unable to create user.", False
            if re.search('ORA-00959', result.get('stdout')):
                return True, u"Error ORA-00959: Oracle Tablespace does not exist. Unable to create user.", False
            if re.search('ORA-\d{5}', result.get('stdout')):
                errors = re.findall('ORA-\d{5}', result.get('stdout'))
                if len(errors) > 1:
                    error_message = u"Oracle errors occurred {0}."
                else:
                    error_message = u"Oracle error occurred {0}."
                return True, error_message.format(', '.join(errors)), False
        return self._parse_ssh_client_result(result,
                                             success_message=u"Successfully executed sql script "
                                                             u"{0}.".format(script_file),
                                             error_message=u"Failed to execute script "
                                                           u"{0}.".format(script_file))

    @staticmethod
    def _sudo_su_command(su_as):
        """
        :param su_as: The user name to su as (String)
        :return: The sudo su command with the requested user (String)
        """
        if not isinstance(su_as, (str, basestring, unicode)):
            raise TypeError('su_as input variable is not a string or unicode type.')
        su_as = su_as.lower()
        if re.match('^root$', su_as):
            return 'sudo su -'
        else:
            return 'sudo su - {SU_AS}'.format(SU_AS=su_as)

    @staticmethod
    def _flatten_list_for_command(values):
        """
        :param values: List or tuple of strings to flatten
        :return: A tuple of ssh connection status (Bool), file exists or not (Bool)

        If a list of values is needed in a command it will flatten the list for you
        """
        return ' '.join(values)

    def _check_if_file_or_directory_exists(self, file_path):
        """
        :param file_path: File name and/or path on the remote system where to check if the file or directory exists
        :return: A tuple of command_execution_status (Bool), if file or directory exists (Bool)
        """
        command = ['sudo su -',
                   '[ -f {FILE_PATH} ] && echo True || [ -d {FILE_PATH} ] && echo True || echo False'.format(FILE_PATH=file_path)]

        # execute command
        result = self.execute_remote_command(command)

        try:
            if result.get('status'):
                stdout = result.get('stdout').split('\n')[1]
                if 'True' in stdout:
                    command_execution_status = True
                    file_exists = True
                elif 'False' in stdout:
                    command_execution_status = True
                    file_exists = False
                else:
                    command_execution_status = False
                    file_exists = None
            else:
                command_execution_status = False
                file_exists = None
        except IndexError:
            command_execution_status = False
            file_exists = None
        except AttributeError:
            command_execution_status = False
            file_exists = None
        return command_execution_status, file_exists

    @staticmethod
    def _parse_ssh_client_result(result,
                                 success_message=u"Successfully executed.",
                                 error_message=u"Failed to execute."):
        """
        :param result: the result from ssh client
        :param success_message: message for success
        :param error_message:  message for failure
        :return: Parses the returned ssh client result into
        a tuple of ssh connection status (Bool), Result Message (String),
        command execution status (Bool)
        """

        # Get return values and return data
        ssh_connection_status = result.get('status')

        if result.get('exit_code') == 0:
            command_execution_status = True
        else:
            command_execution_status = False

        if not ssh_connection_status:
            message = u"{0}; {1}".format(result.get('msg'), error_message)
        elif not command_execution_status:
            message = error_message
        else:
            message = success_message
        return ssh_connection_status, message, command_execution_status