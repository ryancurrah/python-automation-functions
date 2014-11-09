from django.conf import settings
from automation_functions.automation import Automation
from .email_notifications import new_tablespace_user_and_password


def setup_oracle_table_and_user(tablespace_object):
    """
    :param tablespace_object:
    :return: Status (Bool) and Message (String)

    Installs the oracle tablespace and user
    """
    ip_address = tablespace_object.virtual_machine_build.ip_address()
    tablespace_name = tablespace_object.tablespace_name
    user_object = tablespace_object.created_by
    hostname = tablespace_object.virtual_machine_build.get_hostname()

    if not 'None' in ip_address:
        a = Automation(hostname=ip_address,
                       username=settings.DEV_CLOUD_USERNAME,
                       private_key_file=settings.DEV_CLOUD_KEY)

        status, message = a.remote_copy_files(files=[settings.CLOUD_ORACLE_CREATE_TS_SQL_FILEPATH,
                                                     settings.CLOUD_ORACLE_CREATE_USER_SQL_FILEPATH],
                                              destination_directory='/tmp/')
        if not status:
            return {'status': False, 'message': message}

        ssh_status, message, status = a.move_files(files=['/tmp/{0}'.format(settings.CLOUD_ORACLE_CREATE_TS_SQL_FILENAME),
                                                          '/tmp/{0}'.format(settings.CLOUD_ORACLE_CREATE_USER_SQL_FILENAME)],
                                                   destination_directory='/home/oracle')
        if not ssh_status or not status:
            return {'status': False, 'message': message}

        ssh_status, message, status = a.change_file_ownership(chown_username='oracle',
                                                              files=['/home/oracle/{0}'.format(settings.CLOUD_ORACLE_CREATE_TS_SQL_FILENAME),
                                                                     '/home/oracle/{0}'.format(settings.CLOUD_ORACLE_CREATE_USER_SQL_FILENAME)])
        if not ssh_status or not status:
            return {'status': False, 'message': message}

        ssh_status, message, status = a.execute_oracle_sql_script(script_file='/home/oracle/{0}'.format(settings.CLOUD_ORACLE_CREATE_TS_SQL_FILENAME),
                                                                  script_parameters=['{0}'.format(tablespace_name)])
        if not ssh_status or not status:
            return {'status': False, 'message': message}

        password = 'temp123'

        ssh_status, message, status = a.execute_oracle_sql_script(script_file='/home/oracle/{0}'.format(settings.CLOUD_ORACLE_CREATE_USER_SQL_FILENAME),
                                                                  script_parameters=['{0}'.format(tablespace_name),
                                                                                     '{0}'.format(tablespace_name),
                                                                                     password])

        if ssh_status and status:
            # Send oracle tablespace name, user, and password to user
            new_tablespace_user_and_password(user_object,
                                             hostname,
                                             tablespace_name,
                                             tablespace_name,
                                             password)
            return {'status': True, 'message': u"Successfully created new Tablespace name and user. "
                                               u"An email has been sent with the details."}
        else:
            return {'status': False, 'message': message}
    else:
        message = u"Unable to retrieve ip address from django. Cannot create Tablespace."
        return {'status': False, 'message': message}