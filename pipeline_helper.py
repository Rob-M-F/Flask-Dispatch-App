from sqlalchemy import create_engine
from string import punctuation
import urllib.parse
import keyring
import secrets
import json


class PipelineHelper:
    @staticmethod
    def get_keyring_data(data_field, service_id=__name__, generate_if_none=False):
        """ Retrieve Environment Variable Data

        :param data_field: Environment Variable to retrieve
        :param service_id: Service Name under which to seek the variable
        :param generate_if_none: Flag setting whether to generate a random 64 byte value if the variable is None.
        :return: The environment variable requested
        """
        result_data = keyring.get_password(service_name=service_id, username=data_field)
        if generate_if_none and result_data is None:
            new_token = secrets.token_bytes(nbytes=64)
            PipelineHelper.set_keyring_data(service_id=service_id, data_field=data_field, password=new_token,
                                            generate_password=False)
            return new_token
        else:
            return result_data

    @staticmethod
    def set_keyring_data(service_id=__name__, data_field='SECRET', password=None, generate_password=False):
        """ Set an Environment Variable for the Service Name

        :param service_id: Service Name for the variable
        :param data_field: Environment Variable to set
        :param password: Data to store in the Environment Variable, none if random generation is preferred
        :param generate_password: Flag setting whether to generate a random 64 byte value.
        :return: None
        """
        if password is None and generate_password:
            password = secrets.token_urlsafe(nbytes=64)
        keyring.set_password(service_name=service_id, username=data_field, password=password)

    @staticmethod
    def get_engine(service_label):
        """ Gather database connection information from Environment Variables.

        :param service_label: Service ID for Environment Variable recall.
        :return: Database connection JSON
        """
        language = keyring.get_password(service_name=service_label, username='LANGUAGE')
        driver = keyring.get_password(service_name=service_label, username='DRIVER')
        username = keyring.get_password(service_name=service_label, username='USER')
        raw_password = keyring.get_password(service_name=service_label, username='PASSWORD')
        password = urllib.parse.quote_plus(raw_password)
        host = keyring.get_password(service_name=service_label, username='HOST')
        port = keyring.get_password(service_name=service_label, username='PORT')
        database = keyring.get_password(service_name=service_label, username='DATABASE')
        return create_engine(f'{language}+{driver}://{username}:{password}@{host}:{port}/{database}',
                             convert_unicode=True)

    @staticmethod
    def set_database(service_label, dataset):
        """ Change database connection configuration to match provided information.

        :param service_label: Service ID for Environment Variable declaration.
        :param dataset: DICT of values for the fields required to create an SQLALCHEMY Engine.
        :return: String indicating the new database information.
        """

        for key in dataset:
            PipelineHelper.set_keyring_data(service_id=service_label, data_field=key, password=dataset[key])

        return f"Database configuration updated: {dataset['LANGUAGE']}+{dataset['DRIVER']}://" \
               f"{dataset['USERNAME']}@{dataset['HOST']}:{dataset['PORT']}.{dataset['DATABASE']}"

    @staticmethod
    def read_pipeline_config(recurse=True):
        """ Read existing pipeline config file, or create it and read it.

        :param recurse: Indicator on whether the function should allow recursion.
        :return: Pipeline configuration data.
        """
        pipeline_config = {}
        file_error = False
        new_file = False

        try:
            with open("config/pipeline.config", 'r') as infile:
                data = json.load(infile)
            for key in data:
                pipeline_config[key] = data[key]
            message = pipeline_config
            PipelineHelper.overwrite_pipeline_config(pipeline_config)
        except IOError as e:
            file_error = True
            message = f"Cannot open pipeline.config. <{e}>"

        if recurse and file_error:
            new_file = False
            try:
                with open("config/pipeline.config", 'x') as _:
                    new_file = True
            except IOError as e:
                file_error = True
                message = f"{message}\nCannot create pipeline.config <{e}>"

        try:
            if new_file:
                _ = PipelineHelper.overwrite_pipeline_config()
                return PipelineHelper.read_pipeline_config(recurse=False)
        except IOError as e:
            file_error = True
            message = f"{message}\nCannot write to pipeline.config <{e}>"

        if file_error:
            raise IOError(message)
        return message

    @staticmethod
    def overwrite_pipeline_config(data_dict=None):
        file_error = False
        if data_dict is None:
            data_dict = dict()

        data_dict['SERVICE_LABEL'] = data_dict.get('SERVICE_LABEL', 'TASK_PIPELINE')
        data_dict['DEBUG'] = data_dict.get('DEBUG', False)
        data_dict['HOST'] = data_dict.get('HOST', 'localhost')
        data_dict['PORT'] = data_dict.get('PORT', 3306)
        data_dict['LANGUAGE'] = data_dict.get('LANGUAGE', 'mariadb')
        data_dict['DRIVER'] = data_dict.get('DRIVER', 'pymysql')
        data_dict['USER'] = data_dict.get('USER', 'pipeline_connect')
        data_dict['DATABASE'] = data_dict.get('DATABASE', 'pipeline_data')
        data_dict['PASSWORD'] = "********"

        try:
            with open("config/pipeline.config", 'w') as outfile:
                outfile.write(json.dumps(data_dict))
        except IOError:
            file_error = True
        return file_error

    @staticmethod
    def initialize_keyring(recurse=True):
        pipeline_config = PipelineHelper.read_pipeline_config(recurse=recurse)
        if 'PASSWORD' in pipeline_config:
            if pipeline_config['PASSWORD'] != "********":
                PipelineHelper.set_keyring_data(pipeline_config['SERVICE_LABEL'],
                                                "PASSWORD",
                                                pipeline_config["PASSWORD"])
        if 'SERVICE_LABEL' in pipeline_config:
            for key in pipeline_config:
                if key != 'SERVICE_LABEL':
                    if PipelineHelper.get_keyring_data(key, pipeline_config['SERVICE_LABEL'], False) is None:
                        PipelineHelper.set_keyring_data(pipeline_config['SERVICE_LABEL'], key, pipeline_config[key])
        return pipeline_config

    @staticmethod
    def get_demo_data_from_file(file_name, display=False):
        with open(f'data_files/{file_name}.json', 'r') as infile:
            demo_data = json.load(infile)

        if display:
            for key in demo_data:
                print(f'{key}:')
                for identity in demo_data[key]:
                    print(f'\t\t{identity}: {demo_data[key][identity]}')
        else:
            return demo_data

    @staticmethod
    def write_demo_data_to_file(data_tables, file_name):
        data_dict = dict()
        for key in data_tables:
            data_dict[key] = {instance.id: instance.as_dict() for instance in data_tables[key]}

        with open(f'data_files/{file_name}.json', 'w') as outfile:
            json.dump(data_dict, outfile)

    @staticmethod
    def sanitize_text(text=""):
        return text.translate(str.maketrans('', '', punctuation))
