import pandas as pd
import psycopg2
import yaml
from psycopg2 import Error
from typing import Tuple, List


LOG_LINE_LENGTH = 128

CONF_FOLDER = 'configs'
DATABASE_CONFIGS = 'database_configs'

OUTPUT_FOLDER = 'output_folder'
OUTPUT_FILE = 'task_3_output'


class ConfigsExtractor:
    @staticmethod
    def postgres_conf_extractor(conf_folder, database_configs) -> \
            Tuple[str, str, str, str, str]:
        with open('{conf_folder}/{database_configs}.yaml'.format(
                conf_folder=conf_folder,
                database_configs=database_configs
        )) as file:
            conf = yaml.load(file, yaml.Loader)

        postgres_user = conf['postgres']['user']
        postgres_password = conf['postgres']['password']
        postgres_host = conf['postgres']['host']
        postgres_port = conf['postgres']['port']
        postgres_database_name = conf['postgres']['database_name']

        return (postgres_user, postgres_password, postgres_host,
                postgres_port, postgres_database_name)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args,
                                                                 **kwargs)
        return cls._instances[cls]


class DatabaseSelector:
    def __init__(self,
                 connection,
                 cursor):
        self.connection = connection
        self.cursor = cursor

    def execute_query(self, query: str) -> List[str]:

        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            return records

        except (Exception, Error) as error:
            raise error

        finally:
            if self.connection:
                self.cursor.close()
                self.connection.close()


class DBConnector:
    __metaclass__ = Singleton

    def __init__(self,
                 user,
                 password,
                 host,
                 port,
                 database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def create_connection(self):
        connection = psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
        )
        return connection

    @staticmethod
    def create_cursor(connection):
        return connection.cursor()


def main():
    configs_extractor = ConfigsExtractor()
    user, password, host, port, database = \
        configs_extractor.postgres_conf_extractor(
            conf_folder=CONF_FOLDER,
            database_configs=DATABASE_CONFIGS)

    db_connector = DBConnector(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database

    )
    connection = db_connector.create_connection()
    cursor = db_connector.create_cursor(connection)
    database_selector = DatabaseSelector(connection=connection,
                                         cursor=cursor)
    try:
        e_sports_bets_query = \
            '''WITH general_tab AS (
                SELECT bet_id, player_id, accepted_odd   
                FROM bets b 
                JOIN events e ON e.event_id = b.event_id 
                WHERE sport = 'E-Sports'
                AND event_stage = 'Prematch'
                AND create_time >= '2022-03-14 12:00:00.000'
                AND amount >= 10
                AND settlement_time <= '2022-03-15 12:00:00.000'
                AND settlement_time NOT IN ('') 
                AND settlement_time IS NOT NULL
                AND bet_type NOT IN ('System')
                AND is_free_bet = FALSE
                AND result IN ('Lose', 'Win')
                )
                SELECT DISTINCT (als_tab.player_id) 
                FROM (
                    SELECT bet_id, player_id  
                    FROM general_tab
                    WHERE accepted_odd >= 1.5
                    EXCEPT
                    SELECT bet_id, player_id  
                    FROM general_tab
                    WHERE accepted_odd < 1.5) als_tab '''
    except (Exception, Error) as error:
        raise error

    e_sports_result = database_selector.execute_query(e_sports_bets_query)
    result_df = pd.DataFrame({'players': e_sports_result})
    result_df.to_csv('{output_folder}/{output_file}.csv'.format(
        output_folder=OUTPUT_FOLDER,
        output_file=OUTPUT_FILE
    ))


if __name__ == '__main__':
    main()
