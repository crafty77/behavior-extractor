from peewee import PostgresqlDatabase
import yaml


def read_db_config(conf_file):
    with open(conf_file, 'r') as conf_f:
        conf = yaml.load(conf_f)
    return conf


def get_db_connection(db='', user='', passwd='', host='', port='', autocommit=True):
    db = PostgresqlDatabase(
        db,  # databse name
        user=user,
        password=passwd,
        host=host,
        port=port
        )
    return db


# Read in the DB Conf file and establish necessary conf dictionaries
db_conf = read_db_config('conf/db_config.yml')
dwh_db_conf = db_conf['dwh_db']
event_db_conf = db_conf['event_db']

# Make database connections for use in other modules like this
    """
    import dp_etl.databse as db
    db.dwh_db = get_db_connection(**dwh_db_conf)
    db.event_db = get_db_connection(**event_db_conf)
    """

