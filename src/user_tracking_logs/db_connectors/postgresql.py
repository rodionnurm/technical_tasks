from sqlalchemy import URL, create_engine, text
from sqlalchemy.orm import sessionmaker

class Postgres:
    def connector(self, database, user, password, host, port, logger):
        try:
            # create url
            url_object = URL.create(
                drivername='postgresql+psycopg2',
                username=user,
                password=password,
                host=host,
                database=database,
                port=port
            )

            # create session
            engine = create_engine(url_object)
            Session = sessionmaker(engine)
            connection = engine.connect()
            # check connection to db
            with Session() as session:
                result = session.execute(text('SELECT version()')).fetchall()
                logger.info(f'successfully connected to database: {result[0]}')

            return Session, connection

        except Exception as e:
            logger.error(f'failed to connect to database: {e}')
