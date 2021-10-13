from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from pipeline_helper import PipelineHelper


pipeline_config = PipelineHelper.read_pipeline_config(recurse=True)
engine = PipelineHelper.get_engine(service_label=pipeline_config['SERVICE_LABEL'])
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


def init_db():
    # import all modules here that might define models so that they will be registered properly on the metadata.
    # Otherwise you will have to import them first before calling init_db()
    import pipeline_model
    Base.metadata.create_all(bind=engine)
