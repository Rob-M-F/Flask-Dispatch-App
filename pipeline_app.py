from flask import Flask
from flask_security import Security, SQLAlchemySessionUserDatastore, hash_password
from pipeline_helper import PipelineHelper
from pipeline_database import db_session, init_db
from pipeline_model import User, Role, Task
from pipeline_service import BrokerControl, TextTagger, TextParser, RankResources, GradeTask
from pubsub import pub

pipeline_config = PipelineHelper.read_pipeline_config(recurse=True)
app = Flask(__name__)
app.config['DEBUG'] = pipeline_config['DEBUG']

app.config['SECRET_KEY'] = PipelineHelper.get_keyring_data(service_id=pipeline_config['SERVICE_LABEL'],
                                                           data_field='SECRET', generate_if_none=True)
app.config['SECURITY_PASSWORD_SALT'] = PipelineHelper.get_keyring_data(service_id=pipeline_config['SERVICE_LABEL'],
                                                                       data_field='SALT', generate_if_none=True)
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {"pool_pre_ping": True}

user_datastore = SQLAlchemySessionUserDatastore(db_session, User, Role)
security = Security(app, user_datastore)


@app.before_first_request
def create_user():
    init_db()
    if not User.query.all():
        # user_datastore.create_role(name='admin')
        user_datastore.create_user(email='admin', password=hash_password('Larynx'))
        user_datastore.add_role_to_user(user='admin', role='admin')
    db_session.commit()


def check_task_statuses():
    for task in Task.query.filter(Task.ranked == 1).filter(Task.grade.is_(None)).filter(
            Task.ideal_resource.isnot(None)).all():
        app.ranker_broker.publish(message=task.id)

    for task in Task.query.filter(Task.tagged == 1).filter(Task.ranked == 0).all():
        app.tagger_broker.publish(message=task.id)

    for task in Task.query.filter(Task.parsed == 1).filter(Task.tagged == 0).all():
        app.parser_broker.publish(message=task.id)

    for task in Task.query.filter(Task.parsed == 0).all():
        app.input_broker.publish(message=task.id)

    return Task.query.filter(Task.ranked == 1).filter(Task.dispatched == 0).all()


app.input_broker = BrokerControl(publishing_broker=pub, publishing_topic="task_created")
app.parser_broker = BrokerControl(publishing_broker=pub, publishing_topic="task_parsed")
app.tagger_broker = BrokerControl(publishing_broker=pub, publishing_topic="task_tagged")
app.ranker_broker = BrokerControl(publishing_broker=pub, publishing_topic="task_ranked")
app.grader_broker = BrokerControl(publishing_broker=pub, publishing_topic="task_graded")
app.check_task_statuses = check_task_statuses

pipeline_parser = TextParser(publisher=app.parser_broker, database=db_session)
pub.subscribe(pipeline_parser.listener, app.input_broker.listener)
pipeline_tagger = TextTagger(publisher=app.tagger_broker, database=db_session)
pub.subscribe(pipeline_tagger.listener, app.parser_broker.listener)
pipeline_ranker = RankResources(publisher=app.ranker_broker, database=db_session)
pub.subscribe(pipeline_ranker.listener, app.tagger_broker.listener)
pipeline_grader = GradeTask(publisher=app.grader_broker, database=db_session)
pub.subscribe(pipeline_grader.listener, app.ranker_broker.listener)

if __name__ == '__main__':
    app.run()
