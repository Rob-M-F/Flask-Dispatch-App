from pipeline_model import Company, CompanyTags, Contact, ContactTags, ContextTags, Resource, ResourceCompanies
from pipeline_model import ResourceTags, ResourceSites, Site, SiteTags, Tag, Task, TaskParsing, TaskResources, TaskTags
from pipeline_model import User
from nltk.tokenize import word_tokenize, sent_tokenize
from pipeline_app import db_session
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from pubsub import pub
import nltk
import json


class BrokerControl:
    def __init__(self, publishing_broker=None, publishing_topic=None,
                 logging_broker=None, logging_topic=None):
        self.listener = 'logging' if publishing_topic is None else publishing_topic
        self.log_listener = 'logging' if logging_topic is None else logging_topic
        self.publishing_broker = publishing_broker
        self.logging_broker = logging_broker

    def publish(self, message):
        if self.publishing_broker is not None:
            return self.publishing_broker.sendMessage(self.listener, msg=message)

    def log(self, message):
        if self.logging_broker is not None:
            return self.logging_broker.sendMessage(self.log_listener, msg=message)


class LanguageParser:
    stp_wd = set(stopwords.words('english') + [',', '.', ';', ':', '?', '-', '=', '+', '"', "'"])
    replacement_map = dict()
    association_map = dict()

    def __init__(self, engine=None):
        self.engine = engine

    def split_sentences(self, text):
        return sent_tokenize(text) if self.engine is None else self.engine.sent_tokenize(text)

    def extract_content(self, text, parse='tag'):
        if self.engine is None:
            raw_text = text.lower()
            stripped_text = self.strip_stopwords(raw_text)
            mapped_text = self.apply_mapping(stripped_text)
            stem = PorterStemmer()
            phrase_content = word_tokenize(mapped_text)
            if parse == 'tag':
                return nltk.pos_tag(phrase_content)
            else:
                return [stem.stem(word) for word in phrase_content]
        else:
            return self.engine(text)

    def strip_stopwords(self, text):
        stripped_list = []
        for word in text.split(" "):
            if word not in self.stp_wd:
                stripped_list.append(word)
        return " ".join(stripped_list)

    def load_mapping(self, file_name="synonym_map.json"):
        with open(file_name) as text_file:
            reader = text_file.read()
            mapping = json.loads(reader)
        self.replacement_map = mapping['replacement']
        self.association_map = mapping['association']

    def apply_mapping(self, text):
        mapped_list = []
        associations = []
        for word in text.split(" "):
            if word in self.replacement_map:
                mapped_list.append(self.replacement_map[word])
            else:
                mapped_list.append(word)
            if word in self.association_map:
                associations.append(self.association_map[word])
        return " ".join(mapped_list + associations)


class PipelineService:
    def __init__(self, publisher, database):
        self.publisher = publisher
        self.database = database

    def listener(self, msg):
        task = self.process_task(msg)
        task = self.perform_service(task)
        self.database.commit()
        self.publisher.log(message=self.logging_message(task))
        self.publisher.publish(message=self.publish_message(task))

    @staticmethod
    def process_task(task_id):
        task = Task.query.filter(Task.id == task_id).first()
        return task

    @staticmethod
    def perform_service(task):
        return task

    @staticmethod
    def commit_message(task):
        return task

    @staticmethod
    def logging_message(task):
        return f"Logging service not implemented. {str(task)}"

    @staticmethod
    def publish_message(task):
        return task.id


class DataInput(PipelineService):
    def __init__(self, publisher, database, data_loader=None):
        self.loader = data_loader
        super().__init__(publisher=publisher, database=database)

    #  TODO Apply validation to task data
    def process_task(self, task_data):
        task = Task()
        if self.loader is None:
            task_dict = json.loads(task_data)
            task.site_id = self.process_site(task_dict.get(['site']))
            task.user_id = self.process_user(task_dict.get(['user']))
            task.authorizer_id = self.process_contact(task_dict.get(['authorizer']))
            task.company_id = self.process_company(task_dict.get(['company']))
            task.contact_id = self.process_contact(task_dict.get(['contact']))
            task.description = self.process_description(task_dict.get(['description']))
        else:
            task = self.loader(task_data)
        self.database.add(task)
        self.database.commit()
        return task

    @staticmethod
    def logger_message(task):
        return f"Task ({task.id}) loaded, resulting in: {str(task)}"

    @staticmethod
    def process_site(raw_site):
        site = Site.query.filter(Site.id == raw_site).first()
        return site.id

    @staticmethod
    def process_user(raw_user):
        user = User.query.filter(User.id == raw_user).first()
        return user.id

    @staticmethod
    def process_contact(raw_contact):
        contact = Contact.query.filter(Contact.id == raw_contact)
        return contact.id

    @staticmethod
    def process_company(raw_company):
        company = Company.query.filter(Company.id == raw_company)
        return company.id

    @staticmethod
    def process_description(raw_description):
        return raw_description


class TextParser(PipelineService):
    def __init__(self, publisher, database, engine=LanguageParser()):
        super().__init__(publisher=publisher, database=database)
        self.parser = engine

    def perform_service(self, task):
        parsing_contexts = self.parser.split_sentences(task.description)
        for c, context in enumerate(parsing_contexts):
            parsing_dict = dict()
            for word, part in self.parser.extract_content(context):
                parsing_dict[part] = parsing_dict.get(part, list()) + [word]
            task_parsing = TaskParsing(task_id=task.id, ordinal=c, parsed=json.dumps(parsing_dict))
            self.database.add(task_parsing)
        task.parsed = True
        self.database.commit()
        return task

    @staticmethod
    def logger_message(task):
        message = f"Parsed task description {task.description}, resulting in contexts: "
        return message + ", ".join(TaskParsing.query.filter(TaskParsing.task_id == task.id).all())


class TextTagger(PipelineService):
    class WeightedTag:
        def __init__(self, name=None, task_id=None, weight=None):
            self.name = name
            tag = Tag.query.filter(Tag.name == self.name).first()
            self.tag_id = tag.id if tag else None
            self.task_id = task_id
            self.weight = weight

        def __repr__(self):
            return f"{self.weight}"

        def context_tag(self):
            return ContextTags(task_id=self.task_id, tag_id=self.tag_id, name=self.name)

        def task_tag(self):
            return TaskTags(task_id=self.task_id, tag_id=self.tag_id, weight=self.weight)

        def __eq__(self, other):
            return self.tag_id == other.tag_id

        def __add__(self, other):
            return self.weight + other.weight

    def __init__(self, publisher, database, tagger=None):
        super().__init__(publisher=publisher, database=database)
        if tagger is None:
            self.tagger = self.extract_descriptors
        else:
            self.tagger = tagger

    def perform_service(self, task):
        task_tags = dict()
        for tag_group, group_weight in (
                (TaskTags.query.filter(TaskTags.task_id == task.id), 8.0),
                (ContactTags.query.filter(ContactTags.contact_id == task.authorizer_id), 1.0),
                (CompanyTags.query.filter(CompanyTags.company_id == task.company_id), 2.0),
                (SiteTags.query.filter(SiteTags.site_id == task.site_id), 3.0),
                (self.generate_tags(task), 4.0)):
            for tag_association in tag_group:
                tag = Tag.query.filter(Tag.id == tag_association.tag_id).first()
                if tag is not None:
                    task_tag = task_tags.get(tag.id, self.WeightedTag(task_id=task.id, name=tag.name, weight=0.0))

                    task_tag.weight += float(tag_association.weight) * group_weight
                    task_tags[tag_association.tag_id] = task_tag
        for key in task_tags:
            weight_tag = task_tags[key]
            weight_tag.weight = weight_tag.weight / 18.0
            current = TaskTags.query.filter(TaskTags.task_id == weight_tag.task_id).first()
            if current:
                current.weight = task_tags[key].weight
            else:
                current = TaskTags(task_id=task.id, tag_id=weight_tag.tag_id, weight=weight_tag.weight)
                self.database.add(current)
        task.tagged = True
        self.database.commit()
        return task

    def generate_tags(self, task):
        content_tags = dict()
        for context in TaskParsing.query.filter(TaskParsing.task_id == task.id).all():
            context_tags = self.tagger(context)
            weight_tags = [self.WeightedTag(name=tag_name, task_id=task.id, weight=0.0) for tag_name in context_tags]
            for weight_tag in weight_tags:
                self.database.add(weight_tag.context_tag())
                if weight_tag.task_id is not None:
                    current_weight_tag = content_tags.get(weight_tag.tag_id, weight_tag)
                    current_weight_tag.weight += 1.0
                    content_tags[weight_tag.tag_id] = current_weight_tag
        self.database.commit()
        return [content_tags[key] for key in content_tags]

    @staticmethod
    def tag_context(task):
        pass

    @staticmethod
    def extract_descriptors(extract_content):
        context = json.loads(extract_content.parsed)
        descriptors = list()
        for part in ['NN', 'NNS', 'NNP', 'NNPS']:
            descriptors.extend(context.get(part, list()))
        return descriptors

    @staticmethod
    def logger_message(problem_dict):
        return "Parsed context: {}".format(problem_dict["context"])


class RankResources(PipelineService):
    def __init__(self, publisher, database, ranker=None):
        super().__init__(publisher=publisher, database=database)
        self.ranker = ranker

    def perform_service(self, task):
        if self.ranker is None:
            task = self.rank_resources(task)
        else:
            task = self.ranker(task)
        return task

    def rank_resources(self, task):
        company = Company.query.filter(Company.id == task.company_id).first()
        site = Site.query.filter(Site.id == task.site_id).first()
        company_resources = ResourceCompanies.query.filter(ResourceCompanies.company_id == company.id).all()
        site_resources = ResourceSites.query.filter(ResourceSites.site_id == site.id).all()
        restricted_resources = True
        allowed_resources = list()
        if company.resource_restricted and site.resource_restricted:
            allowed_resources = [r.resource_id for r in company_resources if r in site_resources]
        elif company.resource_restricted:
            allowed_resources = [r.resource_id for r in company_resources]
        elif site_resources:
            allowed_resources = [r.resource_id for r in site_resources]
        else:
            restricted_resources = False

        if restricted_resources:
            resources = Resource.query.filter(Resource.active == 1).filter(
                Resource.can_dispatch == 1).filter(Resource.id in allowed_resources).all()
        else:
            resources = Resource.query.filter(Resource.active == 1).filter(
                Resource.can_dispatch == 1).all()

        task_tags = TaskTags.query.filter(TaskTags.task_id == task.id).all()
        scored_resources = dict()

        for resource in resources:
            resource_tag_ids = [r_tag.tag_id for r_tag in
                                ResourceTags.query.filter(ResourceTags.resource_id == resource.id).all()]
            for task_tag in task_tags:
                if task_tag.tag_id in resource_tag_ids:
                    current_resource_tag = ResourceTags.query.filter(
                        ResourceTags.resource_id == resource.id).filter(
                        ResourceTags.tag_id == task_tag.tag_id).first()
                    tag_score = (current_resource_tag.weight / task_tag.weight) - 1
                    scored_resources[resource.id] = scored_resources.get(resource.id, 0.0) + float(tag_score)
        for key in scored_resources:
            scored_resources[key] = scored_resources[key] / len(task_tags)

        for k, key in enumerate(sorted(scored_resources.keys(), key=lambda x: abs(scored_resources[x]))):
            self.database.add(TaskResources(task_id=task.id, resource_id=key, rank=k, weight=scored_resources[key]))
        task.ranked = True
        self.database.commit()
        return task

    @staticmethod
    def logger_message(problem_dict):
        return "Ranked Resources: {}".format(problem_dict["resources"][:5])


class GradeTask(PipelineService):
    def perform_service(self, task):
        ideal_task_resource = TaskResources.query.filter(TaskResources.task_id == task.id).filter(
            TaskResources.resource_id == task.ideal_resource).first()
        if ideal_task_resource is not None:
            if ideal_task_resource.rank <= 2:
                task.grade = 1
            else:
                task.grade = 0
            self.database.commit()
        return task


class ProcessDispatches(PipelineService):
    def __init__(self, publisher, database_connection, grade_list_size=None, console_output=False):
        super().__init__(publisher, database=database_connection)
        self.data_connection = database_connection
        self.problems = list()
        self.grade_list = list()
        self.grade_list_size = grade_list_size
        if self.grade_list_size is None:
            self.trim_grade_list = lambda: None
        else:
            self.trim_grade_list = self.list_trimmer
        if console_output:
            self.output_result = self.write_to_console
        else:
            self.output_result = lambda: None

    def listener(self, problem_json):
        _ = self.data_connection.set_dispatch_data(problem_json)
        super().listener(problem_json)

    def process(self, problem_dict):
        self.problems.append(problem_dict)
        grade_dict = dict()
        grade_dict["id"] = problem_dict["id"]
        problem_resources = [r[0] for r in problem_dict['resources']]
        if problem_dict["ideal"]['id'] in problem_resources[:5]:
            print("Succeeded Problem: {}\t\tIdeal: {}\t\tResources: {}".format(
                problem_dict["text"], problem_dict["ideal"]['id'], problem_resources))
            problem_dict["grade"] = 1
        else:
            print("Failed Problem: {}\t\tIdeal: {}\t\tResources: {}".format(
                problem_dict["text"], problem_dict["ideal"]['id'], problem_resources))
            problem_dict["grade"] = 0
        self.grade_list.append({"id": problem_dict["id"], "grade": problem_dict["grade"]})
        self.trim_grade_list()
        self.output_result()

        return problem_dict

    def list_trimmer(self):
        list_size = len(self.grade_list)
        if list_size > self.grade_list_size:
            self.grade_list = self.grade_list[list_size - self.grade_list_size:]

    def write_to_console(self):
        problem = self.grade_list[-1]
        print(f"Problem {problem['id']} result: {problem['grade']}. Running success rate {self.grade()}")

    def display_problems(self):
        generic_tags = self.data_connection.get_tag()
        tag_names = [generic_tags[tag]['name'] for tag in generic_tags]
        for problem in self.problems:
            print(f"Problem: {problem['text']}")
            tag_list = problem['tags']
            known_tags = 0
            for tag in sorted(tag_list, key=lambda t: tag_list[t]['weight'], reverse=True)[:10]:
                if tag in tag_names:
                    print("\t{}: {:4.3f}".format(tag, problem['tags'][tag]['weight']))
                else:
                    known_tags += 1
            print(f"\t{known_tags} not reflected in resource weights.")

    def grade(self):
        """ Calculate the current success rate of the pipeline.

        :return: Success rate of the pipeline in percentage format.
        """
        successes = len([1 for item in self.grade_list if item['grade'] == 1])
        attempts = len(self.grade_list)
        if attempts > 0:
            return f"{(successes * 100.0) / attempts:6.3f}%"
        else:
            return "0.000%"

    @staticmethod
    def logger_message(problem_dict):
        return "Processing result for problem {}: {}".format(problem_dict["problem_id"], problem_dict["grade"])


def text_data_input(file_name, publisher=None):
    with open(file_name) as text_file:
        reader = text_file.read()
        rows = reader.split(",\n")
        if publisher is None:
            return rows
        else:
            for row in rows:
                publisher.sendMessage(row)
            return True


def assemble_pipeline(database_connection, logging=False):
    input_broker = BrokerControl(publishing_broker=pub,
                                 publishing_topic="RawProblem",
                                 logging_broker=pub if logging else None,
                                 logging_topic="Logging" if logging else None)
    pipeline_input = DataInput(publisher=input_broker, database=database_connection)
    pub.subscribe(pipeline_input.listener, "RawInput")

    parsing_broker = BrokerControl(publishing_broker=pub,
                                   publishing_topic="ParsedProblem",
                                   logging_broker=pub if logging else None,
                                   logging_topic="Logging" if logging else None)
    pipeline_parser = TextParser(publisher=parsing_broker, database=database_connection)
    pub.subscribe(pipeline_parser.listener, "RawProblem")

    tagging_broker = BrokerControl(publishing_broker=pub,
                                   publishing_topic="TaggedProblem",
                                   logging_broker=pub if logging else None,
                                   logging_topic="Logging" if logging else None)
    pipeline_tagger = TextTagger(publisher=tagging_broker, database=database_connection)
    pub.subscribe(pipeline_tagger.listener, "ParsedProblem")

    ranking_broker = BrokerControl(publishing_broker=pub,
                                   publishing_topic="RankedProblem",
                                   logging_broker=pub if logging else None,
                                   logging_topic="Logging" if logging else None)
    pipeline_ranker = RankResources(publisher=ranking_broker, database=database_connection)
    pub.subscribe(pipeline_ranker.listener, "TaggedProblem")

    dispatch_broker = BrokerControl(publishing_broker=None,
                                    publishing_topic=None,
                                    logging_broker=pub if logging else None,
                                    logging_topic="Logging" if logging else None)
    pipeline_dispatch = ProcessDispatches(database_connection=database_connection,
                                          publisher=dispatch_broker)
    pub.subscribe(pipeline_dispatch.listener, "RankedProblem")

    problem_data = text_data_input("config/task_sample.json")
    for problem in problem_data:
        pub.sendMessage("RawInput", problem_json=problem)

    return pipeline_dispatch
