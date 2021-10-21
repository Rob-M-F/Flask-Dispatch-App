from sqlalchemy import Boolean, BigInteger, Column, DateTime, Integer, Numeric, String, ForeignKey
from flask_security import UserMixin, RoleMixin
from sqlalchemy.orm import relationship, backref
from pipeline_database import Base, db_session
import json


class PipelineBase:
    query = db_session.query_property()

    def fields(self):
        return [key for key in self.__dict__ if key[0] not in ('_', '<')]

    def as_dict(self):
        return {key: self.__dict__[key] for key in self.fields()}

    def to_dict(self):
        return self.as_dict()

    def from_dict(self, dict_data):
        fields = self.fields()
        rejected = list()
        for key in dict_data:
            if key in fields:
                exec(f"self.{key} = data_dict[{key}]")
            else:
                rejected.append((key, dict_data[key]))
        return rejected

    def as_json(self):
        return json.dumps(self.as_dict())

    def to_json(self):
        return self.as_json()

    def from_json(self, json_data):
        return self.from_dict(json.loads(json_data))

    def form_load(self, form):
        item_fields = self.fields()
        form_fields = [key.name for key in form]
        for field in form_fields:
            if field in item_fields:
                exec(f"form.{field}.data = self.{field}")
            elif 'validate' in field or 'choice' in field:
                pass
            else:
                exec(f"del form.{field}")

    def form_choices(self, form):
        pass

    def __repr__(self):
        self_dict = self.to_dict()
        return " ".join(f'{key} = {self_dict[key]}' for key in self_dict.keys())


class Tag(Base, PipelineBase):
    __tablename__ = 'tag'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    name = Column(String(60))
    active = Column(Boolean, default=True)
    segment_id = Column(BigInteger(), ForeignKey('segment.id'))

    def __repr__(self):
        return f"{self.name}"


class Resource(Base, PipelineBase):
    __tablename__ = 'resource'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    title = Column(String(60))
    contact_id = Column(BigInteger(), ForeignKey('contact.id'))
    active = Column(Boolean, default=True)
    can_dispatch = Column(Boolean, default=True)
    segment_id = Column(BigInteger(), ForeignKey('segment.id'))
    companies = relationship('ResourceCompanies', backref='resource')
    sites = relationship('ResourceSites', backref='resource')

    def __repr__(self):
        return f"{self.title}, {self.contact_id}"


class Segment(Base, PipelineBase):
    __tablename__ = 'segment'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    name = Column(String(60))
    active = Column(Boolean, default=True)
    tags = relationship(Tag, backref='segment')
    resources = relationship(Resource, backref='segment')

    def __repr__(self):
        return f"{self.name}"


class Company(Base, PipelineBase):
    __tablename__ = 'company'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    name = Column(String(60))
    active = Column(Boolean, default=True)
    resource_restricted = Column(Boolean, default=False)
    contacts = relationship('ContactCompanies', backref='company')
    sites = relationship('Site', backref='company')
    tasks = relationship('Task', backref='company')
    resources = relationship('ResourceCompanies', backref='company')

    def __repr__(self):
        return f"{self.name}"


class RolesUsers(Base, PipelineBase):
    __tablename__ = 'roles_users'
    id = Column(Integer(), primary_key=True, autoincrement=True)
    user_id = Column('user_id', BigInteger(), ForeignKey('user.id'))
    role_id = Column('role_id', BigInteger(), ForeignKey('role.id'))


class Role(Base, PipelineBase, RoleMixin):
    __tablename__ = 'role'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    name = Column(String(80), unique=True)
    description = Column(String(255))

    def __repr__(self):
        return f"{self.name}"


class ContactSites(Base, PipelineBase):
    __tablename__ = 'contacts_sites'
    id = Column(Integer(), primary_key=True, autoincrement=True)
    contact_id = Column('contact_id', BigInteger(), ForeignKey('contact.id'))
    site_id = Column('site_id', BigInteger(), ForeignKey('site.id'))
    authorizer = Column(Boolean, default=False)


class ResourceSites(Base, PipelineBase):
    __tablename__ = 'resource_sites'
    id = Column(Integer(), primary_key=True, autoincrement=True)
    resource_id = Column('resource_id', BigInteger(), ForeignKey('resource.id'))
    site_id = Column('site_id', BigInteger(), ForeignKey('site.id'))


class ContactCompanies(Base, PipelineBase):
    __tablename__ = 'contacts_companies'
    id = Column(Integer(), primary_key=True, autoincrement=True)
    contact_id = Column('contact_id', BigInteger(), ForeignKey('contact.id'))
    company_id = Column('company_id', BigInteger(), ForeignKey('company.id'))
    authorizer = Column(Boolean, default=False)


class ResourceCompanies(Base, PipelineBase):
    __tablename__ = 'resources_companies'
    id = Column(Integer(), primary_key=True, autoincrement=True)
    resource_id = Column('resource_id', BigInteger(), ForeignKey('resource.id'))
    company_id = Column('company_id', BigInteger(), ForeignKey('company.id'))


class Contact(Base, PipelineBase):
    __tablename__ = 'contact'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    name = Column(String(60))
    address_id = Column(BigInteger(), ForeignKey('address.id'))
    active = Column(Boolean, default=True)
    authorizer = Column(Boolean, default=False)
    resource_id = relationship(Resource, backref='contact')
    sites = relationship(ContactSites, backref='contact')
    companies = relationship(ContactCompanies, backref='contact')

    def __repr__(self):
        return f"{self.name}"


class Task(Base, PipelineBase):
    __tablename__ = 'task'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    description = Column(String(512))
    user_id = Column(BigInteger, ForeignKey('user.id'))
    authorizer_id = Column(BigInteger, ForeignKey('contact.id'))
    company_id = Column('company', BigInteger(), ForeignKey('company.id'))
    site_id = Column('site', BigInteger(), ForeignKey('site.id'))
    contact_id = Column('contact', BigInteger(), ForeignKey('contact.id'))
    dispatched_resource = Column('resource', BigInteger(), ForeignKey('resource.id'))
    ideal_resource = Column('ideal', BigInteger(), ForeignKey('resource.id'))
    resource_restricted = Column(Boolean, default=False)
    grade = Column(Boolean)
    active = Column(Boolean, default=True)
    parsed = Column(Boolean, default=False)
    tagged = Column(Boolean, default=False)
    ranked = Column(Boolean, default=False)
    dispatched = Column(Boolean, default=False)
    authorizers = relationship(Contact, foreign_keys=authorizer_id)
    contacts = relationship(Contact, foreign_keys=contact_id)

    def __repr__(self):
        return f"{self.id}: {self.description}"


class User(Base, PipelineBase, UserMixin):
    __tablename__ = 'user'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    alt_id = Column(String(64), unique=True)
    fs_uniquifier = Column(String(64), unique=True)
    email = Column(String(128), unique=True)
    username = Column(String(128))
    password = Column(String(128))
    contact_id = Column(BigInteger(), ForeignKey('contact.id'))
    last_login_at = Column(DateTime())
    current_login_at = Column(DateTime())
    last_login_ip = Column(String(100))
    current_login_ip = Column(String(100))
    login_count = Column(Integer)
    active = Column(Boolean())
    confirmed_at = Column(DateTime())
    roles = relationship('Role', secondary='roles_users', backref=backref('users', lazy='dynamic'))
    tasks = relationship(Task, backref='user')

    def __repr__(self):
        return f"{self.email}"

    # def has_roles(self, *args):
    #     user_roles = RolesUsers.query.filter(RolesUsers.user_id == self.id).all()
    #     user_role_ids = [r.role_id for r in user_roles]
    #     role_names = [r.name for r in Role.query.all() if r.id in user_role_ids]
    #     return set(args).issubset({role.name for role in role_names})


class Site(Base, PipelineBase):
    __tablename__ = 'site'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    name = Column(String(60))
    address_id = Column(BigInteger(), ForeignKey('address.id'))
    company_id = Column(BigInteger(), ForeignKey('company.id'))
    resource_restricted = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    contacts = relationship(ContactSites, backref='site')
    tasks = relationship(Task, backref='site')

    def __repr__(self):
        return f"{self.name}"


class Address(Base, PipelineBase):
    __tablename__ = 'address'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    gps = Column(BigInteger())
    number = Column(String(60))
    street = Column(String(60))
    city = Column(String(60))
    state = Column(String(60))
    postal = Column(String(60))
    country = Column(String(60), default='USA')
    active = Column(Boolean, default=True)
    contacts = relationship(Contact, backref='address')
    sites = relationship(Site, backref='address')

    def __repr__(self):
        return f"{self.number} {self.street}, {self.city} {self.state} {self.postal} "


class CompanyTags(Base, PipelineBase):
    __tablename__ = 'company_tags'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    company_id = Column('company_id', BigInteger(), ForeignKey('company.id'))
    tag_id = Column('tag_id', BigInteger(), ForeignKey('tag.id'))
    weight = Column(Numeric(precision=10, scale=9))
    active = Column(Boolean, default=True)


class ContactTags(Base, PipelineBase):
    __tablename__ = 'contacts_tags'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    contact_id = Column('contact_id', BigInteger(), ForeignKey('contact.id'))
    tag_id = Column('tag_id', BigInteger(), ForeignKey('tag.id'))
    weight = Column(Numeric(precision=10, scale=9))
    active = Column(Boolean, default=True)


class SiteTags(Base, PipelineBase):
    __tablename__ = 'sites_tags'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    site_id = Column('site_id', BigInteger(), ForeignKey('site.id'))
    tag_id = Column('tag_id', BigInteger(), ForeignKey('tag.id'))
    weight = Column(Numeric(precision=10, scale=9))
    active = Column(Boolean, default=True)


class ResourceTags(Base, PipelineBase):
    __tablename__ = 'resources_tags'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    resource_id = Column('resource_id', BigInteger(), ForeignKey('resource.id'))
    tag_id = Column('tag_id', BigInteger(), ForeignKey('tag.id'))
    weight = Column(Numeric(precision=10, scale=9))
    active = Column(Boolean, default=True)


class TaskParsing(Base, PipelineBase):
    __tablename__ = 'task_parsing'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    task_id = Column('task_id', BigInteger(), ForeignKey('task.id'))
    ordinal = Column(Integer, default=0)
    parsed = Column(String(512))


class TaskTags(Base, PipelineBase):
    __tablename__ = 'task_tags'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    task_id = Column('task_id', BigInteger(), ForeignKey('task.id'))
    tag_id = Column('tag_id', BigInteger(), ForeignKey('tag.id'))
    weight = Column(Numeric(precision=10, scale=9))
    active = Column(Boolean, default=True)


class TaskResources(Base, PipelineBase):
    __tablename__ = 'task_resources'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    task_id = Column('task_id', BigInteger(), ForeignKey('task.id'))
    resource_id = Column('resource_id', BigInteger(), ForeignKey('resource.id'))
    rank = Column(Integer, default=1)
    weight = Column(Numeric(precision=10, scale=9))


class ContextTags(Base, PipelineBase):
    __tablename__ = 'context_tags'
    id = Column(BigInteger(), primary_key=True, autoincrement=True)
    task_id = Column('task_id', BigInteger(), ForeignKey('task.id'))
    tag_id = Column('tag_id', BigInteger(), ForeignKey('tag.id'))
    name = Column(String(60))
