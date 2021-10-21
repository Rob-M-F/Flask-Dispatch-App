from flask import render_template, redirect, url_for, send_file  # , request, flash
from flask_security import current_user, auth_required, roles_accepted, hash_password
from io import BytesIO
from matplotlib.figure import Figure
from pipeline_form import PipelineInterface
from pipeline_app import app, db_session, user_datastore
from pipeline_model import Address, Company, Contact, Resource, ResourceTags, Segment, Site, Task, TaskResources
from pipeline_model import TaskTags, Tag, User, Role, RolesUsers
from random import random


def get_data_choices():
    """
    Compiles listing of all data needed for the SelectFields in the data review page.
    :return: Dictionary of choices for the data review select fields.
    """
    choice_dict = dict()
    choice_dict['address'] = [
        (current_address.id, f'{current_address.number} {current_address.street}, {current_address.city}') for
        current_address in Address.query.order_by('postal').all()]
    choice_dict['company'] = [(current_company.id, current_company.name) for current_company in
                              Company.query.order_by('name').all()]
    choice_dict['contact'] = [(current_contact.id, current_contact.name) for current_contact in
                              Contact.query.order_by('name').all()]
    choice_dict['resource'] = [
        (current_resource.id, Contact.query.filter(Contact.id == current_resource.contact_id).first().name) for
        current_resource in Resource.query.order_by('id')]
    choice_dict['site'] = [(current_site.id, current_site.name) for current_site in Site.query.order_by('name').all()]
    choice_dict['task'] = [(current_task.id, Site.query.filter(Site.id == current_task.site_id).first().name) for
                           current_task in Task.query.order_by('id')]
    choice_dict['tag'] = [(current_tag.id, current_tag.name) for current_tag in Tag.query.order_by('name').all()]
    return choice_dict


def populate_address_choices(current_item, form):
    """
    Generates and populates the active address options in SelectFields.
    :param current_item: the object that will have an address associated with it.
    :param form: the pipeline_form requiring the addition of address information.
    :return: None
    """

    all_addresses = [(a.id, str(a)) for a in Address.query.filter(Address.active == 1)]
    form.address.choices = all_addresses if all_addresses else [(0, '')]
    if current_item is None:
        form.address.data = form.address.choices[0][0]
    else:
        form.address.data = current_item.address_id


def populate_contact_choices(current_item, form):
    """
    Generates and populates the active contacts options in SelectFields.
    :param current_item: the object that will have contacts associated with it.
    :param form: the pipeline_form requiring the addition of contact information.
    :return: Flag indicating whether data was found.
    """
    all_contacts = [(c.id, c.name) for c in Contact.query.filter(Contact.active == 1).all()]
    form.all_contacts.choices = all_contacts if all_contacts else [(0, '')]
    form.all_contacts.data = form.all_contacts.choices[0][0]
    if current_item.contacts:
        item_contacts = [(c.id, c.name) for c in current_item.contacts]
        form.contacts.choices = item_contacts if item_contacts else [(0, '')]
        form.contacts.data = form.contacts.choices[0][0]
        return True
    else:
        form.contacts.choices = [(0, '')]
        form.contacts.data = form.contacts.choices[0][0]
        return False


def populate_company_choices(current_item, form):
    """
    Generates and populates the active company options in SelectFields.
    :param current_item: the object that will have a company associated with it.
    :param form: the pipeline_form requiring the addition of company information.
    :return: Flag indicating whether data was found.
    """
    item_companies = [(c.id, c.name) for c in current_item.companies]
    form.companies.choices = item_companies if item_companies else [(0, '')]
    form.companies.data = form.companies.choices[0][0]
    if current_item.companies:
        return True
    else:
        return False


def populate_site_choices(current_item, form):
    """
    Generates and populates the active site options in SelectFields.
    :param current_item: the object that will have a site associated with it.
    :param form: the pipeline_form requiring the addition of site information.
    :return: Flag indicating whether data was found.
    """
    if current_item.sites:
        form.sites.choices = [(s.id, s.name) for s in current_item.sites]
        return True
    else:
        del form.sites
        return False


def populate_task_choices(current_item, form):
    """
    Generates and populates the active tasks options in SelectFields.
    :param current_item: the object that will have tasks associated with it.
    :param form: the pipeline_form requiring the addition of task information.
    :return: Flag indicating whether data was found.
    """
    if current_item.tasks:
        form.tasks.choices = [(t.id, t.desc) for t in current_item.tasks]
        return True
    else:
        del form.tasks
        return False


@app.route('/', methods=['GET', 'POST'])
def landing():
    """
    Sets up initial page presented to unknown users when connecting to the site.
    :return: either the rendered index template or a redirect to the dashboard.
    """
    if current_user.is_anonymous:
        name = 'Guest'
        form = PipelineInterface.IndexForm()
        return render_template('index.html', form=form, name=name, guest=True,
                               app_title="Task Pipeline",
                               page_title="Task Pipeline",
                               page_name="Welcome")
    else:
        return redirect(url_for('home'))


@app.route('/index', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def home():
    """
    Main user dashboard, indicaing current system state.
    :return: rendered template of the dashboard.
    """
    awaiting_dispatch = app.check_task_statuses()
    form = PipelineInterface.DashboardForm()
    form.resource_demand.choices = get_resource_demand()
    form.task_tags.choices = get_task_tags()
    form.awaiting_dispatch.choices = [(task.id,
                                       f"{task.id}: {Site.query.filter(Site.id == task.site_id).first().name}")
                                      for task in awaiting_dispatch]
    if form.validate_on_submit():
        print(f"Resource selected: {form.resource_demand.data}, ",
              f"Tag selected: {form.task_tags.data}, ",
              f"Task selected {form.awaiting_dispatch.data}")
        if form.review_resource_demand.data:
            return redirect(url_for('review_resource', item_id=form.resource_demand.data))
        elif form.review_task_tags.data:
            return redirect(url_for('review_tag', item_id=form.task_tags.data))
        elif form.dispatch_task.data:
            return redirect(url_for('task_dispatch', item_id=form.awaiting_dispatch.data))
    form. resource_demand.data = form.resource_demand.choices[0][0]
    form.task_tags.data = form.task_tags.choices[0][0]
    form.awaiting_dispatch.data = form.awaiting_dispatch.choices[0][0]
    return render_template('dashboard.html', form=form,
                           app_title="Task Pipeline",
                           page_title="Task Pipeline - Health Dashboard",
                           page_name="Welcome")


def get_task_tags(num_tasks=1000):
    """
    Collect task associated tags and prepare them for display.
    :param num_tasks: Count of task history to include.
    :return: list of tags associations, formatted for use in a SelectField
    """
    tag_dict = dict()
    for task_id in [task.id for task in Task.query.order_by(Task.id.desc()).limit(num_tasks).all()]:
        task_tag_list = TaskTags.query.filter(TaskTags.task_id == task_id).all()
        for task_tag in task_tag_list:
            tag_dict[task_tag.tag_id] = tag_dict.get(task_tag.tag_id, 0.0) + float(task_tag.weight)
    return [(key, f"{tag_dict[key]:5.3f}: {Tag.query.filter(Tag.id == key).first().name}") for key in
            sorted(tag_dict.keys(), key=lambda k: tag_dict[k], reverse=True)]


def get_resource_demand(num_tasks=1000):
    """
    Get resource demand and prepare it for display.
    :param num_tasks: Count of task history to include.
    :return: list of resources, ordered by demand level, formatted for use in a SelectField.
    """
    resource_data = {}
    ordered_data = []
    for current_task in Task.query.order_by(Task.id.desc()).limit(num_tasks).all():
        for t in TaskResources.query.filter(TaskResources.task_id == current_task.id).all():
            current_resource = resource_data.get(t.resource_id, list())
            current_resource.append(t.rank)
            resource_data[t.resource_id] = current_resource
    for key in resource_data.keys():
        ave = sum(resource_data[key]) / float(len(resource_data[key]))
        ordered_data.append((ave, key))
    if ordered_data:
        return [(key, "{:^5.3f}: {}".format(ave, Contact.query.filter(Contact.id == Resource.query.filter(
            Resource.id == key).first().contact_id).first().name)) for ave, key in sorted(ordered_data, reverse=True)]
    else:
        return [(0, '')]


@app.route('/task_tag_plot', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def task_tag_plot(tag_list=None):
    img = BytesIO()
    fig = Figure()
    ax = fig.add_subplot(1, 1, 1)
    if True:
        xs = [random() for r in range(20)]
        ys = [random() for r in range(20)]

    if tag_list is None:
        task_list = Task.query.filter(Task.grade.isnot(None)).all()

    ax.plot(xs, ys)
    fig.savefig(img, format='png')
    img.seek(0)
    return send_file(img, mimetype='image/png')


@app.route('/task_grade_plot', methods=['GET'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def task_grade_plot(num_tasks=100):
    img = BytesIO()
    fig = Figure()
    ax = fig.add_subplot(1, 1, 1)
    total, correct, grade = grade_tasks(num_tasks)
    ax.pie([correct/total, 1 - correct/total],  labels=('Correct', 'Incorrect'),
           explode=(0.1, 0), autopct='%1.1f%%', shadow=True, startangle=90)
    ax.axis('equal')
    fig.savefig(img, format='png')
    img.seek(0)
    return send_file(img, mimetype='image/png')


@app.route('/add_task', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def add_task():
    form = PipelineInterface.TaskAddForm()
    choices = get_data_choices()
    form.company.choices = choices['company']
    form.site.choices = choices['site']
    form.authorizer.choices = choices['contact']
    form.tag.choices = choices['tag']
    if form.validate_on_submit():
        db_session.add(Task(user_id=current_user.id, company_id=form.company.data, site_id=form.site.data,
                            authorizer_id=form.authorizer.data, description=form.description.data))
        db_session.commit()
        return redirect(url_for('landing'))
    return render_template('add_task.html', user=current_user.email, form=form,
                           app_title="Task Pipeline", page_title="Task Pipeline - Add Task")


@app.route('/review_task<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def review_task(item_id):
    task = Task.query.filter(Task.id == item_id).first()
    form = PipelineInterface.ReviewTask(obj=task)
    form.authorizer.choices = [(c.id, str(c)) for c in
                               Contact.query.filter(Contact.active == 1)]
    form.authorizer.data = task.authorizer_id
    form.company.choices = [(c.id, str(c)) for c in Company.query.filter(Company.active == 1)]
    form.company.data = task.company_id
    form.site.choices = [(s.id, str(s)) for s in Site.query.filter(Site.active == 1)]
    form.site.data = task.site_id
    if form.validate_on_submit():
        if form.validate_cancel.data or form.validate_update.data:
            if form.validate_update.data:
                db_session.commit()
            return redirect(url_for('select_data'))
    return render_template('review_task.html', form=form,
                           app_title="Task Pipeline", page_title="Task Pipeline - Review Task")


@app.route('/select_data', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def select_data():
    form = PipelineInterface.SelectData()
    choices = get_data_choices()
    form.address.choices = choices['address'] if choices['address'] else [(0, '')]
    form.company.choices = choices['company'] if choices['company'] else [(0, '')]
    form.contact.choices = choices['contact'] if choices['contact'] else [(0, '')]
    form.resource.choices = choices['resource'] if choices['resource'] else [(0, '')]
    form.site.choices = choices['site'] if choices['site'] else [(0, '')]
    form.task.choices = choices['task'] if choices['task'] else [(0, '')]
    form.tag.choices = choices['tag'] if choices['tag'] else [(0, '')]
    if form.validate_on_submit():
        if form.address_add.data:
            return redirect(url_for('review_address'))
        elif form.address_select.data and form.address.data != 0:
            return redirect(url_for('review_address', item_id=form.address.data))
        elif form.company_add.data:
            return redirect(url_for('review_company'))
        elif form.company_select.data and form.company.data != 0:
            return redirect(url_for('review_company', item_id=form.company.data))
        elif form.contact_add.data:
            return redirect(url_for('review_contact'))
        elif form.contact_select.data and form.contact.data != 0:
            return redirect(url_for('review_contact', item_id=form.contact.data))
        elif form.resource_add.data:
            return redirect(url_for('review_resource'))
        elif form.resource_select.data and form.resource.data != 0:
            return redirect(url_for('review_resource', item_id=form.resource.data))
        elif form.site_add.data:
            return redirect(url_for('review_site'))
        elif form.site_select.data and form.site.data != 0:
            return redirect(url_for('review_site', item_id=form.site.data))
        elif form.tag_add.data:
            return redirect(url_for('review_tag'))
        elif form.tag_select.data and form.tag.data != 0:
            return redirect(url_for('review_tag', item_id=form.tag.data))
        elif form.task_add.data:
            return redirect(url_for('add_task'))
        elif form.task_select.data and form.task.data != 0:
            return redirect(url_for('review_task', item_id=form.task.data))
    return render_template('select_data.html', form=form,
                           app_title="Task Pipeline", page_title="Task Pipeline - Data Maintenance")


@app.route('/manage_user', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin')
def manage_user():
    form = PipelineInterface.ManageUser()
    form.user.choices = [(u.id, u.email) for u in User.query.all()]

    if form.validate_on_submit():
        if form.user_add.data:
            return redirect(url_for('review_user'))
        elif form.user_select.data and form.user.data != 0:
            return redirect(url_for('review_user', item_id=form.user.data))
    return render_template('manage_user.html', form=form,
                           app_title="Task Pipeline", page_title="Task Pipeline - Data Maintenance")


@app.route('/add_user', methods=['GET', 'POST'])
@app.route('/review_user<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin')
def review_user(item_id=None):
    """
    Handle creation, review, and modification of user accounts.
    :param item_id: User ID of the user to be modified.
    :return: User add/review interface rendered template
    """
    if item_id is None:
        page_type = "Add User"
        user = None
        form = PipelineInterface.ReviewUser()
        form.id.data = -1
        form.submit.label.text = "Create"
        form.role.choices = [(r.id, r.name) for r in Role.query.all()]
    else:
        page_type = "Review User"
        user = User.query.filter(User.id == item_id).first()
        form = PipelineInterface.ReviewUser()
        form.id.data = user.id
        form.contact.choices = [(c.id, c.name) for c in Contact.query.filter(Contact.active == 1).all()]
        form.role.choices = [(r.id, r.name) for r in Role.query.all()]

    form.contact.choices = [(c.id, c.name) for c in Contact.query.filter(Contact.active == 1).all()]

    if form.validate_on_submit():
        if form.submit.data:
            if item_id is None:
                user_datastore.create_user(email=form.email.data, username=form.username.data,
                                           password=hash_password(form.password.data), contact_id=form.contact.data,
                                           active=form.active.data)
            else:
                user.email = form.email.data
                user.username = form.username.data
                user.password = form.password.data
                user.contact_id = form.contact.data
                user.active = form.active.data
            db_session.commit()

            user = User.query.filter(User.username == form.username.data).first()
            print(user)
            user_roles = [Role.query.filter(Role.id == r.role_id).first() for r in RolesUsers.query.filter(
                RolesUsers.user_id == user.id).all()]
            for role in user_roles:
                if role.id not in form.role.data:
                    user_datastore.remove_role_from_user(user=user, role=role)

            user_role_list = [Role.query.filter(Role.id == r.role_id).first().id for r in RolesUsers.query.filter(
                RolesUsers.user_id == user.id).all()]
            for role_id in form.role.data:
                if role_id not in user_role_list:
                    user_datastore.add_role_to_user(user=user, role=Role.query.filter(Role.id == role_id).first())
            db_session.commit()

    if item_id is None:
        form.email.data = ''
        form.username.data = ''
        form.contact.data = ''
        form.active.data = False
    else:
        form.email.data = user.email
        form.username.data = user.username
        form.contact.data = user.contact_id
        form.active.data = user.active
        form.role.data = [r.role_id for r in RolesUsers.query.filter(RolesUsers.user_id == user.id).all()]
        form.contact.data = form.contact.choices[0][0] if item_id is None else user.contact_id

    return render_template('review_user.html', form=form,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - {page_type}")


@app.route('/add_address', methods=['GET', 'POST'])
@app.route('/review_address<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def review_address(item_id=None):
    if item_id is None:
        page_type = "Add Address"
        form = PipelineInterface.ReviewAddress()
        form.id.data = -1
        form.contacts.choices = [(0, '')]
        form.contacts.data = 0
        contacts = False
        form.sites.choices = [(0, '')]
        form.sites.data = 0
        sites = False
    else:
        page_type = "Review Address"
        address = Address.query.filter(Address.id == item_id).first()
        form = PipelineInterface.ReviewAddress(obj=address)
        contacts = populate_contact_choices(current_item=address, form=form)
        sites = populate_site_choices(current_item=address, form=form)

    if form.validate_on_submit():
        if form.validate_update.data:
            if item_id is None:
                db_session.add(Address(number=form.number.data, street=form.street.data, city=form.city.data,
                                       state=form.state.data, postal=form.postal.data, country=form.country.data,
                                       active=form.active.data))
            db_session.commit()
            return redirect(url_for('select_data'))
        elif form.review_contact.data:
            return redirect(url_for('review_contact', item_id=form.contacts.data))
        elif form.review_site.data:
            return redirect(url_for('review_site', item_id=form.sites.data))

        if item_id is None:
            return redirect(url_for('add_address'))
        else:
            return redirect(url_for('review_address', item_id=item_id))
    elif form.validate_cancel.data:
        return redirect(url_for('select_data'))
    return render_template('review_address.html', form=form, contact=contacts, site=sites,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - {page_type}")


@app.route('/add_company', methods=['GET', 'POST'])
@app.route('/review_company<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def review_company(item_id=None):
    if item_id is None:
        page_type = "Add Company"
        form = PipelineInterface.ReviewCompany()
        form.id.data = -1
        del form.contacts
        contacts = False
        del form.sites
        sites = False
    else:
        page_type = "Review Company"
        company = Company.query.filter(Company.id == item_id).first()
        form = PipelineInterface.ReviewCompany(obj=company)
        contacts = populate_contact_choices(current_item=company, form=form)
        sites = populate_site_choices(current_item=company, form=form)

    authorizers = True
    if contacts:
        authorizer_list = Contact.query.filter(Contact.authorizer == 1).filter(Contact.id in company.contacts).filter(
            Contact.active == 1)
        if authorizer_list:
            form.authorizers.choices = [(a.id, str(a)) for a in authorizer_list]
        else:
            del form.authorizers
            authorizers = False
    else:
        del form.authorizers
        authorizers = False

    if form.validate_on_submit():
        if form.validate_update.data:
            if item_id is None:
                db_session.add(Company(name=form.name.data, active=form.active.data))
            db_session.commit()
            return redirect(url_for('select_data'))
        if contacts and form.review_contact.data:
            return redirect(url_for('review_contact', item_id=form.contacts.data))
        if sites and form.review_site.data:
            return redirect(url_for('review_site', item_id=form.sites.data))
        return redirect(url_for('select_data'))
    elif form.validate_cancel.data:
        return redirect(url_for('select_data'))

    return render_template('review_company.html', form=form, authorizer=authorizers, contact=contacts, site=sites,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - {page_type}")


@app.route('/add_contact', methods=['GET', 'POST'])
@app.route('/review_contact<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def review_contact(item_id=None):
    resource = False
    if item_id is None:
        page_type = "Add Contact"
        form = PipelineInterface.ReviewContact()
        form.id.data = -1
        del form.sites
        sites = False
        del form.companies
        companies = False
    else:
        page_type = "Review Contact"
        contact = Contact.query.filter(Contact.id == item_id).first()
        form = PipelineInterface.ReviewContact(obj=contact)
        sites = populate_site_choices(current_item=contact, form=form)
        companies = populate_company_choices(current_item=contact, form=form)
        if contact.resource_id:
            resource = True
            form.resource.data = contact.resource_id
    populate_address_choices(current_item=None, form=form)

    if form.validate_on_submit():
        if form.validate_update.data:
            if item_id is None:
                db_session.add(Contact(name=form.name.data, address_id=form.address.data, active=form.active.data))
            db_session.commit()
            return redirect(url_for('select_data'))
        elif form.review_resource.data:
            return redirect(url_for('review_resource', item_id=form.resource.data))
        elif form.review_site.data:
            return redirect(url_for('review_site', item_id=form.sites.data))
        elif form.review_company.data:
            return redirect(url_for('review_company', item_id=form.companies.data))
    elif form.validate_cancel.data:
        return redirect(url_for('select_data'))

    return render_template('review_contact.html', form=form, resource=resource, site=sites, company=companies,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - {page_type}")


@app.route('/add_resource', methods=['GET', 'POST'])
@app.route('/review_resource<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def review_resource(item_id=None):
    if item_id is None:
        page_type = "Add Resource"
        form = PipelineInterface.ReviewResource()
        form.id.data = -1
        form.contact.choices = [(c.id, c.name) for c in Contact.query.filter(Contact.active == 1)]
        form.segment.choices = [(s.id, s.name) for s in Segment.query.filter(Segment.active == 1)]
        form.contact.data = form.contact.choices[0][0]
        form.segment.data = form.segment.choices[0][0]
        del form.all_tags
        del form.resource_tags
        tags = False
    else:
        page_type = "Review Resource"
        resource = Resource.query.filter(Resource.id == item_id).first()
        form = PipelineInterface.ReviewResource(obj=resource)
        form.contact.choices = [(c.id, c.name) for c in Contact.query.filter(Contact.active == 1)]
        form.segment.choices = [(s.id, s.name) for s in Segment.query.filter(Segment.active == 1)]
        form.contact.data = resource.contact_id
        form.segment.data = resource.segment_id
        resource_tags = ResourceTags.query.filter(ResourceTags.resource_id == resource.id).all()
        resource_tag_ids = [t.tag_id for t in resource_tags]
        all_tags_list = [(t.id, t.name) for t in Tag.query.all() if t.id not in resource_tag_ids]
        form.all_tags.choices = all_tags_list if all_tags_list else [(0, '')]
        resource_tags_list = [(t.tag_id, f"{t.weight:4.3f}: {Tag.query.filter(Tag.id == t.tag_id).first().name}")
                              for t in resource_tags]
        form.resource_tags.choices = resource_tags_list if resource_tags_list else [(0, '')]
        tags = True

    if form.validate_on_submit():
        if form.validate_update.data:
            if item_id is None:
                db_session.add(Resource(title=form.title.data, contact_id=form.contact.data,
                                        segment_id=form.segment.data, can_dispatch=form.can_dispatch.data,
                                        active=form.active.data))
            db_session.commit()
            return redirect(url_for('select_data'))
        elif form.review_contact.data:
            return redirect(url_for('review_contact', item_id=form.contact.data))
        elif form.review_segment.data:
            return redirect(url_for('review_segment', item_id=form.contact.data))
    return render_template('review_resource.html', form=form, tags=tags,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - {page_type}")


@app.route('/add_site', methods=['GET', 'POST'])
@app.route('/review_site<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def review_site(item_id=None):
    if item_id is None:
        page_type = "Add Site"
        site = None
        form = PipelineInterface.ReviewSite()
        form.id.data = -1
    else:
        page_type = "Review Site"
        site = Site.query.filter(Site.id == item_id).first()
        form = PipelineInterface.ReviewSite(obj=site)
    populate_address_choices(current_item=site, form=form)
    form.company.choices = [(c.id, str(c)) for c in Company.query.filter(Company.active == 1)]
    form.company.data = form.company.choices[0][0] if site is None else site.company_id
    contacts = populate_contact_choices(current_item=site, form=form)
    tasks = populate_task_choices(current_item=site, form=form)

    if form.validate_on_submit():
        if form.validate_update.data:
            if item_id is None:
                db_session.add(Site(name=form.name.data, address_id=form.address.data, company_id=form.company.data,
                                    active=form.active.data))
            db_session.commit()
            return redirect(url_for('select_data'))
        elif form.review_address.data:
            return redirect(url_for('review_address', item_id=form.address.data))
        elif form.review_company.data:
            return redirect(url_for('review_company', item_id=form.company.data))
        elif form.review_contact.data:
            return redirect(url_for('review_contact', item_id=form.contact.data))
        elif form.review_task.data:
            return redirect(url_for('review_task', item_id=form.task.data))
    return render_template('review_site.html', form=form, contact=contacts, task=tasks,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - {page_type}")


@app.route('/add_tag', methods=['GET', 'POST'])
@app.route('/review_tag<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher', 'user')
def review_tag(item_id=None):
    if item_id is None:
        page_type = "Add Tag"
        tag = None
        form = PipelineInterface.ReviewTag()
        form.id.data = -1
    else:
        page_type = "Review Tag"
        tag = Tag.query.filter(Tag.id == item_id).first()
        form = PipelineInterface.ReviewTag(obj=tag)
    form = PipelineInterface.ReviewTag(obj=tag)
    form.segment.choices = [(s.id, s.name) for s in Segment.query.filter(Segment.active == 1)]
    form.segment.data = form.segment.choices[0][0] if tag is None else tag.segment_id

    if form.validate_on_submit():
        if form.validate_update.data:
            if item_id is None:
                db_session.add(Tag(name=form.name.data, segment_id=form.segment.data, active=form.active.data))
            db_session.commit()
            return redirect(url_for('select_data'))
    return render_template('review_tag.html', form=form,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - {page_type}")


@app.route('/task_grader', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin')
def task_grader():
    form = PipelineInterface.TaskGrader()
    tasks_without_grades = [(task.id, f"{task.id}: {Site.query.filter(Site.id == task.site_id).first()}")
                            for task in Task.query.filter(Task.active == 1).filter(Task.grade.is_(None)).all()]
    form.ungraded_tasks.choices = tasks_without_grades if tasks_without_grades else [(0, ''*30)]

    tasks_with_grades = [(task.id, f"{'CORRECT' if task.grade else 'INCORRECT'} {task.id}: " +
                                   f"{Site.query.filter(Site.id == task.site_id).first()}")
                         for task in Task.query.order_by(Task.grade.desc()).filter(Task.active == 1).filter(
            Task.grade.isnot(None)).all()]
    form.graded_tasks.choices = tasks_with_grades if tasks_with_grades else [(0, ''*30)]
    form.resources.choices = get_resource_choices()

    if form.validate_on_submit():
        if form.review_ungraded_task.data:
            return redirect(url_for('review_task_grade', item_id=form.ungraded_tasks.data))
        elif form.review_graded_task.data:
            return redirect(url_for('review_task_grade', item_id=form.graded_tasks.data))
        elif form.review_resource.data:
            return redirect(url_for('review_resource', item_id=form.resources.data))
        elif form.assign_ideal.data:
            task = Task.query.filter(Task.id == form.ungraded_tasks.data).first()
            task.ideal_resource = form.resources.data
            db_session.commit()
            _ = app.check_task_statuses()
            return redirect(url_for('task_grader'))
        else:
            return redirect(url_for('task_grader'))

    form.ungraded_tasks.data = form.ungraded_tasks.choices[0][0]
    form.graded_tasks.data = form.graded_tasks.choices[0][0]
    form.resources.data = form.resources.choices[0][0]
    return render_template('task_grader.html', form=form,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - Task Grader")


@app.route('/dispatch_task<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin', 'dispatcher')
def task_dispatch(item_id):
    task = Task.query.filter(Task.id == item_id).first()
    form = PipelineInterface.TaskDispatch()
    task_site = Site.query.filter(Site.id == task.site_id).first()
    form.resource.choices = get_resource_choices()
    form.task.data = f"{task.id}: {task_site} - {task.description}"

    if form.validate_on_submit():
        if form.review_task:
            return redirect(url_for('review_task', item_id=task.id))
        elif form.resource.data:
            if form.dispatch_task.data:
                task.dispatched_resource = form.resource.data
                task.dispatched = 1
                db_session.commit()
                return redirect(url_for('home'))
            elif form.review_resource:
                return redirect(url_for('review_resource', item_id=task.id))
        return redirect(url_for('task_dispatch', item_id=task.id))

    form.resource.data = form.resource.choices[0][0]
    return render_template('task_dispatch.html', form=form,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - Task Dispatch")


@app.route('/review_task_grade<item_id>', methods=['GET', 'POST'])
@auth_required()
@roles_accepted('admin')
def review_task_grade(item_id):
    form = PipelineInterface.ReviewGrade()
    task = Task.query.filter(Task.id == item_id).first()
    form.authorizer.data = Contact.query.filter(Contact.id == task.authorizer_id).first().name
    form.company.data = Company.query.filter(Company.id == task.company_id).first().name
    form.site.data = Site.query.filter(Site.id == task.site_id).first().name
    form.description.data = task.description

    if task.ideal_resource is None:
        form.ideal.data = 'IDEAL RESOURCE UNSELECTED.'
    else:
        form.ideal.data = Contact.query.filter(Contact.id == Resource.query.filter(
            Resource.id == task.ideal_resource).first().contact_id).first().name

    resource_choices = list()
    for t in TaskResources.query.order_by(TaskResources.rank.asc()).filter(TaskResources.task_id == task.id).all():
        for r in Resource.query.filter(Resource.id == t.resource_id).all():
            r_name = Contact.query.filter(Contact.id == r.contact_id).first().name
            resource_choices.append((r.id, f"{t.weight:2.3f}: {r_name}"))
    form.resource.choices = resource_choices if resource_choices else [(0, "")]

    tag_choices = list()
    for t in TaskTags.query.order_by(TaskTags.weight.desc()).filter(TaskTags.task_id == task.id).all():
        for tag in Tag.query.filter(Tag.id == t.tag_id).all():
            tag_choices.append((tag.id, f"{t.weight:2.3f}: {tag.name}"))
    form.tag.choices = tag_choices if tag_choices else [(0, "")]

    form.grade.data = "NOT APPLICABLE." if task.grade is None else 'CORRECT' if task.grade else 'INCORRECT'

    if form.validate_on_submit():
        if form.review_authorizer.data:
            return redirect(url_for('review_contact', item_id=task.authorizer_id))
        elif form.review_company.data:
            return redirect(url_for('review_company', item_id=task.company_id))
        elif form.review_site.data:
            return redirect(url_for('review_site', item_id=task.site_id))
        elif form.review_ideal.data:
            return redirect(url_for('review_resource', item_id=task.ideal_resource))
        elif form.review_resource.data:
            return redirect(url_for('review_resource', item_id=form.resource.data))
        elif form.review_tag.data:
            return redirect(url_for('review_tag', item_id=form.tag.data))
        else:
            return redirect(url_for('review_task_grade', item_id=task.id))

    form.resource.data = form.resource.choices[0][0]
    form.tag.data = form.tag.choices[0][0]
    return render_template('review_grade.html', form=form,
                           app_title="Task Pipeline", page_title=f"Task Pipeline - Review Task Awaiting Dispatch")


def grade_tasks(num_tasks=100):
    tasks = Task.query.order_by(Task.id.desc()).filter(Task.grade.isnot(None)).limit(num_tasks).all()
    total = len(tasks)
    correct = len([task.id for task in tasks if task.grade])
    grade = 100 * correct / total
    return total, correct, grade


def get_resource_choices(active=True):
    resource_list = list()
    if active:
        resource_list = [(r.id, f"{r.id}: {Contact.query.filter(Contact.id == r.contact_id).first().name}")
                         for r in Resource.query.filter(Resource.active == 1).all()]
    else:
        resource_list = [(r.id, f"{r.id}: {Contact.query.filter(Contact.id == r.contact_id).first().name}")
                         for r in Resource.query.all()]
    if resource_list:
        return resource_list
    else:
        return [(0, '')]


if __name__ == '__main__':
    app.run()
