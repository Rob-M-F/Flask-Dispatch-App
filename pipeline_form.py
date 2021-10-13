from wtforms import StringField, BooleanField, FloatField, SubmitField, HiddenField, SelectField, TextAreaField
from wtforms import SelectMultipleField, PasswordField
from wtforms.validators import DataRequired, Email
from flask_wtf import FlaskForm


class PipelineInterface:
    @staticmethod
    class IndexForm(FlaskForm):
        add_task = SubmitField('Add Task')
        monitoring_menu = SubmitField('Monitoring')
        settings_menu = SubmitField('Settings')
        logout = SubmitField('Logout')

    @staticmethod
    class ReviewUser(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        email = StringField('email', validators=[DataRequired(), Email()])
        username = StringField('username', validators=[DataRequired()])
        password = PasswordField('password', validators=[DataRequired()])
        contact = SelectField('contact', validators=[DataRequired()], coerce=int)
        role = SelectMultipleField('role', validators=[DataRequired()], coerce=int)
        active = BooleanField('active')
        submit = SubmitField(label='Update')
        cancel = SubmitField(label='Cancel')

    @staticmethod
    class TaskAddForm(FlaskForm):
        company = SelectField(label='Company', validators=[DataRequired()], coerce=int)
        site = SelectField(label='Site', validators=[DataRequired()], coerce=int)
        authorizer = SelectField(label='Authorizer', validators=[DataRequired()], coerce=int)
        tag = SelectMultipleField(label='Select Tags', coerce=int)
        description = TextAreaField(label='Description', validators=[DataRequired()])
        submit = SubmitField(label='Create')
        cancel = SubmitField(label='Cancel')

    @staticmethod
    class DispatchForm(FlaskForm):
        close = SubmitField('Close')

    @staticmethod
    class DataInspectionForm(FlaskForm):
        close = SubmitField('Close')

    @staticmethod
    class ResourceReviewForm(FlaskForm):
        close = SubmitField('Close')

    @staticmethod
    class QueueMonitorForm(FlaskForm):
        close = SubmitField('Close')

    @staticmethod
    class SelectData(FlaskForm):
        address = SelectField("Address", coerce=int)
        address_add = SubmitField('Add')
        address_select = SubmitField('Review')
        company = SelectField("Company", coerce=int)
        company_add = SubmitField('Add')
        company_select = SubmitField('Review')
        contact = SelectField("Contact", coerce=int)
        contact_add = SubmitField('Add')
        contact_select = SubmitField('Review')
        resource = SelectField("Resource", coerce=int)
        resource_add = SubmitField('Add')
        resource_select = SubmitField('Review')
        site = SelectField("Site", coerce=int)
        site_add = SubmitField('Add')
        site_select = SubmitField('Review')
        tag = SelectField("Tag", coerce=int)
        tag_add = SubmitField('Add')
        tag_select = SubmitField('Review')
        task = SelectField("Task", coerce=int)
        task_add = SubmitField('Add')
        task_select = SubmitField('Review')

    @staticmethod
    class ManageUser(FlaskForm):
        user = SelectField("User", coerce=int)
        user_add = SubmitField('Add')
        user_select = SubmitField('Review')

    @staticmethod
    class ReviewAddress(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        number = StringField('Number', validators=[DataRequired()])
        street = StringField('Street', validators=[DataRequired()])
        city = StringField('City', validators=[DataRequired()])
        state = StringField('State', validators=[DataRequired()])
        postal = StringField('Postal', validators=[DataRequired()])
        country = StringField('Country', validators=[DataRequired()])
        active = BooleanField('Active')
        all_contacts = SelectField('All contacts', coerce=int)
        contacts = SelectField('Contacts', coerce=int)
        review_contact = SubmitField('Review')
        sites = SelectField('Sites', coerce=int)
        review_site = SubmitField('Review')
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')

    @staticmethod
    class ReviewResource(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        title = StringField('Title', validators=[DataRequired()])
        contact = SelectField('Contact', coerce=int)
        active = BooleanField('Active')
        can_dispatch = BooleanField('Can Dispatch')
        segment = SelectField('Segment', coerce=int)
        review_contact = SubmitField('Review')
        all_tags = SelectField('All tags', coerce=int)
        resource_tags = SelectField('Resource tags', coerce=int)
        tag_weight = FloatField('Tag weight')
        add_tag = SubmitField('Add')
        remove_tag = SubmitField('Remove')
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')

    @staticmethod
    class ReviewCompany(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        name = StringField('Name', validators=[DataRequired()])
        active = BooleanField('Active')
        sites = SelectField('Sites', coerce=int)
        all_contacts = SelectField('All contacts', coerce=int)
        contacts = SelectField('Contacts', coerce=int)
        authorizers = SelectField('Authorizers', coerce=int)
        add_authorizer = SubmitField('Authorizer')
        remove_authorizer = SubmitField('Remove')
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')
        review_site = SubmitField('Review')
        review_contact = SubmitField('Review')

    @staticmethod
    class ReviewSite(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        name = StringField('Name', validators=[DataRequired()])
        active = BooleanField('Active')
        address = SelectField('Address', coerce=int)
        company = SelectField('Company', coerce=int)
        all_contacts = SelectField('All contacts', coerce=int)
        contacts = SelectField('Contacts', coerce=int)
        tasks = SelectField('Tasks', coerce=int)
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')
        review_address = SubmitField('Review')
        review_company = SubmitField('Review')
        review_contact = SubmitField('Review')
        review_task = SubmitField('Review')

    @staticmethod
    class ReviewContact(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        name = StringField('Name', validators=[DataRequired()])
        address = SelectField('Address', validators=[DataRequired()], coerce=int)
        active = BooleanField('Active')
        resource = HiddenField('Resource')
        review_resource = SubmitField('Review Resource')
        sites = SelectField('Sites', coerce=int)
        companies = SelectField('Companies', coerce=int)
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')
        review_site = SubmitField('Review')
        review_company = SubmitField('Review')

    @staticmethod
    class ReviewTag(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        name = StringField('Name', validators=[DataRequired()])
        active = BooleanField('Active')
        segment = SelectField('Segment', coerce=int)
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')

    @staticmethod
    class ReviewTask(FlaskForm):
        id = HiddenField('ID', validators=[DataRequired()])
        company = SelectField(label='Company', validators=[DataRequired()], coerce=int)
        site = SelectField(label='Site', validators=[DataRequired()], coerce=int)
        authorizer = SelectField(label='Authorizer', validators=[DataRequired()], coerce=int)
        tag = SelectMultipleField(label='Select Tags', coerce=int)
        description = TextAreaField(label='Description', validators=[DataRequired()])
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')

    @staticmethod
    class VisualizeData(FlaskForm):
        validate_update = SubmitField('Update')
        validate_cancel = SubmitField('Cancel')

    @staticmethod
    class TaskGrader(FlaskForm):
        ungraded_tasks = SelectField(label='Ungraded Tasks', coerce=int)
        review_ungraded_task = SubmitField('Review')
        graded_tasks = SelectField(label='Graded Tasks', coerce=int)
        review_graded_task = SubmitField('Review')
        resources = SelectField(label='Resources', coerce=int)
        review_resource = SubmitField('Review')
        assign_ideal = SubmitField('Assign Ideal Resource')

    @staticmethod
    class DashboardForm(FlaskForm):
        resource_demand = SelectField(label='Resource Demand', coerce=int)
        review_resource_demand = SubmitField('Review')
        task_tags = SelectField(label='Task Tags', coerce=int)
        review_task_tags = SubmitField('Review')
        awaiting_dispatch = SelectField(label='Awaiting Dispatch', coerce=int)
        dispatch_task = SubmitField('Dispatch')

    @staticmethod
    class TaskDispatch(FlaskForm):
        resource = SelectField("Resource", coerce=int)
        review_resource = SubmitField('Review')
        task = TextAreaField("Task")
        review_task = SubmitField('Review')
        dispatch_task = SubmitField('Dispatch')

    @staticmethod
    class ReviewGrade(FlaskForm):
        id = HiddenField('ID')
        authorizer = StringField(label='Authorizer')
        review_authorizer = SubmitField('Review')
        company = StringField(label='Company')
        review_company = SubmitField('Review')
        site = StringField(label='Site')
        review_site = SubmitField('Review')
        description = TextAreaField(label='Description')
        ideal = StringField(label='Ideal Resource')
        review_ideal = SubmitField('Review')
        resource = SelectField(label='Resources', coerce=int)
        review_resource = SubmitField('Review')
        tag = SelectField(label='Tags', coerce=int)
        review_tag = SubmitField('Review')
        grade = StringField(label='Grade')
        back = SubmitField('Back')
