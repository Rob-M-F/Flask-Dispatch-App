# RETRIEVE TAG
SELECT id, name, active FROM database.tag WHERE tag.id = tag_id;

# RETRIEVE ASSIGNED_TAG
SELECT id, object, type, tag, weight FROM database.assigned_tag WHERE assigned_tag.object = object_id AND assigned_tag.type = object_type;

# RETRIEVE ADDRESS
SELECT id, gps, number, street, city, state, country, postal, active FROM database.address WHERE address.id = address_id;
    # RETRIEVE ASSIGNED_TAG

# RETRIEVE CONTACT_METHOD
SELECT id, name FROM database.contact_method WHERE contact_method.id = method;

# RETRIEVE CONTACT_DETAILS
SELECT id, name, method FROM database.contact_details WHERE contact_details.id = details;
    # RETRIEVE CONTACT_METHOD

# RETRIEVE CONTACT_PRIORITY
SELECT id, contact, details, priority FROM database.contact_priority WHERE contact.id = contact_id;
    # RETRIEVE CONTACT_DETAILS

# RETRIEVE CONTACT
SELECT id, name, address, active FROM database.contact WHERE contact.id = contact_id;
    # RETRIEVE ADDRESS
    # RETRIEVE ASSIGNED_TAG
    # RETRIEVE CONTACT_PRIORITY

# RETRIEVE ASSIGNED_CONTACT
SELECT id, contact, object, type, priority FROM database.assigned_contact WHERE assigned_contact.object = object_id, AND assigned_contact.type = object_type;
    # RETRIEVE CONTACT

# RETRIEVE COMPANY
SELECT id, name, active FROM database.company WHERE company.id = company_id
    # RETRIEVE ASSIGNED_CONTACT
    # RETRIEVE ASSIGNED_TAG

# RETRIEVE SITE
SELECT id, name, address, company, active FROM database.site WHERE site.id = site_id;
    # RETRIEVE ADDRESS
    # RETRIEVE COMPANY
    # RETRIEVE ASSIGNED_CONTACT
    # RETRIEVE ASSIGNED_TAG

# RETRIEVE RESOURCE
SELECT id, name, title, contact, active FROM database.resource WHERE resource.id = resource_id;
    # RETRIEVE CONTACT
    # RETRIEVE ASSIGNED_TAG

# RETRIEVE PROBLEM_IDEAL_RESOURCE
SELECT id, problem, resource FROM database.problem_ideal_resource WHERE problem_ideal_resource.problem = problem_id;
    # RETRIEVE_RESOURCE

# RETRIEVE PROBLEM_RESOURCE
SELECT id, problem, resource, weight FROM database.problem_resource WHERE problem_resource.problem = problem_id;

# RETRIEVE PROBLEM
SELECT id, text, company, site, contact, authorizer, user, active FROM database.problem WHERE problem.id = problem_id;
    # RETRIEVE COMPANY
    # RETRIEVE SITE
    # RETRIEVE CONTACT
    # RETRIEVE CONTACT(AUTHORIZER)
    # RETRIEVE RESOURCE(USER)
    # RETRIEVE ASSIGNED_TAG(GENERATED)
    # RETRIEVE PROBLEM_RESOURCE
    # RETRIEVE PROBLEM_IDEAL_RESOURCE
