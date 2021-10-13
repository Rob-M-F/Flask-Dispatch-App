import mariadb
import random
import json

rank = {"Installer": 1, "Technician": 2, "Specialist": 3, "Engineer": 4, "Programmer": 5}

manuf = {15: 'Genetec', 17: 'Avigilon', 18: 'Lenel', 21: 'Honeywell'}

systems = {22: {'product': 'OnGuard', 'manuf': 18},
           20: {'product': 'Vindicator', 'manuf': 21},
           19:	{'product': 'ProWatch', 'manuf': 21},
           38: {'product': 'ACM', 'manuf': 17},
           39: {'product': 'Security Center', 'manuf': 15}}

scale = {23: 'Enterprise', 40: 'Regional', 41: 'Local'}

reader_tech = {7: 'magstripe', 26: 'prox', 27: 'smartcard', 28: 'FIPS', 29: 'MiFare', 32: 'iClass'}

door = {1: 'maglock', 2: 'strike', 3: 'mortise', 4:	'rex', 5: 'reader', 6: 'cardreader', 8: 'gate', 9: 'panel',
        34: 'rim', 33: 'delayed egress', 11: 'system'}


def update_value(tag_dict, key, label):
    generated = 1
    while generated < 0.1 or generated > 0.9:
        generated = random.gauss(0.75, 0.25)

    if key in tag_dict:
        tag_dict[key]['weight'] = 1 - (tag_dict[key]['weight'] * generated)
    else:
        tag_dict[key] = {'id': key, 'item': label, 'weight': generated}

    removal_list = list()
    for decay_tag in tag_dict:
        if decay_tag != key:
            tag_dict[decay_tag]['weight'] = tag_dict[decay_tag]['weight'] * 0.95
        if tag_dict[decay_tag]['weight'] < 0.1:
            removal_list.append(decay_tag)

    for removal_tag in removal_list:
        removed_value = tag_dict.pop(removal_tag)
        print(f"{removed_value['item']} ({removed_value['weight']:4.3f}) has decayed too far and will be removed.")

    return tag_dict[key]


def add_reader(tag_dict):
    reader_item = random.choice(list(reader_tech.keys()))
    tag_dict[reader_item] = update_value(tag_dict, reader_item, reader_tech[reader_item])

    return tag_dict


def add_scale(tag_dict):
    scale_item = random.choice(list(scale.keys()))
    tag_dict[scale_item] = update_value(tag_dict, scale_item, scale[scale_item])
    return tag_dict


def add_system(tag_dict):
    system_choice = random.choice(list(systems.keys()))
    system_item = systems[system_choice]['product']
    tag_dict[system_choice] = update_value(tag_dict, system_choice, system_item)

    manuf_choice = systems[system_choice]['manuf']
    manuf_item = manuf[manuf_choice]
    tag_dict[manuf_choice] = update_value(tag_dict, manuf_choice, manuf_item)

    return tag_dict


def get_installer_tags(tag_dict=None, tag_count=10):
    if tag_dict is None:
        tag_dict = dict()

    for i in range(tag_count):
        item_picked = random.choice(list(door.keys()))
        tag_dict[item_picked] = update_value(tag_dict, item_picked, door[item_picked])

        if item_picked in (5, 6):
            tag_dict = add_reader(tag_dict)

        if item_picked == 11:
            tag_dict = add_scale(tag_dict)

        if item_picked in (9, 11):
            tag_dict = add_system(tag_dict)
    return tag_dict


def get_service_tags(tech_rank=1, tag_dict=None, tag_count=None):
    rank_map = {"Installer": 1, "Technician": 2, "Specialist": 3, "Engineer": 4, "Programmer": 5}
    if tech_rank not in rank_map.values():
        if tech_rank in rank_map:
            tech_rank = rank_map[tech_rank]
        else:
            tech_rank = 1

    if tag_dict is None:
        tag_dict = dict()
    if tag_count is None:
        tag_count = tech_rank * 10
    for i in range(tag_count):
        tag_type = random.randrange(1, tech_rank*5)
        if tag_type < 5:
            tag_dict = get_installer_tags(tag_dict=tag_dict, tag_count=1)
        elif tag_type < 10:
            tag_dict[5] = update_value(tag_dict=tag_dict, key=5, label=door[5])
            tag_dict = add_reader(tag_dict)
        elif tag_type < 15:
            tag_dict[9] = update_value(tag_dict=tag_dict, key=9, label=door[9])
            tag_dict = add_system(tag_dict)
        else:
            tag_dict[11] = update_value(tag_dict=tag_dict, key=11, label=door[11])
            tag_dict = add_system(tag_dict)
            tag_dict = add_scale(tag_dict)
    return tag_dict


def make_sql_pool(file_name="database_string.config"):
    with open(file_name, 'r') as infile:
        data = json.load(infile)
    return mariadb.ConnectionPool(user=data['user'], password=data['password'], database=data['database'],
                                  host=data['host'], port=data['port'], pool_name="tag_generator", pool_size=20)


def set_assigned_tags(conn_pool, object_dict, object_type):
    data_template = "({}, {}, {}, {}), "
    data_string = ""
    for object_record in object_dict:
        for current_tag in object_dict[object_record]['tags']:
            object_id = object_dict[object_record]['id']
            print("Current Tag: ", object_dict[object_record]['tags'][current_tag])
            tag_id = object_dict[object_record]['tags'][current_tag]['id']
            tag_weight = object_dict[object_record]['tags'][current_tag]['weight']
            data_string = data_string + data_template.format(object_id, tag_id, tag_weight, object_type)
    statement = f"INSERT INTO assigned_tag (`object`, `tag`, `weight`, `type`) VALUES {data_string[:-2]};"
    try:
        conn = conn_pool.get_connection()
        conn_cursor = conn.cursor()
        conn_cursor.execute(statement)
    except mariadb.Error as e:
        message = f"Error connecting to MariaDB Platform: {e}"
        print(message)


def build_resource_tags(conn_pool):
    resources = dict()
    try:
        conn = conn_pool.get_connection()
        conn_cursor = conn.cursor()
        conn_cursor.execute("SELECT `id`, `title` FROM resource;")
        for row in conn_cursor:
            resources[row[0]] = {"id": row[0], "title": row[1]}
    except mariadb.Error as e:
        message = f"Error connecting to MariaDB Platform: {e}"
        print(message)

    for resource in resources:
        resource_dict = resources[resource]
        resources[resource]['tags'] = get_service_tags(tech_rank=resource_dict['title'])
        print("\n", resource_dict['title'], resource_dict['id'], "Tags:")
        for tag in resources[resource]['tags']:
            print(resources[resource]['tags'][tag])
    set_assigned_tags(conn_pool=sql_pool, object_dict=resources, object_type=1)


def build_site_tags(conn_pool):
    sites = dict()
    try:
        conn = conn_pool.get_connection()
        conn_cursor = conn.cursor()
        conn_cursor.execute("SELECT `id`, `name`, `company` FROM site;")
        for row in conn_cursor:
            sites[row[0]] = {"id": row[0], "name": row[1], "company": row[2]}
    except mariadb.Error as e:
        message = f"Error connecting to MariaDB Platform: {e}"
        print(message)
    for site in sites:
        tag_dict = dict()

        if random.randrange(0, 10) == 0:
            site_systems = random.randrange(1, 4)
        else:
            site_systems = 1

        for s in range(site_systems):
            tag_dict = add_scale(tag_dict)
            tag_dict = add_system(tag_dict)

        if random.randrange(0, 10) == 0:
            site_readers = random.randrange(1, 4)
        else:
            site_readers = 1
        for r in range(site_readers):
            tag_dict = add_reader(tag_dict)

        tech_tags = get_service_tags(tech_rank=1)
        for current_tag in tech_tags:
            if current_tag in door:
                tag_dict[current_tag] = tech_tags[current_tag]
        sites[site]['tags'] = tag_dict
    set_assigned_tags(conn_pool=sql_pool, object_dict=sites, object_type=2)


def build_company_tags(conn_pool):
    companies = dict()
    try:
        conn = conn_pool.get_connection()
        conn_cursor = conn.cursor()
        conn_cursor.execute("SELECT `id`, `name` FROM company;")
        for row in conn_cursor:
            companies[row[0]] = {"id": row[0], "name": row[1]}
    except mariadb.Error as e:
        message = f"Error connecting to MariaDB Platform: {e}"
        print(message)

    for company in companies:
        tag_dict = dict()

        if random.randrange(0, 25) == 0:
            company_systems = random.randrange(1, 4)
        else:
            company_systems = 1

        for c in range(company_systems):
            tag_dict = add_scale(tag_dict)
            tag_dict = add_system(tag_dict)

        if random.randrange(0, 25) == 0:
            company_readers = random.randrange(1, 4)
        else:
            company_readers = 1
        for r in range(company_readers):
            tag_dict = add_reader(tag_dict)

        tech_tags = get_service_tags(tech_rank=1)
        for current_tag in tech_tags:
            if current_tag in door:
                tag_dict[current_tag] = tech_tags[current_tag]
        companies[company]['tags'] = tag_dict
    set_assigned_tags(conn_pool=conn_pool, object_dict=companies, object_type=3)


sql_pool = make_sql_pool()
result_list = []
try:
    conn = sql_pool.get_connection()
    conn_cursor = conn.cursor()
    conn_cursor.execute("SELECT a.id, (a.weight + b.weight) / 2 as "
                        "ave_weight FROM service_corp.assigned_tag AS a JOIN service_corp.assigned_tag AS b ON "
                        "a.object = b.object WHERE a.`type` = b.`type` AND a.tag = 38 AND b.tag = 17;")
    for row in conn_cursor:
        result_list.append((row[0], float(row[1])))
except mariadb.Error as e:
    message = f"Error connecting to MariaDB Platform: {e}"
    print(message)
#  build_resource_tags(sql_pool)
#  build_site_tags(sql_pool)
#  build_company_tags(sql_pool)
try:
    conn = sql_pool.get_connection()
    conn_cursor = conn.cursor()
    for line in result_list:
        conn_cursor.execute(f"UPDATE service_corp.assigned_tag SET `weight` = {line[1]} WHERE `id` = {line[0]};")
except mariadb.Error as e:
    message = f"Error connecting to MariaDB Platform: {e}"
    print(message)

site_list = [23, 31, 31, 35, 41, 41, 41, 44, 44, 46, 63, 63, 75, 75, 88, 100, 123, 123, 127, 130, 180, 180, 184, 192,
             192, 197, 200, 200, 204, 238, 240, 240, 242, 242, 243, 256, 271, 289, 289, 293, 298]



