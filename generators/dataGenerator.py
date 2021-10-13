from ParsingPipeline.pipeline import DatabaseConnection
from random import randint


class DataGenerator:
    postal_city_state = {
        37302: {"city": "Apison", "state": "Tennessee"}, 37304: {"city": "Bakewell", "state": "Tennessee"},
        37308: {"city": "Birchwood", "state": "Tennessee"}, 37341: {"city": "Harrison", "state": "Tennessee"},
        37343: {"city": "Hixson", "state": "Tennessee"}, 37350: {"city": "Lookout Mountain", "state": "Tennessee"},
        37351: {"city": "Lupton City", "state": "Tennessee"}, 37363: {"city": "Ooltewah", "state": "Tennessee"},
        37373: {"city": "Sale Creek", "state": "Tennessee"}, 37377: {"city": "Signal Mountain", "state": "Tennessee"},
        37379: {"city": "Soddy Daisy", "state": "Tennessee"}, 37402: {"city": "Chattanooga", "state": "Tennessee"},
        37403: {"city": "Chattanooga", "state": "Tennessee"}, 37404: {"city": "Chattanooga", "state": "Tennessee"},
        37405: {"city": "Chattanooga", "state": "Tennessee"}, 37406: {"city": "Chattanooga", "state": "Tennessee"},
        37407: {"city": "Chattanooga", "state": "Tennessee"}, 37408: {"city": "Chattanooga", "state": "Tennessee"},
        37409: {"city": "Chattanooga", "state": "Tennessee"}, 37410: {"city": "Chattanooga", "state": "Tennessee"},
        37411: {"city": "Chattanooga", "state": "Tennessee"}, 37412: {"city": "Chattanooga", "state": "Tennessee"},
        37415: {"city": "Chattanooga", "state": "Tennessee"}, 37416: {"city": "Chattanooga", "state": "Tennessee"},
        37419: {"city": "Chattanooga", "state": "Tennessee"}, 37421: {"city": "Chattanooga", "state": "Tennessee"},
        37450: {"city": "Chattanooga", "state": "Tennessee"}}

    max_street_nums = 9999

    street_names = [
        "First", "Second", "Third", "Fourth", "Fifth", "Sixth", "Seventh", "Eighth", "Ninth", "Tenth", "Oak", "Fir",
        "Maple", "Pine", "Cedar", "Elm", "Spruce", "Main", "Park", "Acacia", "Eucalyptus", "Washington", "Hamilton",
        "Lincoln", "Franklin", "Cherry", "Grove", "Dogwood", "Palm"]

    street_extensions = ["Street", "Lane", "Boulevard", "Parkway", "Highway", "Road", "Avenue"]

    company_names = [
        "Incorporated", "LLC", "Company", "Store", "Offices", "Partners", "Logistics", "Restaurant", "Hotel", "Shoppe",
        "Warehousing", ]

    first_names = [
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David",
        "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Christopher",
        "Nancy", "Daniel", "Margaret", "Matthew", "Lisa", "Anthony", "Betty", "Donald", "Dorothy", "Mark", "Sandra",
        "Paul",
        "Ashley", "Steven", "Kimberly", "Andrew", "Donna", "Kenneth", "Emily", "Joshua", "Michelle", "George", "Carol",
        "Kevin", "Amanda", "Brian", "Melissa", "Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca",
        "Jason", "Laura", "Jeffrey", "Sharon", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Helen", "Nicholas",
        "Amy", "Eric", "Shirley", "Stephen", "Angela", "Jonathan", "Anna", "Larry", "Brenda", "Justin", "Pamela",
        "Scott", "Nicole",
        "Brandon", "Ruth", "Frank", "Katherine", "Benjamin", "Samantha", "Gregory", "Christine", "Samuel", "Emma",
        "Raymond", "Catherine", "Patrick", "Debra", "Alexander", "Virginia", "Jack", "Rachel", "Dennis", "Carolyn",
        "Jerry", "Janet", "Tyler", "Maria", "Aaron", "Heather", "Jose", "Diane", "Henry", "Julie", "Douglas", "Joyce",
        "Adam", "Victoria", "Peter", "Kelly", "Nathan", "Christina", "Zachary", "Joan", "Walter", "Evelyn", "Kyle",
        "Lauren",
        "Harold", "Judith", "Carl", "Olivia", "Jeremy", "Frances", "Keith", "Martha", "Roger", "Cheryl", "Gerald",
        "Megan",
        "Ethan", "Andrea", "Arthur", "Hannah", "Terry", "Jacqueline", "Christian", "Ann", "Sean", "Jean", "Lawrence",
        "Alice", "Austin", "Kathryn", "Joe", "Gloria", "Noah", "Teresa", "Jesse", "Doris", "Albert", "Sara", "Bryan",
        "Janice", "Billy", "Julia", "Bruce", "Marie", "Willie", "Madison", "Jordan", "Grace", "Dylan", "Judy", "Alan",
        "Theresa", "Ralph", "Beverly", "Gabriel", "Denise", "Roy", "Marilyn", "Juan", "Amber", "Wayne", "Danielle",
        "Eugene", "Abigail", "Logan", "Brittany", "Randy", "Rose", "Louis", "Diana", "Russell", "Natalie", "Vincent",
        "Sophia", "Philip", "Alexis", "Bobby", "Lori", "Johnny", "Kayla", "Bradley", "Jane"]

    surnames = [
        "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas",
        "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez",
        "Lewis",
        "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", "King", "Wright", "Lopez", "Hill", "Scott", "Green",
        "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips",
        "Campbell",
        "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez", "Morris", "Rogers", "Reed", "Cook", "Morgan",
        "Bell",
        "Murphy", "Bailey", "Rivera", "Cooper", "Richardson", "Cox", "Howard", "Ward", "Torres", "Peterson", "Gray",
        "Ramirez", "James", "Watson", "Brooks", "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross",
        "Henderson", "Coleman", "Jenkins", "Perry", "Powell", "Long", "Patterson", "Hughes", "Flores", "Washington",
        "Butler", "Simmons", "Foster", "Gonzales", "Bryant", "Alexander", "Russell", "Griffin", "Diaz", "Hayes",
        "Myers",
        "Ford", "Hamilton", "Graham", "Sullivan", "Wallace", "Woods", "Cole", "West", "Jordan", "Owens", "Reynolds",
        "Fisher", "Ellis", "Harrison", "Gibson", "Mcdonald", "Cruz", "Marshall", "Ortiz", "Gomez", "Murray", "Freeman",
        "Wells", "Webb", "Simpson", "Stevens", "Tucker", "Porter", "Hunter", "Hicks", "Crawford", "Henry", "Boyd",
        "Mason",
        "Morales", "Kennedy", "Warren", "Dixon", "Ramos", "Reyes", "Burns", "Gordon", "Shaw", "Holmes", "Rice",
        "Robertson",
        "Hunt", "Black", "Daniels", "Palmer", "Mills", "Nichols", "Grant", "Knight", "Ferguson", "Rose", "Stone",
        "Hawkins",
        "Dunn", "Perkins", "Hudson", "Spencer", "Gardner", "Stephens", "Payne", "Pierce", "Berry", "Matthews", "Arnold",
        "Wagner", "Willis", "Ray", "Watkins", "Olson", "Carroll", "Duncan", "Snyder", "Hart", "Cunningham", "Bradley",
        "Lane", "Andrews", "Ruiz", "Harper", "Fox", "Riley", "Armstrong", "Carpenter", "Weaver", "Greene", "Lawrence",
        "Elliott", "Chavez", "Sims", "Austin", "Peters", "Kelley", "Franklin", "Lawson", "Fields", "Gutierrez", "Ryan",
        "Schmidt", "Carr", "Vasquez", "Castillo", "Wheeler", "Chapman", "Oliver", "Montgomery", "Richards",
        "Williamson",
        "Johnston", "Banks", "Meyer", "Bishop", "Mccoy", "Howell", "Alvarez", "Morrison", "Hansen", "Fernandez",
        "Garza",
        "Harvey", "Little", "Burton", "Stanley", "Nguyen", "George", "Jacobs", "Reid", "Kim", "Fuller", "Lynch", "Dean",
        "Gilbert", "Garrett", "Romero", "Welch", "Larson", "Frazier", "Burke", "Hanson", "Day", "Mendoza", "Moreno",
        "Bowman", "Medina", "Fowler", "Brewer", "Hoffman", "Carlson", "Silva", "Pearson", "Holland", "Douglas",
        "Fleming",
        "Jensen", "Vargas", "Byrd", "Davidson", "Hopkins", "May", "Terry", "Herrera", "Wade", "Soto", "Walters",
        "Curtis",
        "Neal", "Caldwell", "Lowe", "Jennings", "Barnett", "Graves", "Jimenez", "Horton", "Shelton", "Barrett",
        "Obrien",
        "Castro", "Sutton", "Gregory", "Mckinney", "Lucas", "Miles", "Craig", "Rodriquez", "Chambers", "Holt",
        "Lambert",
        "Fletcher", "Watts", "Bates", "Hale", "Rhodes", "Pena", "Beck", "Newman", "Haynes", "Mcdaniel", "Mendez",
        "Bush",
        "Vaughn", "Parks", "Dawson", "Santiago", "Norris", "Hardy", "Love", "Steele", "Curry", "Powers", "Schultz",
        "Barker", "Guzman", "Page", "Munoz", "Ball", "Keller", "Chandler", "Weber", "Leonard", "Walsh", "Lyons",
        "Ramsey",
        "Wolfe", "Schneider", "Mullins", "Benson", "Sharp", "Bowen", "Daniel", "Barber", "Cummings", "Hines", "Baldwin",
        "Griffith", "Valdez", "Hubbard", "Salazar", "Reeves", "Warner", "Stevenson", "Burgess", "Santos", "Tate",
        "Cross",
        "Garner", "Mann", "Mack", "Moss", "Thornton", "Dennis", "Mcgee", "Farmer", "Delgado", "Aguilar", "Vega",
        "Glover",
        "Manning", "Cohen", "Harmon", "Rodgers", "Robbins", "Newton", "Todd", "Blair", "Higgins", "Ingram", "Reese",
        "Cannon", "Strickland", "Townsend", "Potter", "Goodwin", "Walton", "Rowe", "Hampton", "Ortega", "Patton",
        "Swanson",
        "Joseph", "Francis", "Goodman", "Maldonado", "Yates", "Becker", "Erickson", "Hodges", "Rios", "Conner",
        "Adkins",
        "Webster", "Norman", "Malone", "Hammond", "Flowers", "Cobb", "Moody", "Quinn", "Blake", "Maxwell", "Pope",
        "Floyd",
        "Osborne", "Paul", "Mccarthy", "Guerrero", "Lindsey", "Estrada", "Sandoval", "Gibbs", "Tyler", "Gross",
        "Fitzgerald", "Stokes", "Doyle", "Sherman", "Saunders", "Wise", "Colon", "Gill", "Alvarado", "Greer", "Padilla",
        "Simon", "Waters", "Nunez", "Ballard", "Schwartz", "Mcbride", "Houston", "Christensen", "Klein", "Pratt",
        "Briggs",
        "Parsons", "Mclaughlin", "Zimmerman", "French", "Buchanan", "Moran", "Copeland", "Roy", "Pittman", "Brady",
        "Mccormick", "Holloway", "Brock", "Poole", "Frank", "Logan", "Owen", "Bass", "Marsh", "Drake", "Wong",
        "Jefferson",
        "Park", "Morton", "Abbott", "Sparks", "Patrick", "Norton", "Huff", "Clayton", "Massey", "Lloyd", "Figueroa",
        "Carson", "Bowers", "Roberson", "Barton", "Tran", "Lamb", "Harrington", "Casey", "Boone", "Cortez", "Clarke",
        "Mathis", "Singleton", "Wilkins", "Cain", "Bryan", "Underwood", "Hogan", "Mckenzie", "Collier", "Luna",
        "Phelps",
        "Mcguire", "Allison", "Bridges", "Wilkerson", "Nash", "Summers", "Atkins", "Wilcox", "Pitts", "Conley",
        "Marquez",
        "Burnett", "Richard", "Cochran", "Chase", "Davenport", "Hood", "Gates", "Clay", "Ayala", "Sawyer", "Roman",
        "Vazquez", "Dickerson", "Hodge", "Acosta", "Flynn", "Espinoza", "Nicholson", "Monroe", "Wolf", "Morrow", "Kirk",
        "Randall", "Anthony", "Whitaker", "Oconnor", "Skinner", "Ware", "Molina", "Kirby", "Huffman", "Bradford",
        "Charles",
        "Gilmore", "Dominguez", "Oneal", "Bruce", "Lang", "Combs", "Kramer", "Heath", "Hancock", "Gallagher", "Gaines",
        "Shaffer", "Short", "Wiggins", "Mathews", "Mcclain", "Fischer", "Wall", "Small", "Melton", "Hensley", "Bond",
        "Dyer", "Cameron", "Grimes", "Contreras", "Christian", "Wyatt", "Baxter", "Snow", "Mosley", "Shepherd",
        "Larsen",
        "Hoover", "Beasley", "Glenn", "Petersen", "Whitehead", "Meyers", "Keith", "Garrison", "Vincent", "Shields",
        "Horn",
        "Savage", "Olsen", "Schroeder", "Hartman", "Woodard", "Mueller", "Kemp", "Deleon", "Booth", "Patel", "Calhoun",
        "Wiley", "Eaton", "Cline", "Navarro", "Harrell", "Lester", "Humphrey", "Parrish", "Duran", "Hutchinson", "Hess",
        "Dorsey", "Bullock", "Robles", "Beard", "Dalton", "Avila", "Vance", "Rich", "Blackwell", "York", "Johns",
        "Blankenship", "Trevino", "Salinas", "Campos", "Pruitt", "Moses", "Callahan", "Golden", "Montoya", "Hardin",
        "Guerra", "Mcdowell", "Carey", "Stafford", "Gallegos", "Henson", "Wilkinson", "Booker", "Merritt", "Miranda",
        "Atkinson", "Orr", "Decker", "Hobbs", "Preston", "Tanner", "Knox", "Pacheco", "Stephenson", "Glass", "Rojas",
        "Serrano", "Marks", "Hickman", "English", "Sweeney", "Strong", "Prince", "Mcclure", "Conway", "Walter", "Roth",
        "Maynard", "Farrell", "Lowery", "Hurst", "Nixon", "Weiss", "Trujillo", "Ellison", "Sloan", "Juarez", "Winters",
        "Mclean", "Randolph", "Leon", "Boyer", "Villarreal", "Mccall", "Gentry", "Carrillo", "Kent", "Ayers", "Lara",
        "Shannon", "Sexton", "Pace", "Hull", "Leblanc", "Browning", "Velasquez", "Leach", "Chang", "House", "Sellers",
        "Herring", "Noble", "Foley", "Bartlett", "Mercado", "Landry", "Durham", "Walls", "Barr", "Mckee", "Bauer",
        "Rivers",
        "Everett", "Bradshaw", "Pugh", "Velez", "Rush", "Estes", "Dodson", "Morse", "Sheppard", "Weeks", "Camacho",
        "Bean",
        "Barron", "Livingston", "Middleton", "Spears", "Branch", "Blevins", "Chen", "Kerr", "Mcconnell", "Hatfield",
        "Harding", "Ashley", "Solis", "Herman", "Frost", "Giles", "Blackburn", "William", "Pennington", "Woodward",
        "Finley", "Mcintosh", "Koch", "Best", "Solomon", "Mccullough", "Dudley", "Nolan", "Blanchard", "Rivas",
        "Brennan",
        "Mejia", "Kane", "Benton", "Joyce", "Buckley", "Haley", "Valentine", "Maddox", "Russo", "Mcknight", "Buck",
        "Moon",
        "Mcmillan", "Crosby", "Berg", "Dotson", "Mays", "Roach", "Church", "Chan", "Richmond", "Meadows",
        "Faulkner", "Oneill", "Knapp", "Kline", "Barry", "Ochoa", "Jacobson", "Gay", "Avery", "Hendricks", "Horne",
        "Shepard", "Hebert", "Cherry", "Cardenas", "Mcintyre", "Whitney", "Waller", "Holman", "Donaldson", "Cantu",
        "Terrell", "Morin", "Gillespie", "Fuentes", "Tillman", "Sanford", "Bentley", "Peck", "Key", "Salas",
        "Rollins", "Gamble", "Dickson", "Battle", "Santana", "Cabrera", "Cervantes", "Howe", "Hinton", "Hurley",
        "Spence", "Zamora", "Yang", "Mcneil", "Suarez", "Case", "Petty", "Gould", "Mcfarland", "Sampson", "Carver",
        "Bray", "Rosario", "Macdonald", "Stout", "Hester", "Melendez", "Dillon", "Farley", "Hopper", "Galloway",
        "Potts", "Bernard", "Joyner", "Stein", "Aguirre", "Osborn", "Mercer", "Bender", "Franco", "Rowland",
        "Sykes", "Benjamin", "Travis", "Pickett", "Crane", "Sears", "Mayo", "Dunlap", "Hayden", "Wilder", "Mckay",
        "Coffey", "Mccarty", "Ewing", "Cooley", "Vaughan", "Bonner", "Cotton", "Holder", "Stark", "Ferrell",
        "Cantrell", "Fulton", "Lynn", "Lott", "Calderon", "Rosa", "Pollard", "Hooper", "Burch", "Mullen", "Fry",
        "Riddle", "Levy", "David", "Duke", "Odonnell", "Guy", "Michael", "Britt", "Frederick", "Daugherty",
        "Berger", "Dillard", "Alston", "Jarvis", "Frye", "Riggs", "Chaney", "Odom", "Duffy", "Fitzpatrick",
        "Valenzuela", "Merrill", "Mayer", "Alford", "Mcpherson", "Acevedo", "Donovan", "Barrera", "Albert", "Cote",
        "Reilly", "Compton", "Raymond", "Mooney", "Mcgowan", "Craft", "Cleveland", "Clemons", "Wynn", "Nielsen",
        "Baird", "Stanton", "Snider", "Rosales", "Bright", "Witt", "Stuart", "Hays", "Holden", "Rutledge", "Kinney",
        "Clements", "Castaneda", "Slater", "Hahn", "Emerson", "Conrad", "Burks", "Delaney", "Pate", "Lancaster",
        "Sweet", "Justice", "Tyson", "Sharpe", "Whitfield", "Talley", "Macias", "Irwin", "Burris", "Ratliff",
        "Mccray", "Madden", "Kaufman", "Beach", "Goff", "Cash", "Bolton", "Mcfadden", "Levine", "Good", "Byers",
        "Kirkland", "Kidd", "Workman", "Carney", "Dale", "Mcleod", "Holcomb", "England", "Finch", "Head", "Burt",
        "Hendrix", "Sosa", "Haney", "Franks", "Sargent", "Nieves", "Downs", "Rasmussen", "Bird", "Hewitt",
        "Lindsay", "Le", "Foreman", "Valencia", "Oneil", "Delacruz", "Vinson", "Dejesus", "Hyde", "Forbes",
        "Gilliam", "Guthrie", "Wooten", "Huber", "Barlow", "Boyle", "Mcmahon", "Buckner", "Rocha", "Puckett",
        "Langley", "Knowles", "Cooke", "Velazquez", "Whitley", "Noel", "Vang"]

    def __init__(self, tag_list, contact_method_list, contact_detail_list=None, contact_priority_list=None,
                 address_list=None, company_list=None, site_list=None, contacts_list=None, resource_list=None,
                 problem_list=None):
        self.tags = tag_list
        self.contact_methods = contact_method_list
        self.contact_details = contact_detail_list
        self.contact_priorities = contact_priority_list
        self.addresses = address_list
        self.companies = company_list
        self.sites = site_list
        self.contacts = contacts_list
        self.resources = resource_list
        self.problems = problem_list

    @staticmethod
    def random_select(choices=2):
        return randint(0, choices - 1)

    @staticmethod
    def random_from_dict(choice_dict):
        return list(choice_dict.keys())[DataGenerator.random_select(len(choice_dict))]

    def address_generator(self):
        if self.addresses is None:
            print("No addresses in the database, generating...")
            house_number = self.random_select(self.max_street_nums)
            street_name = self.street_names[self.random_select(len(self.street_names))]
            street_ext = self.street_extensions[self.random_select(len(self.street_extensions))]
            postal_codes = [key for key in self.postal_city_state.keys()]
            postal = postal_codes[self.random_select(len(postal_codes))]
            return {
                "id": None,
                "gps": None,
                "number": house_number,
                "street": f"{street_name} {street_ext}",
                "city": self.postal_city_state[postal]["city"],
                "state": self.postal_city_state[postal]["state"],
                "postal": postal,
                "active": True
            }
        else:
            print(f"Found {len(self.addresses)} addresses in the database, selecting...")
            return self.addresses[self.random_from_dict(self.addresses)]

    def tag_generator(self):
        return self.tags[self.random_from_dict(self.tags)]

    def contact_method_generator(self):
        # Return randomly selected existing method
        return self.contact_methods[self.random_from_dict(self.contact_methods)]

    def contact_details_generator(self):
        if self.contact_details is None:
            segment1 = randint(100, 999)
            segment2 = randint(100, 999)
            segment3 = randint(1000, 9999)
            return {
                "id": None,
                "name": f"{segment1}-{segment2}-{segment3}",
                "method": self.contact_method_generator()
            }
        else:
            return self.contact_details[self.random_from_dict(self.contact_details)]

    def first_name_generator(self):
        return self.first_names[self.random_select(len(self.first_names))]

    def surname_generator(self):
        return self.surnames[self.random_select(len(self.surnames))]

    def person_name_generator(self):
        names = [self.first_name_generator()]
        for _ in range(self.random_select(3)):
            if self.random_select() > 0:
                names.append(self.surname_generator())
            else:
                names.append(self.first_name_generator())
        names.append(self.surname_generator())
        return " ".join(names)

    def company_name_generator(self):
        name = self.surname_generator()
        company_name = self.company_names[self.random_select(len(self.company_names))]
        return f"{name} {company_name}"

    def company_generator(self):
        if self.companies is None:
            name = self.company_name_generator()
            return {
                "id": None,
                "name": name,
                "active": True
            }
        else:
            return self.companies[self.random_from_dict(self.companies)]

    def site_generator(self):
        if self.sites is None:
            return {
                "id": None,
                "name": self.company_name_generator(),
                "address": self.address_generator(),
                "company": self.company_generator(),
                "active": True
            }
        else:
            return self.sites[self.random_from_dict(self.sites)]

    def contact_generator(self):
        if self.contacts is None:
            return {
                "id": None,
                "name": self.person_name_generator(),
                "address": self.address_generator(),
                "active": True}
        else:
            return self.contacts[self.random_from_dict(self.contacts)]

    def contact_priorities_generator(self, contact_dict=None):
        if contact_dict is None:
            return {"id": None,
                    "contact": self.contact_generator(),
                    "method": self.contact_details(),
                    "priority": randint(-16, 16),
                    "active": 1}

    def resource_generator(self):
        if self.resources is None:
            return {
                "id": None,
                "title": "placeholder_job_title",
                "contact": self.contact_generator(),
                "dispatch": 1,
                "active": 1
            }
        else:
            return self.resources[self.random_from_dict(self.resources)]

    def problem_generator(self):
        if self.problems is None:
            return {
                "id": None,
                "text": "placeholder_problem_text",
                "company": self.company_generator(),
                "site": self.site_generator(),
                "contact": self.contact_generator(),
                "authorizer": self.contact_generator(),
                "user": self.resource_generator(),
                "active": True
            }
        else:
            return self.problems[self.random_from_dict(self.problems)]


if __name__ == "__main__":
    import json
    with open("database_string.config", 'r') as infile:
        data = json.load(infile)
    data_conn = DatabaseConnection(host=data['host'],
                                   port=data['port'],
                                   database=data['database'],
                                   user=data['user'],
                                   password=data['password'])
    tags = data_conn.get_object(object_type='tag')
    print("Tag listing: ", tags)

    contact_methods = data_conn.get_object(object_type='contact_method')
    print("Contact method listing: ", contact_methods)

    contact_details = data_conn.get_object(object_type='contact_details')
    for detail in contact_details:
        contact_details[detail] = data_conn.get_object(object_type='contact_details', object_id=detail)
    print("Contact details listing: ", contact_details)

    addresses = data_conn.get_object(object_type='address')
    print("Address listing: ", addresses)

    companies = data_conn.get_object(object_type='company')
    print("Company listing: ", companies)

    sites = data_conn.get_object(object_type='site')
    for site in sites:
        sites[site] = data_conn.get_object(object_type='site', object_id=site)
    print("Site listing: ", sites)

    contacts = data_conn.get_object(object_type='contact')
    for contact in contacts:
        contacts[contact] = data_conn.get_object(object_type='contact', object_id=contact)
    print("Contact listing: ", contacts)

    resources = data_conn.get_object(object_type='resource')
    for resource in resources:
        resources[resource] = data_conn.get_object(object_type='resource', object_id=resource)
    print("Resource listing: ", resources)

    problems = data_conn.get_object(object_type='problem')
    for problem in problems:
        problems[problem] = data_conn.get_object(object_type='problem', object_id=problem)
    print("Problem listing: ", problems)

    gen = DataGenerator(tag_list=tags,
                        contact_method_list=contact_methods,
                        contact_detail_list=contact_details if contact_details else None,
                        address_list=addresses if addresses else None,
                        company_list=companies if companies else None,
                        site_list=sites if sites else None,
                        contacts_list=contacts if contacts else None,
                        resource_list=resources if resources else None,
                        problem_list=problems if problems else None
                        )
    for _ in range(1000):
        value_set = f"'{details['name']}', {details['method']['id']}"
        statement = f"INSERT INTO contact_details(name, method) VALUES({value_set})"
        print(statement)
        #data_conn.execute_command(statement=statement)
