
# Data Wrangling Project
# the region selected was 'bengaluru, India' which is my hometown.
 # The dataset is as large as 600+MB. And there were a few challenges I had to face while cleaning the data, which is described below.

# Explaination at the bottom

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
import sqlite3
import cerberus
import schema

OSM_PATH = "bengaluru_india.osm"
NODES_PATH = "bangalore/node.csv"
NODE_TAGS_PATH = "bangalore/node_tags.csv"
WAYS_PATH = "bangalore/way.csv"
WAY_NODES_PATH = "bangalore/way_nodes.csv"
WAY_TAGS_PATH = "bangalore/way_tags.csv"

# VALIDATION PROCESS

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

# Fixing street names
def value_fixer(value):
#     Make all upper/lower case of 'road|cross|main' to Road
    re_road = re.compile(re.escape('road'), re.IGNORECASE)
    re_main = re.compile(re.escape('main'), re.IGNORECASE)
    re_cross = re.compile(re.escape('cross'), re.IGNORECASE)
    if re.search('road', value, re.IGNORECASE):
        value = re_road.sub('Road',value)
    if re.search('cross', value, re.IGNORECASE):
        value = re_road.sub('Cross',value)
    if re.search('main', value, re.IGNORECASE):
        value = re_road.sub('Main',value)

#     Fix Rd or Rd. to Road
#     Added a space so as to avoid considering 'rd' as a part of '3rd' and only taking abc rd.
    re_rd_period = re.compile(re.escape(' rd\.'), re.IGNORECASE)
    re_rd = re.compile(re.escape(' rd'), re.IGNORECASE)
    if re.search(' rd\.', value, re.IGNORECASE):
        value = re_rd_period.sub(' Road', value)
    elif re.search(' rd', value, re.IGNORECASE):
        value = re_rd.sub(' Road', value)


#     Fix mn or mn. to Main
    re_mn_period = re.compile(re.escape('mn.'), re.IGNORECASE)
    re_mn = re.compile(re.escape('mn'), re.IGNORECASE)
    if 'Main' not in value:
        if re.search('th mn\.', value, re.IGNORECASE) or re.search('st mn\.', value, re.IGNORECASE) or re.search('nd mn\.', value, re.IGNORECASE) or re.search('rd mn\.', value, re.IGNORECASE):
            value = re_mn_period.sub('Main', value)
        elif re.search('th mn', value, re.IGNORECASE) or re.search('st mn', value, re.IGNORECASE) or re.search('nd mn', value, re.IGNORECASE) or re.search('rd mn', value, re.IGNORECASE):
            value = re_mn.sub('Main', value)
    if 'Main Road' not in value:
        value = value.replace('Main','Main Road')

#     Fix crs or Crs. or cros to Cross
    re_crs_period = re.compile(re.escape('crs.'), re.IGNORECASE)
    re_crs = re.compile(re.escape('crs'), re.IGNORECASE)
    re_cros = re.compile(re.escape('cros'), re.IGNORECASE)
    if 'Cross' not in value:
        if re.search('th crs\.', value, re.IGNORECASE) or re.search('st crs\.', value, re.IGNORECASE) or re.search('nd crs\.', value, re.IGNORECASE) or re.search('rd crs\.', value, re.IGNORECASE):
            value = re_crs_period.sub('Cross', value)
        elif re.search('th crs', value, re.IGNORECASE) or re.search('st crs', value, re.IGNORECASE) or re.search('nd crs', value, re.IGNORECASE) or re.search('rd crs', value, re.IGNORECASE):
            value = re_crs.sub('Cross', value)
        elif re.search('cros', value, re.IGNORECASE):
            value = re_crs.sub('Cross', value)
    if 'Cross Road' not in value:
        value = value.replace('Cross','Cross Road')

#     Remove everything after 'Road'. I.e. Removing area name for roads. Like 4th Main Road, Banashankri should become 4th Main Road.
    if 'Road' in value:
        value=value[0:value.index('Road')+4]
    return value


# Fixing Key values that are not so good

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE HERE
    count = 0
    count2 = 0
    if element.tag == 'node':
        node_attribs= {
            'id': int(element.attrib['id']),
            'user': element.attrib['user'].encode('utf-8').strip(),
            'uid': int(element.attrib['uid']),
            'version': element.attrib['version'].encode('utf-8').strip(),
            'lat': float(element.attrib['lat']),
            'lon': float(element.attrib['lon']),
            'timestamp': element.attrib['timestamp'].encode('utf-8').strip(),
            'changeset': int(element.attrib['changeset']),
        }
        for t in element:
            if t.tag=='tag':
#                 Pt. 1 and 2 of 'Key values that are not so good'
                if t.attrib['k'] is not 'created_by':
                    t.attrib['v'].replace('_',' ')
#                 We don't want to make name as :kn and type as 'name', so we make an alternative condition.
                if ':kn' not in t.attrib['k']:
                    if ':' in t.attrib['k']:
                        type = t.attrib['k'][0:t.attrib['k'].index(':')]
                        key = t.attrib['k'][t.attrib['k'].index(':')+1:]
                    else:
                        type = 'regular'
                        key = t.attrib['k']
                    value = value_fixer(t.attrib['v'].encode('utf-8').strip())
                    tags.append({
                        'id':int(element.attrib['id']),
                        'key': key.encode('utf-8').strip(),
                        'value': value,
                        'type': type.encode('utf-8').strip()
                        })
                else:
                    if ':' in t.attrib['k'][:t.attrib['k'].index(':kn')]:
                        type = t.attrib['k'][0:t.attrib['k'].index(':')]
                        key = t.attrib['k'][t.attrib['k'].index(':')+1:]
                    else:
                        type = 'regular'
                        key = t.attrib['k']
                    value = value_fixer(t.attrib['v'].encode('utf-8').strip())
                    tags.append({
                        'id':int(element.attrib['id']),
                        'key': key.encode('utf-8').strip(),
                        'value': value,
                        'type': type.encode('utf-8').strip()
                        })
        result = {'node': node_attribs, 'node_tags': tags}
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        way_attribs= {
            'id': int(element.attrib['id']),
            'user': element.attrib['user'].encode('utf-8').strip(),
            'uid': int(element.attrib['uid']),
            'version': element.attrib['version'].encode('utf-8').strip(),
            'timestamp': str(element.attrib['timestamp']),
            'changeset': int(element.attrib['changeset']),
        }
        n=0
        for t in element:
            if t.tag=='tag':
#                 Pt. 1 and 2 of 'Key values that are not so good'
                if t.attrib['k'] is not 'created_by':
                    t.attrib['v'].replace('_',' ')
                if ':kn' not in t.attrib['k']:
                    if ':' in t.attrib['k']:
                        type = t.attrib['k'][0:t.attrib['k'].index(':')]
                        key = t.attrib['k'][t.attrib['k'].index(':')+1:]
                    else:
                        type = 'regular'
                        key = t.attrib['k']
                    value = value_fixer(t.attrib['v'].encode('utf-8').strip())
                    tags.append({
                        'id':int(element.attrib['id']),
                        'key': key.encode('utf-8').strip(),
                        'value': value,
                        'type': type.encode('utf-8').strip()
                        })
                else:
#                     If tag is of local language kannada, then :kn must not be in the key. 
                    if ':' in t.attrib['k'][:t.attrib['k'].index(':kn')]:
                        type = t.attrib['k'][0:t.attrib['k'].index(':')]
                        key = t.attrib['k'][t.attrib['k'].index(':')+1:]
                    else:
                        type = 'regular'
                        key = t.attrib['k']
                    value = value_fixer(t.attrib['v'].encode('utf-8').strip())
                    tags.append({
                        'id':int(element.attrib['id']),
                        'key': key.encode('utf-8').strip(),
                        'value': value,
                        'type': type.encode('utf-8').strip()
                        })
            elif t.tag=='nd':
                way_nodes.append({
                    'id': element.attrib['id'],
                    'node_id': t.attrib['ref'],
                    'position': n
                })
                n += 1
                
        result = {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

# USED CODE FROM THE FINAL CHAPTER OF THE DATA WRANGLING COURSE
# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

# USED CODE FROM THE FINAL CHAPTER OF THE DATA WRANGLING COURSE
# ================================================== #
#               Helper Functions                     #
# ================================================== #
def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        raise Exception(message_string.format(field, error_string))

# USED CODE FROM THE FINAL CHAPTER OF THE DATA WRANGLING COURSE

# ================================================== #
#               Helper Functions                     #
# ================================================== #
class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

# USED CODE FROM THE FINAL CHAPTER OF THE DATA WRANGLING COURSE
# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

process_map(OSM_PATH, validate=False)

# PUSH csv to SQLITE DB

# Creates SQL Table
def create_table(create_table_query,conn):
    c = conn.cursor()
    c.execute(create_table_query)
    conn.commit()

# Reads csv
def read_csv(csv_file):
    with open(csv_file, "rb") as f:
        reader = csv.reader(f)
        header = reader.next()
        data = [row for row in reader]
        return header,data

# adds each row of csv into sql table
def add_into_table(csv_file):
    c = conn.cursor()
    table_name = csv_file[csv_file.index('/')+1:csv_file.index('.csv')]
    header,data = read_csv(csv_file)
    header_string = '(\"'+ '\", \"'.join(str(x) for x in header)+'\")'
    for row in data:
        data_string = '(\"'+ '\", \"'.join(str(x).replace('\"','').replace('\'','') for x in row)+'\")'
        query_string = "INSERT INTO "+table_name+header_string+" VALUES "+data_string
        try:
            c.execute(query_string)
        except:
            # print 'error query: ', query_string
            conn.commit()
            return
    conn.commit()


# CREATING A TABLE



conn = sqlite3.connect('bengaluru_map.db')
# Create tables for each csv file
# Node
create_table_query = '''CREATE TABLE IF NOT EXISTS node(id INTEGER, lat REAL, lon REAL, user STRING, uid INTEGER,
                    version STRING, changeset INTEGER, timestamp STRING, PRIMARY KEY(id ASC))'''
create_table(create_table_query,conn)

# Node_tags
conn = sqlite3.connect('bengaluru_map.db')
create_table_query = '''CREATE TABLE IF NOT EXISTS node_tags(id INTEGER, key STRING, value STRING, type STRING,
                    FOREIGN KEY(id) REFERENCES node(id))'''
create_table(create_table_query,conn)
# way
create_table_query = '''CREATE TABLE IF NOT EXISTS way(id INTEGER, user STRING, uid INTEGER, version STRING,
                    changeset INTEGER, timestamp STRING, PRIMARY KEY(id ASC))'''
create_table(create_table_query,conn)

# way_nodes
create_table_query = '''CREATE TABLE IF NOT EXISTS way_nodes(id INTEGER, node_id INTEGER, position INTEGER, 
                    FOREIGN KEY(id) REFERENCES way(id), FOREIGN KEY(node_id) REFERENCES node(id))'''
create_table(create_table_query,conn)

# way_tags
create_table_query = '''CREATE TABLE IF NOT EXISTS way_tags(id INTEGER, key STRING, value STRING, type STRING,
                    FOREIGN KEY(id) REFERENCES way(id))'''
create_table(create_table_query,conn)

conn.close()


# Build connection
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
# Add data into table:
# Node
add_into_table("bangalore/node.csv")
# Node_tags
add_into_table("bangalore/node_tags.csv")
# way
add_into_table("bangalore/way.csv")
# way_nodes
add_into_table("bangalore/way_nodes.csv")
# way_tags
add_into_table("bangalore/way_tags.csv")
conn.close()


# QUERIES
print "Few information Queried from the db"

print "1. No of node tags written in kannada."
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(*) FROM node_tags WHERE key LIKE '%:kn%'")
for row in result:
    print row
conn.close()


print "2. No of way tags written in kannada."
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(*) FROM way_tags WHERE key LIKE '%:kn%'")
for row in result:
    print row
conn.close()



# USED CODE FROM THE SAMEPLE CODE GIVEN IN https://gist.github.com/carlward/54ec1c91b62a5f911c42#map-area
# Questions - Most popular Amenities in bangalore
print "3. Most popular aminities in bangalore"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT value,count(*) FROM node_tags WHERE key LIKE '%amenity%' GROUP BY value ORDER BY count(*) DESC LIMIT 10")
for row in result:
    print row
conn.close()


# MODIFIED CODE FROM THE SAMEPLE CODE GIVEN IN https://gist.github.com/carlward/54ec1c91b62a5f911c42#map-area
# Questions - Most popular Banks in India

print "4. Most popular bank in Bangalore"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT node_tags.value, COUNT(*) as num FROM node_tags,(SELECT DISTINCT(id) FROM node_tags WHERE value LIKE '%bank%') as banknodes ON node_tags.id=banknodes.id WHERE node_tags.key IN ('name','operator','brand') AND node_tags.value LIKE '%bank%' GROUP BY node_tags.value ORDER BY num DESC LIMIT 10")
for row in result:
    print row
conn.close()


# USED CODE FROM THE SAMEPLE CODE GIVEN IN https://gist.github.com/carlward/54ec1c91b62a5f911c42#map-area
# Questions - Food in bangalore - The more popular cuisines
print "5. Popular cuisines in bangalore"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT value,count(*) as quantity FROM node_tags,(SELECT DISTINCT(id) FROM node_tags WHERE value IN ('restaurant','cafe','fast_food')) as foodnodes ON node_tags.id=foodnodes.id WHERE key IN ('cuisine') GROUP BY value ORDER BY quantity DESC LIMIT 10")
# u'cuisine'
# result=c.execute("SELECT DISTINCT(id) FROM node_tags WHERE value IN ('restaurant','cafe','fast_food')")
for row in result:
    print row
conn.close()


print "Top nodes with highest number of intersection."

conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
node_through_which_highest_way_pass = "(SELECT node_id,count(*) FROM way_nodes GROUP BY node_id ORDER BY count(*) DESC LIMIT 10) as busy_nodes"
info_about_that_node = "SELECT id FROM node,"+node_through_which_highest_way_pass+" ON busy_nodes.node_id=node.id WHERE node.id=busy_nodes.node_id "
result = c.execute(info_about_that_node)
op=[]
for row in result:
    op.append(row[0])
conn.close()
print op

print "To find out more about these nodes, we investigated further. We find no node tags."
tags_of_those_nodes = "SELECT * FROM node_tags,("+info_about_that_node+") as main_nodes WHERE main_nodes.id=node_tags.id"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute(tags_of_those_nodes)
for row in result:
    print row
conn.close()


print "ADDITIONAL STATISTICS"

print "Number of nodes: 2882959"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(*) FROM node")
for row in result:
    print row
conn.close()
print "Number of node_tags: 93243"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(*) FROM node_tags")
for row in result:
    print row
conn.close()
print "Number of ways: 660784"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(*) FROM way")
for row in result:
    print row
conn.close()
print "Number of way_nodes: 3576371"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(*) FROM way_nodes")
for row in result:
    print row
conn.close()
print "Number of way_tags: 723631"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(*) FROM way_tags")
for row in result:
    print row
conn.close()


# USED CODE FROM THE SAMEPLE CODE GIVEN IN https://gist.github.com/carlward/54ec1c91b62a5f911c42#map-area
print "# OF UNIQUE USERS "
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT COUNT(DISTINCT(e.uid)) FROM (SELECT uid FROM node UNION ALL SELECT uid FROM way) e")
for row in result:
    print row[0]
conn.close()


# USED CODE FROM THE SAMEPLE CODE GIVEN IN https://gist.github.com/carlward/54ec1c91b62a5f911c42#map-area
print "Highest contributing user"
conn = sqlite3.connect('bengaluru_map.db')
c = conn.cursor()
result = c.execute("SELECT e.user, COUNT(*) as num FROM (SELECT user FROM node UNION ALL SELECT user FROM way) e GROUP BY e.user ORDER BY num DESC LIMIT 10")
for row in result:
    print row
conn.close()



