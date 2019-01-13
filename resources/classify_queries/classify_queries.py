import sys
import re
import os.path
import json


if len(sys.argv) < 3:
    print("ERROR: Source Zabbix template file or queries.json not specified. QUITTING!")
    print("Usage: " + os.path.basename(__file__) + " <zabbix_template_file> <queries_json_file>\n\n")
    exit(1)

xml_zabbix_template = ""
queries_obj = None
try:
    xml_zabbix_template_filename = sys.argv[1]
    with open(xml_zabbix_template_filename) as xml_zabbix_template_file:
        xml_zabbix_template = xml_zabbix_template_file.read()
except:
    print("ERROR: Failed to open or read the xml Zabbix template file specified. LEAVING.")
    exit(2)

try:
    queries_obj_filename = sys.argv[2]
    with open(queries_obj_filename) as queries_obj_file:
        queries_obj = json.loads(queries_obj_file.read())
except:
    print("ERROR: Failed to open or read the queries.json file specified and load it as valid JSON object. LEAVING.")
    exit(3)

in_item_conf = False
in_item_applications_conf = False
items = {}
applications = []
cur_query_name = ""
for line in xml_zabbix_template.split('\n'):
    if line.strip().startswith('<item_prototype>'):
        in_item_conf = True
    elif line.strip().startswith('</item_prototype>'):
        in_item_conf = False
        items[cur_query_name] = applications

    if in_item_conf and line.strip().startswith('<key>'):
        query_name_raw = re.findall(r'<key>web\.page\.regexp\[\{\$METRIC_COLLECTOR_HOST\},smart\-onion\/([a-z\/\-_]+)\?.*', line.strip())
        if len(query_name_raw) > 0:
            query_name = query_name_raw[0]
            cur_query_name = query_name

    if in_item_conf and line.strip().startswith('<applications>'):
        in_item_applications_conf = True
        applications = []

    if in_item_conf and line.strip().startswith('</applications>'):
        in_item_applications_conf = False

    if in_item_applications_conf and line.strip().startswith('<name>'):
        application_raw = re.findall(r'<name>([a-z]+:[a-z_]+)<\/name>', line.strip())
        if len(application_raw) > 0:
            applications.append(application_raw[0])

for query in queries_obj:
    query_to_find = ""
    if queries_obj[query]["type"] == "QUERY_COUNT":
        query_to_find = "query-count/" + query

    if queries_obj[query]["type"] == "FIELD_QUERY":
        query_to_find = "field-query/" + query

    if queries_obj[query]["type"] == "SIMILARITY_TEST":
        query_to_find = "test-similarity/" + query

    if queries_obj[query]["type"] == "LIST_HASH":
        query_to_find = "list-hash/" + query

    if len(query_to_find) > 1 and query_to_find in items:
        queries_obj[query]["tags"] = ','.join(items[query_to_find])


res = ""
for query_type in ["LLD", "QUERY_COUNT", "FIELD_QUERY", "SIMILARITY_TEST", "LIST_HASH"]:
    res = res + '"___________________________________________' + query_type + '___________________________________________": {"type": "COMMENT"}, '
    for query in queries_obj:
        if queries_obj[query]["type"] == query_type:
            res = res + '"' + query + '": ' + json.dumps(queries_obj[query]) + ","
res = "{" + res.strip(",") + "}"
# print(json.dumps(queries_obj, indent=2))
# print(json.dumps(obj=json.loads(res), indent=2))
print(res)