#!/usr/bin/python3.5
import json

from bottle import Bottle, request, redirect, response
import mysql.connector
import uuid
import random
import os.path

# CAPTCHA-SITE-KEY: 6LekKboUAAAAAKQHsZxhrCByUZzvWkDTw0Xdc_Ve
# CAPTCHA-SECRET-KEY: 6LekKboUAAAAADS95bHyjQMgP7TBSaRy-gcW6I-G


class CyberGuyQuizFrontEnd:

    def __init__(self, listen_ip, listen_port):
        self.__root_path = os.path.dirname(__file__)
        self.__bootstrap_path = os.path.join(self.__root_path, 'bootstrap-3.4.1-dist')
        self.__jquery_path = os.path.join(self.__root_path, 'jquery')
        self.__jquery_script_filename = 'jquery-3.4.1.min.js'
        self.__index_html_filename = os.path.join(self.__root_path, 'index.html')
        self.__styles_css_filename = os.path.join(self.__root_path, 'styles.css')
        self.__db_conn = mysql.connector.connect(user='cyber_guy_quiz', password='Vofaba80tusi!', host='127.0.0.1', database='CyberGuyQuizFrontEnd_DB')
        self.__db_conn.autocommit = True
        self.__host = listen_ip
        self.__port = listen_port
        self.__app = Bottle()
        self.__route()

    def __route(self):
        self.__app.add_hook(name='before_request', func=self.__set_cors_header)
        self.__app.add_hook(name='before_request', func=self.__set_server_header)
        self.__app.error_handler[404] = self.__error_handler
        self.__app.error_handler[405] = self.__error_handler
        self.__app.error_handler[500] = self.__error_handler
        # self.__app.route('/', method="GET", callback=self.__root_handler)
        # self.__app.route('/bootstrap/<bootstrap_res_type>/<bootstrap_filename>', method="GET", callback=self.__get_bootstrap_files)
        # self.__app.route('/jquery', method="GET", callback=self.__get_jquery_script)
        # self.__app.route('/index.htm', method="GET", callback=self.__get_page)
        # self.__app.route('/styles.css', method="GET", callback=self.__stylesheet)
        self.__app.route('/report-query/<query_id>', method="POST", callback=self.__report_query)
        self.__app.route('/test-db-conn', method="GET", callback=self.__test_db_conn)
        self.__app.route('/add-category', method="POST", callback=self.__insert_new_category)
        self.__app.route('/get-all-query-ids', method="GET", callback=self.__get_all_query_ids)
        self.__app.route('/get-all-categories', method=["GET", "OPTIONS"], callback=self.__get_all_categories)
        self.__app.route('/get-random-question', method=["POST", "OPTIONS"], callback=self.__get_random_query)
        self.__app.route('/get-grade', method="POST", callback=self.__get_grade)
        # self.__app.route('/validate-token/<captcha_token>', method="GET", callback=self.__validate_captcha)


    def run(self):
        self.__app.run(host=self.__host, port=self.__port)

    def __set_server_header(self):
        response.set_header('Server', 'Microsoft-IIS/8.5')

    def __set_cors_header(self):
        response.set_header('Access-Control-Allow-Origin', 'http://localhost:4200')
        response.set_header('Access-Control-Allow-Methods', 'OPTIONS, POST, GET')
        response.set_header('Access-Control-Allow-Headers', 'Access-Control-Request-Headers,Access-Control-Request-Method,content-type')

    def __error_handler(self, error):
        response.set_header('Server', 'Microsoft-IIS/8.5')
        return json.dumps({
            "code": error.status_code,
            "msg": error.status_line
        })

    # def __root_handler(self):
    #     response.set_header('Server', 'Microsoft-IIS/8.5')
    #     redirect('/index.htm')
    #
    # def __get_bootstrap_files(self, bootstrap_res_type, bootstrap_filename):
    #     response.set_header('Server', 'Microsoft-IIS/8.5')
    #     if bootstrap_res_type == 'css':
    #         response.content_type = 'text/css; charset=UTF-8'
    #     requested_bootstrap_folder = os.path.join(os.path.join(self.__bootstrap_path, bootstrap_res_type))
    #     requested_bootstrap_resource = os.path.join(requested_bootstrap_folder, bootstrap_filename)
    #     if os.path.abspath(requested_bootstrap_resource).startswith(self.__bootstrap_path) and os.path.exists(requested_bootstrap_resource):
    #         with open(requested_bootstrap_resource) as bootstrap_file:
    #             bootstrap_file_contents = bootstrap_file.read()
    #         return bootstrap_file_contents
    #     else:
    #         return ''
    #
    # def __get_jquery_script(self):
    #     response.set_header('Server', 'Microsoft-IIS/8.5')
    #     with open(os.path.join(self.__jquery_path, self.__jquery_script_filename)) as jquery_script_file:
    #         jquery_script_file_contents = jquery_script_file.read()
    #     return jquery_script_file_contents
    #
    # def __get_page(self):
    #     response.set_header('Server', 'Microsoft-IIS/8.5')
    #     with open(self.__index_html_filename) as html_file:
    #         html_contents = html_file.read()
    #     return html_contents
    #
    # def __stylesheet(self):
    #     response.content_type = 'text/css; charset=UTF-8'
    #     response.set_header('Server', 'Microsoft-IIS/8.5')
    #     with open(self.__styles_css_filename) as css_file:
    #         css_contents = css_file.read()
    #     return css_contents
    #
    # def __validate_captcha(self, captcha_token):
    #     response.content_type = 'text/css; charset=UTF-8'
    #     response.set_header('Server', 'Microsoft-IIS/8.5')
    #
    #     token_valid = False
    #     if token_valid:
    #         session_token = uuid.uuid4()
    #         response.set_cookie('session_token', session_token)

    def __get_grade(self):
        response.content_type = 'text/css; charset=UTF-8'
        response.set_header('Server', 'Microsoft-IIS/8.5')
        return json.dumps({"grade": 85})

    '''
    {
        "category_name": "blah blah blah"
    }
    '''
    def __insert_new_category(self):
        category_id = str(uuid.uuid4())
        cursor = self.__db_conn.cursor()

        # Insert category if needed
        query_template = 'INSERT IGNORE INTO CyberGuyQuizFrontEnd_DB.categories (category_id, category_name) VALUES (%s, %s);'
        values = [ category_id, request.json["category_name"] ]
        cursor.execute(query_template, values)

        return category_id

    '''
    {
        "categories" [
            "77eb937c-c356-11e9-b01d-3bb424fb81da",
            "77eb937c-c356-11e9-b01d-3bb424fb81db",
            "77eb937c-c356-11e9-b01d-3bb424fb81dc"
        ]
    }
    '''
    def __report_query(self, query_id):
        cursor = None
        try:
            categorization_ids = []
            cursor = self.__db_conn.cursor()
            # Insert category if needed
            insert_query_template = 'INSERT INTO CyberGuyQuizFrontEnd_DB.categorized_metrics_anomalies (' \
                                'categorization_id, ' \
                                'category, ' \
                                'question, ' \
                                'times_categorized' \
                             ') VALUES (' \
                                '%s, ' \
                                '%s, ' \
                                '%s, ' \
                                '1' \
                             ') ON DUPLICATE KEY UPDATE times_categorized=times_categorized+1;'
            select_query_template = 'SELECT * FROM CyberGuyQuizFrontEnd_DB.categorized_metrics_anomalies WHERE categorization_id=%s;'
            for category_id in request.json["categories"]:
                categorization_id = str(uuid.uuid4())
                values = [categorization_id, category_id, query_id]
                cursor.execute(insert_query_template, values)
                cursor.execute(select_query_template, [categorization_id])
                lines_modified = cursor.fetchall()
                if len(lines_modified) > 0:
                    # TODO: If the times_categorized is above 1 the current client gets a point (later the grade will be determined by the percentage of these points out of the questions he answered.
                    categorization_ids.append(categorization_id)

            response.set_header('Content-Type', 'application/json')
            return json.dumps({
                "categorization_ids": categorization_ids
            })
        except Exception as ex:
            print(ex)
            raise
        finally:
            try:
                cursor.close()
            except:
                pass

    def __get_all_categories(self):
        cursor = self.__db_conn.cursor()
        query_template = 'SELECT category_id, category_name FROM CyberGuyQuizFrontEnd_DB.categories;'
        cursor.execute(query_template)

        qry_res = cursor.fetchall()
        categories = []
        for raw_res in qry_res:
            categories.append({
                "category_id": raw_res[0],
                "category_name": raw_res[1]
            })

        response.set_header('Content-Type', 'application/json')
        return json.dumps({
            "categories": categories
        })

    def __get_all_query_ids(self):
        cursor = self.__db_conn.cursor()
        query_template = 'SELECT question_id FROM CyberGuyQuizFrontEnd_DB.quiz_questions;'
        cursor.execute(query_template)

        qry_res = cursor.fetchall()
        query_ids = []
        for raw_res in qry_res:
            query_ids.append(raw_res[0])

        response.set_header('Content-Type', 'application/json')
        return json.dumps({
            "query_ids": query_ids
        })

    def __get_query(self, query_id):
        cursor = self.__db_conn.cursor()
        query_template = 'SELECT quiz_questions.question_id, quiz_questions.metric, metrics.metric_name, quiz_questions.anomaly_type, anomaly_types.anomaly_type, quiz_questions.description FROM quiz_questions JOIN metrics ON (quiz_questions.metric = metrics.metric_id) JOIN anomaly_types ON (quiz_questions.anomaly_type = anomaly_types.anomaly_type_id) WHERE question_id=%s;'
        cursor.execute(query_template, [query_id])
        qry_res = cursor.fetchall()

        response.set_header('Content-Type', 'application/json')
        return json.dumps({
            "question_id": qry_res[0][0],
            "metric_id": qry_res[0][1],
            "metric_name": qry_res[0][2],
            "anomaly_type_id": qry_res[0][3],
            "anomaly_type": qry_res[0][4],
            "description": qry_res[0][5]
        })

    '''
    {
        "answered_questions_ids: [] 
    }
    '''
    def __get_random_query(self):
        all_ids_raw = self.__get_all_query_ids()
        all_ids = json.loads(all_ids_raw)["query_ids"]
        rand_idx = random.randint(0, len(all_ids) - 1)
        selected_id = all_ids[rand_idx]
        i = 0
        while request.json is not None and "answered_questions_ids" in request.json and selected_id in request.json["answered_questions_ids"]:
            if i == len(all_ids):
                return json.dumps([])
            selected_id = all_ids[random.randint(0, len(all_ids) - 1)]
            i = i + 1

        response.set_header('Content-Type', 'application/json')
        return self.__get_query(selected_id)

    def __test_db_conn(self):
        cursor = self.__db_conn.cursor()
        cursor.execute('select * from quiz_questions limit 1')
        row_count = 0
        for row in cursor:
            row_count = row_count + 1
        return str(row_count)


CyberGuyQuizFrontEnd('0.0.0.0', 8080).run()