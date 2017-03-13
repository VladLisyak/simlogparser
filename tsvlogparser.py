#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import os
from json import loads

from influxdb import InfluxDBClient
from collections import defaultdict

RESULTS_FOLDER = '/opt/gatling/results/'
SIMLOG_NAME = 'simulation.log'
SEARCH_WORD = "REQUEST"
JSON_BODY = '[{"measurement": "errors",' \
            '"tags": {"test_type": "%(test_type)s",' \
            '"simulation": "%(simulation)s",' \
            '"user_count": %(user_count)d},' \
            '"time": %(request_start)s,' \
            '"fields": {' \
            '"request_name": "%(request_name)s",' \
            '"error_status": "%(response_code)s",' \
            '"error_code": "%(error_code)s",' \
            '"error_details": "%(error)s",' \
            '"error_class": "%(error_class)s",' \
            '"response_time": %(response_time)s}}]'

DB_URL = '10.23.11.220'
DB_PORT = 8086
DB_LOGIN = ''
DB_PASSWORD = ''
DB_NAME = 'perftest'


class ErrorClassifier:
    serialization_error = "failed to parse"
    connection_error = "not-connected"

    undefined = "undefined"
    timeout_exceeded = "req_timeout"
    not_serializable = "%s_not_serializable"
    error_status = "%s_%s"
    error_undefined = "%s_undefined"
    not_connected = "conn_timeout"

    def __init__(self):
        pass

    @staticmethod
    def classify_entry(arguments):
        if "TimeoutException" in arguments['gatling_error']:
            return ErrorClassifier.classify_time_error(arguments)
        if arguments['response_code'] is not "undefined":
            return ErrorClassifier.classify_code_error(arguments)

        return ErrorClassifier.undefined

    @staticmethod
    def classify_time_error(arguments):
        if ErrorClassifier.connection_error in arguments['gatling_error']:
            return ErrorClassifier.not_connected
        return ErrorClassifier.timeout_exceeded

    @staticmethod
    def classify_code_error(arguments):
        if ErrorClassifier.serialization_error in arguments['gatling_error']:
            return ErrorClassifier.not_serializable % arguments['response_code']
        if ErrorClassifier.undefined == arguments['error_code']:
            return ErrorClassifier.error_undefined % arguments['response_code']
        return ErrorClassifier.error_status % (arguments['response_code'], arguments['error_code'])


class SimulationLogParser(object):
    def __init__(self, test_type, user_count, simulation):
        self.user_count = user_count
        self.test_type = test_type
        self.simulation = simulation

    def parse_log(self):
        """Parse line with error and send to database"""
        client = InfluxDBClient(DB_URL, DB_PORT, DB_LOGIN, DB_PASSWORD, DB_NAME)
        path = self.find_log()
        with open(path) as tsv:
            for line in csv.reader(tsv, delimiter="\t"):
                if len(line) >= 10 and (line[7] == "KO"):
                    data = self.parseEntry(line)
                    error_json = loads(JSON_BODY % data)
                    client.write_points(error_json)

    def parseEntry(self, values):
        """Parse error entry"""
        regexp = re.search(r".+ (https?://.+?),* .*HTTP Code: ?(.+?), Response: ?(.*),*", values[9])

        arguments = defaultdict()
        arguments['test_type'] = self.test_type
        arguments['user_count'] = self.user_count
        arguments['simulation'] = self.simulation
        arguments['requests'] = values[2]
        arguments['request_name'] = values[4]
        arguments['request_start'] = values[5] + "000000"
        arguments['request_end'] = values[6]
        arguments['response_time'] = int(values[6]) - int(values[5])
        arguments['gatling_error'] = values[8]
        arguments['response_code'] = self.extract_response_code(regexp)
        arguments['error_code'] = self.extract_error_code(values[9])
        arguments['error'] = values[9].replace('"', '\\"')

        arguments['error_class'] = ErrorClassifier.classify_entry(arguments)
        print arguments['error_class']
        return arguments

    def extract_error_code(self, error_code):
        error_code_regex = re.search(r'"code": ?"?(-?\d+)"?,', error_code)
        code = "undefined"
        if error_code_regex:
            code = str(error_code_regex.group(1))
        return code

    def extract_response_code(self, regex):
        code = "undefined"
        if regex.group(2):
            code_matcher = re.search(r"[a-zA-Z]*(\d+)", regex.group(2))
            if code_matcher:
                return str(code_matcher.group(1))
        return code

    @staticmethod
    def find_log():
        for d, dirs, files in os.walk(RESULTS_FOLDER):
            for f in files:
                if f == SIMLOG_NAME:
                    simlog_folder = os.path.basename(d)
                    return os.path.join(RESULTS_FOLDER, simlog_folder, SIMLOG_NAME)
        print "error no simlog"


if __name__ == '__main__':
    print "parsing simlog"
    parser = argparse.ArgumentParser(description='Simlog parser.')
    # parser.add_argument("-f", "--file", required=True, help="Log to parse.")
    parser.add_argument("-c", "--count", required=True, type=int, help="User count.")
    parser.add_argument("-t", "--type", required=True, help="Test type.")
    parser.add_argument("-s", "--simulation", required=True, help="Simulation")
    # parser.add_argument("-s", "--host", required=True)
    # parser.add_argument("-p", "--port")
    # parser.add_argument("-l", "--login")
    # parser.add_argument("-w", "--password")
    # parser.add_argument("-b", "--database")

    args = vars(parser.parse_args())

    # logPath = args['file']
    userCount = args['count']
    testType = args['type']
    simulation = args['simulation']
    # host = args['host']
    # port = args['port']
    # login = args['login']
    # password = args['password']
    # db  = args['database']

    logParser = SimulationLogParser(testType, userCount, simulation)
    logParser.parse_log()
