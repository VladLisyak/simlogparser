#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import re
from difflib import SequenceMatcher
from json import loads

from influxdb import InfluxDBClient
from collections import defaultdict

SEARCH_WORD = "REQUEST"
JSON_BODY = '[{"measurement": "errors",' \
            '"tags": {"test_type": "capacity",' \
            '"simulation": "%(simulation)s",' \
            '"user_count": 20,' \
            '"request_name": "%(request_name)s",' \
            '"error_status": "%(response_code)s",' \
            '"error_code": "%(error_code)s",' \
            '"error_details": "%(error)s"},' \
            '"time": %(request_start)s,' \
            '"fields": {"response_time": %(response_time)s}}]'
SEEN_ERRORS = []


class SimulationLogParser(object):
    def write_to_db(self, json_body):
        client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
        client.create_database('test')
        client.write_points(json_body)

    def parse_log(self, path):
        with open(path) as tsv:
            for line in csv.reader(tsv, delimiter="\t"):
                if len(line) >= 8 and (line[7] == "KO"):
                    data = self.parse(line)
                    self.write_to_db(loads(JSON_BODY % data))

    def parse(self, values):
        regexp = re.match(r".+ (https?://.+?),* .*HTTP Code: (.+?), Response: ?(.*),*", values[9])
        error_code_regex = re.search(r'"code":"?(-?\d+)"?,', values[9])

        arguments = defaultdict()
        arguments['simulation'] = values[1]
        arguments['requests'] = values[2]
        arguments['request_name'] = values[4]
        arguments['request_start'] = values[5] + "000000"
        arguments['request_end'] = values[6]
        arguments['response_time'] = int(values[6]) - int(values[5])
        arguments['gatling_error'] = values[8]
        arguments['response_code'] = self.extract_response_code(regexp.group(2))
        arguments['error_code'] = self.extract_error_code(error_code_regex)
        arguments['error'] = values[9].replace('"', '\\"') #self.compare_error(current_err)
        return arguments

    def extract_error_code(self, error_code_regex):
        code = "undefined"
        if error_code_regex:
            code = str(error_code_regex.group(1))
        return code

    def extract_response_code(self, code):
        code_matcher = re.search(r"[a-zA-Z]*(\d+)", code)
        if code_matcher:
            return code_matcher.group(1)
        return code

    # def compare_error(self, current_err):
    #     if len(SEEN_ERRORS) == 0:
    #         SEEN_ERRORS.append(current_err)
    #         return current_err
    #     else:
    #         for seen_err in SEEN_ERRORS:
    #             if SequenceMatcher(None, current_err, seen_err).ratio() >= 0.7:
    #                 return seen_err
    #
    #         SEEN_ERRORS.append(current_err)
    #         return current_err


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simlog parser.')
    parser.add_argument("-f", "--file", required=True, help="Log to parse.")
    # parser.add_argument("-c", "--count", required=True, type=int, help="User count.")
    # parser.add_argument("-t", "--type", required=True, help="Test type.")

    args = vars(parser.parse_args())

    logPath = args['file']
    # userCount = args['count']
    # testType = args['type']

    logParser = SimulationLogParser()
    logParser.parse_log(logPath)
