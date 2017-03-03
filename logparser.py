#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import re
from influxdb import InfluxDBClient
from collections import defaultdict

SEARCH_WORD = "REQUEST"
JSON_BODY = '[{"measurement": "errors",' \
            '"tags": {"test_type": "capacity",' \
            '"simulation": "%(simulation)s",' \
            '"user_count": 20,' \
            '"request_name": "%(request_name)s",' \
            '"request_params": "%(request_params)s",' \
            '"error_status": "%(response_code)s",' \
            '"error_details": "%(error)s"},' \
            '"time": %(request_start)s,' \
            '"fields": {"response_time": %(response_time)s}}]'


class SimulationLogParser(object):
    def write_to_db(self, json_body):

        client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
        client.create_database('test')
        client.write_points(json_body)

        # result = client.query('select value from cpu_load_short;')
        # print("Result: {0}".format(result))

    def parse_log(self, path):
        with open(path) as infile:
            for line in infile:
                if "\tKO\t" in line:  # iterate through all without check
                    data = self.parse(line)
                    # print data
                    print JSON_BODY % data
                    self.write_to_db(json.loads(JSON_BODY % data))

    def parse(self, line):
        values = re.split('\\t+', line)
        regexp = re.match(r".+ (https?://.+?),* .*HTTP Code: (.+?), Response: ?(.*),*", values[8])
        arguments = defaultdict()
        arguments['simulation'] = values[1]
        arguments['requests'] = values[2]
        arguments['request_name'] = values[3]
        arguments['request_start'] = values[4]
        arguments['request_end'] = values[5]
        arguments['response_time'] = int(int(arguments['request_end']) - int(arguments['request_start']))
        arguments['gatling_error'] = values[7]

        arguments['request_params'] = self.extract_params(regexp)
        arguments['response_code'] = regexp.group(2)

        code_matcher = re.search(r"[a-zA-Z]*(\d+)", arguments['response_code'])

        if code_matcher:
            arguments['response_code'] = code_matcher.group(1)

        arguments['error'] = regexp.group(3).replace('"', '\\"')

        return arguments

    def extract_params(self, regexp):
        params_list = regexp.group(1).split("?")
        if len(params_list) <= 1:
            params = ""
        else:
            params = params_list[len(params_list) - 1]
        return params


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simlog parser.')
    parser.add_argument("-f", "--file", required=True, help="Log to parse.")
    parser.add_argument("-c", "--count", required=True, type=int, help="User count.")
    parser.add_argument("-t", "--type", required=True, help="Test type.")

    args = vars(parser.parse_args())

    logPath = args['file']
    userCount = args['count']
    testType = args['type']

    logParser = SimulationLogParser()
    logParser.parse_log(logPath)
