#  Copyright 2013-2014 Nokia Solutions and Networks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from __future__ import with_statement
from datetime import datetime
from hashlib import sha1
from robot.api import ExecutionResult
from sqlalchemy.exc import IntegrityError

from dbbot import Logger

import pytz


class RobotResultsParser(object):

    def __init__(self, include_keywords, db, verbose_stream):
        self._verbose = Logger('Parser', verbose_stream)
        self._include_keywords = include_keywords
        self._db = db

    def xml_to_db(self, xml_file):
        self._verbose('- Parsing %s' % xml_file)
        test_run = ExecutionResult(xml_file, include_keywords=self._include_keywords)
        hash_string = self._hash(xml_file)
        try:
            test_run_id = self._db.insert('test_runs', {
                'hash': hash_string,
                'imported_at': datetime.now(pytz.timezone('US/Pacific')),
                'source_file': test_run.source,
                'started_at': self._format_robot_timestamp(test_run.suite.starttime),
                'finished_at': self._format_robot_timestamp(test_run.suite.endtime),
                'passed': test_run.statistics.total.passed,
                'failed': test_run.statistics.total.failed,
                'skipped': test_run.statistics.total.skipped
            })
        except IntegrityError:
            test_run_id = self._db.fetch_id('test_runs', {
                'source_file': test_run.source,
                'started_at': self._format_robot_timestamp(test_run.suite.starttime),
                'finished_at': self._format_robot_timestamp(test_run.suite.endtime)
            })
        self._parse_suite(test_run.suite, test_run_id)

    @staticmethod
    def _hash(xml_file):
        block_size = 68157440
        hasher = sha1()
        with open(xml_file, 'rb') as f:
            chunk = f.read(block_size)
            while len(chunk) > 0:
                hasher.update(chunk)
                chunk = f.read(block_size)
        return hasher.hexdigest()

    def _parse_suite(self, suite, test_run_id, parent_suite_id=None):
        self._verbose('`--> Parsing suite: %s' % suite.name)
        try:
            suite_id = self._db.insert('suites', {
                'suite_id': parent_suite_id,
                'xml_id': suite.id,
                'name': suite.name,
                'source': suite.source,
                'doc': suite.doc
            })
        except IntegrityError:
            suite_id = self._db.fetch_id('suites', {
                'name': suite.name,
                'source': suite.source
            })
        self._parse_suites(suite, test_run_id, suite_id)
        self._parse_tests(suite.tests, test_run_id, suite_id)

    def _parse_suites(self, suite, test_run_id, parent_suite_id):
        [self._parse_suite(subsuite, test_run_id, parent_suite_id) for subsuite in suite.suites]

    def _parse_tests(self, tests, test_run_id, suite_id):
        [self._parse_test(test, test_run_id, suite_id) for test in tests]

    def _parse_test(self, test, test_run_id, suite_id):
        self._verbose('  `--> Parsing test: %s' % test.name)
        try:
            test_id = self._db.insert('tests', {
                'suite_id': suite_id,
                'xml_id': test.id,
                'name': test.name,
                'timeout': test.timeout,
                'doc': test.doc
            })
        except IntegrityError:
            test_id = self._db.fetch_id('tests', {
                'suite_id': suite_id,
                'name': test.name
            })
        self._parse_test_status(test_run_id, test_id, test)

    def _parse_test_status(self, test_run_id, test_id, test):
        self._db.insert_or_ignore('test_status', {
            'test_run_id': test_run_id,
            'test_id': test_id,
            'status': test.status,
            'elapsed': test.elapsedtime
        })

    @staticmethod
    def _format_robot_timestamp(timestamp):
        return datetime.strptime(timestamp, '%Y%m%d %H:%M:%S.%f') if timestamp else None

    @staticmethod
    def _string_hash(string):
        return sha1(string.encode()).hexdigest() if string else None
