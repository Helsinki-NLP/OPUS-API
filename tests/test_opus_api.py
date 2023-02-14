import sqlite3
import unittest

import opusapi
from opusapi import clean_up_parameters, run_query, run_default_query, run_corpora_query, run_languages_query

DB_FILE = 'tests/testdata.db'
opusapi.DB_FILE = DB_FILE

class TestOpusApi(unittest.TestCase):

    def test_get_latest_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml', 'latest': 'True'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 136272)

    def test_get_all_versions_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [112278, 112934, 114221, 115588, 123919, 123920, 136272, 136273])

    def test_get_specific_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml', 'version': 'v1'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 112278)

    def test_get_all_preprocessings_for_latest_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'latest': 'True'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [126145, 128126, 130626, 130627, 133658, 136272, 136273])

    def test_get_specific_preprocessing_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'preprocessing': 'moses',  'latest': 'True'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 128126)

    def test_get_latest_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml', 'latest': 'True'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 136362)

    def test_get_all_versions_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [112299, 112972, 114258, 115629, 123968, 136362])

    def test_get_specific_version_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml', 'version': 'v1'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 112299)

    def test_get_all_preprocessings_for_latest_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'latest': 'True'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [127362, 127435, 127436, 129380, 129423, 136362])

    def test_get_specific_preprocessing_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'raw', 'latest': 'True'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 129423)

    def test_list_all_corpora(self):
        params = {}
        ret = run_corpora_query(params)
        self.assertEqual(len(ret), 45)

    def test_list_corpora_one_lan(self):
        params = {'source': 'fi'}
        ret = run_corpora_query(params)
        self.assertEqual(len(ret), 17)

    def test_list_corpora_two_lan(self):
        params = {'source': 'en', 'target': 'fi'}
        ret = run_corpora_query(params)
        self.assertEqual(len(ret), 16)

    def test_list_all_languages(self):
        params = {}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 339)

    def test_list_languages_one_lan(self):
        params = {'source': 'zh'}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 93)

    def test_list_languages_one_corp(self):
        params = {'corpus': 'RF'}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 5)

    def test_list_languages_one_lan_one_corp(self):
        params = {'corpus': 'RF', 'source': 'sv'}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 4)
