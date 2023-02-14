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
        self.assertEqual(ret[0]['id'], 320501)

    def test_get_all_versions_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [297875, 299782, 303016, 306498, 311401, 320501])

    def test_get_specific_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml', 'version': 'v1'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 297875)

    def test_get_all_preprocessings_for_latest_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'latest': 'True'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [320501, 320502, 320503, 320504, 326318, 328183])

    def test_get_specific_preprocessing_bilingual(self):
        params = {'source': 'en', 'target': 'fi', 'corpus': 'OpenSubtitles', 'preprocessing': 'moses',  'latest': 'True'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 326318)

    def test_get_latest_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml', 'latest': 'True'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 325438)

    def test_get_all_versions_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [298604, 300489, 303773, 307401, 314221, 325438])

    def test_get_specific_version_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'xml', 'version': 'v1'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 298604)

    def test_get_all_preprocessings_for_latest_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'latest': 'True'}
        ret = run_default_query(params)
        for i in ret:
            self.assertTrue(i['id'] in [325434, 325435, 325436, 325437, 325438, 325439])

    def test_get_specific_preprocessing_monolingual(self):
        params = {'source': 'en', 'target': '', 'corpus': 'OpenSubtitles', 'preprocessing': 'raw', 'latest': 'True'}
        ret = run_default_query(params)
        self.assertEqual(ret[0]['id'], 325439)

    def test_list_all_corpora(self):
        params = {}
        ret = run_corpora_query(params)
        self.assertEqual(len(ret), 1172)

    def test_list_corpora_one_lan(self):
        params = {'source': 'fi'}
        ret = run_corpora_query(params)
        self.assertEqual(len(ret), 104)

    def test_list_corpora_two_lan(self):
        params = {'source': 'en', 'target': 'fi'}
        ret = run_corpora_query(params)
        self.assertEqual(len(ret), 78)

    def test_list_all_languages(self):
        params = {}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 785)

    def test_list_languages_one_lan(self):
        params = {'source': 'zh'}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 248)

    def test_list_languages_one_corp(self):
        params = {'corpus': 'RF'}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 5)

    def test_list_languages_one_lan_one_corp(self):
        params = {'corpus': 'RF', 'source': 'sv'}
        ret = run_languages_query(params)
        self.assertEqual(len(ret), 4)
