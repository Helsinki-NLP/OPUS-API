from sqlalchemy import create_engine
import unittest

from opusapi import getLanguages, getCorpora, submitCommand, make_sql_command, addRelatedMonoData

opusapi_connection = create_engine('sqlite:////testdata.db')

class TestOpusApi(unittest.TestCase):

    def test_get_specific_corpus(self):
        params = [('source', 'en'), ('target', 'fi'), ('corpus', 'OpenSubtitles'), ('preprocessing', 'xml'), ('version', '#EMPTY#'), ('latest', 'True')]
        command, params = make_sql_command(params, True)
        ret = submitCommand(command, params, True)
        self.assertEqual(len(ret), 4)
        for i in ret:
            self.assertEqual(i['corpus'], 'OpenSubtitles')

    def test_get_all_versions(self):
        params = [('source', 'en'), ('target', 'fi'), ('corpus', 'OpenSubtitles'), ('preprocessing', 'xml'), ('version', '#EMPTY#'), ('latest', '#EMPTY#')]
        command, params = make_sql_command(params, True)
        ret = submitCommand(command, params, True)
        self.assertEqual(len(ret), 20)
        for i in ret:
            self.assertEqual(i['corpus'], 'OpenSubtitles')

    def test_get_specific_version(self):
        params = [('source', 'en'), ('target', 'fi'), ('corpus', 'OpenSubtitles'), ('preprocessing', 'xml'), ('version', 'v1'), ('latest', '#EMPTY#')]
        command, params = make_sql_command(params, True)
        ret = submitCommand(command, params, True)
        self.assertEqual(len(ret), 3)
        for i in ret:
            self.assertEqual(i['corpus'], 'OpenSubtitles')

    def test_get_all_preprocessings_for_latest(self):
        params = [('source', 'en'), ('target', 'fi'), ('corpus', 'OpenSubtitles'), ('preprocessing', '#EMPTY#'), ('version', '#EMPTY#'), ('latest', 'True')]
        command, params = make_sql_command(params, True)
        ret = submitCommand(command, params, True)
        self.assertEqual(len(ret), 19)
        for i in ret:
            self.assertEqual(i['corpus'], 'OpenSubtitles')

    def test_get_specific_preprocessing(self):
        params = [('source', 'en'), ('target', 'fi'), ('corpus', 'OpenSubtitles'), ('preprocessing', 'raw'), ('version', '#EMPTY#'), ('latest', 'True')]
        command, params = make_sql_command(params, True)
        ret = submitCommand(command, params, True)
        self.assertEqual(len(ret), 4)
        for i in ret:
            self.assertEqual(i['corpus'], 'OpenSubtitles')

    def test_get_specific_corpus_one_lan(self):
        parameters = [('source', 'en'), ('target', '#EMPTY#'), ('corpus', 'RF'), ('preprocessing', 'xml'), ('version', '#EMPTY#'), ('latest', 'True')]
        command, params = make_sql_command(parameters.copy(), False)
        ret = submitCommand(command, params, False)
        ret = addRelatedMonoData(ret, parameters)
        self.assertEqual(len(ret), 9)
        for i in ret:
            self.assertEqual(i['corpus'], 'RF')

    def test_get_all_versions_one_lan(self):
        parameters = [('source', 'en'), ('target', '#EMPTY#'), ('corpus', 'SETIMES'), ('preprocessing', 'xml'), ('version', '#EMPTY#'), ('latest', '#EMPTY#')]
        command, params = make_sql_command(parameters.copy(), False)
        ret = submitCommand(command, params, False)
        ret = addRelatedMonoData(ret, parameters)
        self.assertEqual(len(ret), 36)
        for i in ret:
            self.assertEqual(i['corpus'], 'SETIMES')

    def test_get_specific_version_one_lan(self):
        parameters = [('source', 'en'), ('target', '#EMPTY#'), ('corpus', 'SETIMES'), ('preprocessing', 'xml'), ('version', 'v1'), ('latest', '#EMPTY#')]
        command, params = make_sql_command(parameters.copy(), False)
        ret = submitCommand(command, params, False)
        ret = addRelatedMonoData(ret, parameters)
        self.assertEqual(len(ret), 17)
        for i in ret:
            self.assertEqual(i['corpus'], 'SETIMES')

    def test_get_all_preprocessings_for_latest_one_lan(self):
        parameters = [('source', 'en'), ('target', '#EMPTY#'), ('corpus', 'RF'), ('preprocessing', '#EMPTY#'), ('version', '#EMPTY#'), ('latest', 'True')]
        command, params = make_sql_command(parameters.copy(), False)
        ret = submitCommand(command, params, False)
        ret = addRelatedMonoData(ret, parameters)
        self.assertEqual(len(ret), 103)
        for i in ret:
            self.assertEqual(i['corpus'], 'RF')

    def test_get_specific_preprocessing_one_lan(self):
        parameters = [('source', 'en'), ('target', '#EMPTY#'), ('corpus', 'RF'), ('preprocessing', 'raw'), ('version', '#EMPTY#'), ('latest', 'True')]
        command, params = make_sql_command(parameters.copy(), False)
        ret = submitCommand(command, params, False)
        ret = addRelatedMonoData(ret, parameters)
        self.assertEqual(len(ret), 9)
        for i in ret:
            self.assertEqual(i['corpus'], 'RF')

    def test_list_all_corpora(self):
        corpora = getCorpora('#EMPTY#', '#EMPTY#')
        self.assertEqual(len(corpora), 45)

    def test_list_corpora_one_lan(self):
        corpora = getCorpora('fi', '#EMPTY#')
        self.assertEqual(len(corpora), 17)

    def test_list_corpora_two_lan(self):
        corpora = getCorpora('en', 'fi')
        self.assertEqual(len(corpora), 16)

    def test_list_all_languages(self):
        languages = getLanguages('#EMPTY#', '#EMPTY#')
        self.assertEqual(len(languages), 339)

    def test_list_languages_one_lan(self):
        languages = getLanguages('zh', '#EMPTY#')
        self.assertEqual(len(languages), 92)

    def test_list_languages_one_corp(self):
        languages = getLanguages('#EMPTY#', 'RF')
        self.assertEqual(len(languages), 5)

    def test_list_languages_one_lan_one_corp(self):
        languages = getLanguages('sv', 'RF')
        self.assertEqual(len(languages), 4)
