from unittest import mock, skipUnless

from django.db import connection
from django.db.backends.mysql.features import DatabaseFeatures
from django.test import TestCase


@skipUnless(connection.vendor == 'mysql', 'MySQL tests')
class TestFeatures(TestCase):

    def test_supports_transactions(self):
        """
        All storage engines except MyISAM support transactions.
        """
        with mock.patch('django.db.connection.features._mysql_storage_engine', 'InnoDB'):
            self.assertTrue(connection.features.supports_transactions)
        del connection.features.supports_transactions
        with mock.patch('django.db.connection.features._mysql_storage_engine', 'MyISAM'):
            self.assertFalse(connection.features.supports_transactions)
        del connection.features.supports_transactions

    def test_select_for_share_or_update_skip_locked_nowait_of(self):
        with mock.MagicMock() as _connection:
            _connection.mysql_version = (8, 0, 1)
            _connection.mysql_is_mariadb = False
            features = DatabaseFeatures(_connection)
            self.assertIs(features.has_select_for_share_skip_locked, True)
            self.assertIs(features.has_select_for_share_nowait, True)
            self.assertIs(features.has_select_for_share_of, True)
            self.assertIs(features.has_select_for_update_skip_locked, True)
            self.assertIs(features.has_select_for_update_nowait, True)
            self.assertIs(features.has_select_for_update_of, True)
        with mock.MagicMock() as _connection:
            _connection.mysql_version = (8, 0, 0)
            _connection.mysql_is_mariadb = False
            features = DatabaseFeatures(_connection)
            self.assertIs(features.has_select_for_share_skip_locked, False)
            self.assertIs(features.has_select_for_share_nowait, False)
            self.assertIs(features.has_select_for_share_of, False)
            self.assertIs(features.has_select_for_update_skip_locked, False)
            self.assertIs(features.has_select_for_update_nowait, False)
            self.assertIs(features.has_select_for_update_of, False)

    def test_allows_auto_pk_0(self):
        with mock.MagicMock() as _connection:
            _connection.sql_mode = {'NO_AUTO_VALUE_ON_ZERO'}
            database_features = DatabaseFeatures(_connection)
            self.assertIs(database_features.allows_auto_pk_0, True)
