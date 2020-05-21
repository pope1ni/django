import contextlib
import threading
import time
from unittest import mock

from multiple_database.routers import TestRouter

from django.core.exceptions import FieldError
from django.db import (
    DatabaseError, NotSupportedError, connection, connections, router,
    transaction,
)
from django.test import (
    TransactionTestCase, override_settings, skipIfDBFeature,
    skipUnlessDBFeature,
)
from django.test.utils import CaptureQueriesContext

from .models import (
    City, CityCountryProxy, Country, EUCity, EUCountry, Person, PersonProfile,
)


class SelectForShareOrUpdateTests(TransactionTestCase):

    available_apps = ['select_for_share_or_update']

    def setUp(self):
        # This is executed in autocommit mode so that code in
        # run_select_for_share_or_update can see this data.
        self.country1 = Country.objects.create(name='Belgium')
        self.country2 = Country.objects.create(name='France')
        self.city1 = City.objects.create(name='Liberchies', country=self.country1)
        self.city2 = City.objects.create(name='Samois-sur-Seine', country=self.country2)
        self.person = Person.objects.create(name='Reinhardt', born=self.city1, died=self.city2)
        self.person_profile = PersonProfile.objects.create(person=self.person)

    def build_raw_sql(self, operation, connection, model, **kwargs):
        if operation == 'select_for_share':
            function = connection.ops.for_share_sql
        elif operation == 'select_for_update':
            function = connection.ops.for_update_sql
        else:
            raise ValueError('Unknown operation.')
        return 'SELECT * FROM %s %s;' % (model._meta.db_table, function(**kwargs))

    @contextlib.contextmanager
    def blocking_transaction(self, operation):
        # We need another database connection in transaction to test that one
        # connection issuing a SELECT ... FOR SHARE/UPDATE will block.
        new_connection = connection.copy()
        try:
            new_connection.set_autocommit(False)
            # Start a blocking transaction. At some point,
            # end_blocking_transaction() should be called.
            self.cursor = new_connection.cursor()
            sql = self.build_raw_sql(operation, new_connection, Person)
            self.cursor.execute(sql, ())
            self.cursor.fetchone()

            yield

        finally:
            # Roll back the blocking transaction.
            self.cursor.close()
            new_connection.rollback()
            new_connection.set_autocommit(True)
            new_connection.close()

    def assertHasForShareOrUpdateSQL(self, operation, queries, **kwargs):
        # Examine the SQL that was executed to determine whether it
        # contains the 'SELECT..FOR SHARE/UPDATE' stanza.
        if operation == 'select_for_share':
            function = connection.ops.for_share_sql
        elif operation == 'select_for_update':
            function = connection.ops.for_update_sql
        else:
            raise ValueError('Unknown operation.')
        self.assertIs(
            any(function(**kwargs)in query['sql'] for query in queries),
            True,
        )

    def test_for_share_or_update_sql_generated(self):
        """
        The backend's FOR SHARE/UPDATE variant appears in generated SQL when
        select_for_share() or select_for_update() is invoked.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share'),
            ('select_for_update', 'has_select_for_update'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = Person.objects.all()
                    qs = getattr(qs, operation)()
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries)

    def test_for_share_or_update_sql_generated_nowait(self):
        """
        The backend's FOR SHARE/UPDATE NOWAIT variant appears in generated SQL
        when select_for_share() or select_for_update() is invoked.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_nowait'),
            ('select_for_update', 'has_select_for_update_nowait'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = Person.objects.all()
                    qs = getattr(qs, operation)(nowait=True)
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, nowait=True)

    def test_for_share_or_update_sql_generated_skip_locked(self):
        """
        The backend's FOR SHARE/UPDATE SKIP LOCKED variant appears in generated
        SQL when select_for_share() or select_for_update() is invoked.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_skip_locked'),
            ('select_for_update', 'has_select_for_update_skip_locked'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = Person.objects.all()
                    qs = getattr(qs, operation)(skip_locked=True)
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, skip_locked=True)

    @skipUnlessDBFeature('has_select_for_key_share')
    def test_for_share_sql_generated_key(self):
        """
        The backend's FOR KEY SHARE variant appears in generated SQL when
        select_for_share() is invoked.
        """
        with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
            list(Person.objects.select_for_share(key=True))
        self.assertHasForShareOrUpdateSQL('select_for_share', ctx.captured_queries, key=True)

    @skipUnlessDBFeature('has_select_for_no_key_update')
    def test_for_update_sql_generated_no_key(self):
        """
        The backend's FOR NO KEY UPDATE variant appears in generated SQL when
        select_for_update() is invoked.
        """
        with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
            list(Person.objects.select_for_update(no_key=True))
        self.assertHasForShareOrUpdateSQL('select_for_update', ctx.captured_queries, no_key=True)

    def test_for_share_or_update_sql_generated_of(self):
        """
        The backend's FOR SHARE/UPDATE OF variant appears in the generated SQL
        when select_for_share() or select_for_update() is invoked.
        """
        for operation, flag, column_flag in [
            ('select_for_share', 'has_select_for_share_of', 'select_for_share_of_column'),
            ('select_for_update', 'has_select_for_update_of', 'select_for_update_of_column'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                if getattr(connection.features, column_flag):
                    expected = ['%s_person"."id', '%s_country"."entity_ptr_id']
                else:
                    expected = ['%s_person', '%s_country']
                expected = [
                    connection.ops.quote_name(value % self.available_apps[0])
                    for value in expected
                ]
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = Person.objects.select_related('born__country')
                    qs = getattr(qs, operation)(of=['born__country'])
                    qs = getattr(qs, operation)(of=['self', 'born__country'])
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, of=expected)

    def test_for_share_or_update_sql_model_inheritance_generated_of(self):
        for operation, flag, column_flag in [
            ('select_for_share', 'has_select_for_share_of', 'select_for_share_of_column'),
            ('select_for_update', 'has_select_for_update_of', 'select_for_update_of_column'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                if getattr(connection.features, column_flag):
                    expected = ['%s_eucountry"."country_ptr_id']
                else:
                    expected = ['%s_eucountry']
                expected = [
                    connection.ops.quote_name(value % self.available_apps[0])
                    for value in expected
                ]
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = EUCountry.objects.all()
                    qs = getattr(qs, operation)(of=['self'])
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, of=expected)

    def test_for_share_or_update_sql_model_inheritance_ptr_generated_of(self):
        for operation, flag, column_flag in [
            ('select_for_share', 'has_select_for_share_of', 'select_for_share_of_column'),
            ('select_for_update', 'has_select_for_update_of', 'select_for_update_of_column'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                if getattr(connection.features, column_flag):
                    expected = ['%s_eucountry"."country_ptr_id', '%s_country"."entity_ptr_id']
                else:
                    expected = ['%s_eucountry', '%s_country']
                expected = [
                    connection.ops.quote_name(value % self.available_apps[0])
                    for value in expected
                ]
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = EUCountry.objects.all()
                    qs = getattr(qs, operation)(of=['self', 'country_ptr'])
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, of=expected)

    def test_for_share_or_update_sql_related_model_inheritance_generated_of(self):
        for operation, flag, column_flag in [
            ('select_for_share', 'has_select_for_share_of', 'select_for_share_of_column'),
            ('select_for_update', 'has_select_for_update_of', 'select_for_update_of_column'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                if getattr(connection.features, column_flag):
                    expected = ['%s_eucity"."id', '%s_eucountry"."country_ptr_id']
                else:
                    expected = ['%s_eucity', '%s_eucountry']
                expected = [
                    connection.ops.quote_name(value % self.available_apps[0])
                    for value in expected
                ]
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = EUCity.objects.select_related('country')
                    qs = getattr(qs, operation)(of=['self', 'country'])
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, of=expected)

    def test_for_share_or_update_sql_model_inheritance_nested_ptr_generated_of(self):
        for operation, flag, column_flag in [
            ('select_for_share', 'has_select_for_share_of', 'select_for_share_of_column'),
            ('select_for_update', 'has_select_for_update_of', 'select_for_update_of_column'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                if getattr(connection.features, column_flag):
                    expected = ['%s_eucity"."id', '%s_country"."entity_ptr_id']
                else:
                    expected = ['%s_eucity', '%s_country']
                expected = [
                    connection.ops.quote_name(value % self.available_apps[0])
                    for value in expected
                ]
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = EUCity.objects.select_related('country')
                    qs = getattr(qs, operation)(of=['self', 'country__country_ptr'])
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, of=expected)

    def test_for_share_or_update_sql_multilevel_model_inheritance_ptr_generated_of(self):
        for operation, flag, column_flag in [
            ('select_for_share', 'has_select_for_share_of', 'select_for_share_of_column'),
            ('select_for_update', 'has_select_for_update_of', 'select_for_update_of_column'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                if getattr(connection.features, column_flag):
                    expected = ['%s_country"."entity_ptr_id', '%s_entity"."id']
                else:
                    expected = ['%s_country', '%s_entity']
                expected = [
                    connection.ops.quote_name(value % self.available_apps[0])
                    for value in expected
                ]
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = EUCountry.objects.all()
                    qs = getattr(qs, operation)(of=['country_ptr', 'country_ptr__entity_ptr'])
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, of=expected)

    def test_for_share_or_update_sql_model_proxy_generated_of(self):
        for operation, flag, column_flag in [
            ('select_for_share', 'has_select_for_share_of', 'select_for_share_of_column'),
            ('select_for_update', 'has_select_for_update_of', 'select_for_update_of_column'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                if getattr(connection.features, column_flag):
                    expected = ['%s_country"."entity_ptr_id']
                else:
                    expected = ['%s_country']
                expected = [
                    connection.ops.quote_name(value % self.available_apps[0])
                    for value in expected
                ]
                with transaction.atomic(), CaptureQueriesContext(connection) as ctx:
                    qs = CityCountryProxy.objects.select_related('country')
                    qs = getattr(qs, operation)(of=['country'])
                    list(qs)
                self.assertHasForShareOrUpdateSQL(operation, ctx.captured_queries, of=expected)

    def test_for_share_or_update_of_followed_by_values(self):
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic():
                    qs = Person.objects.all()
                    qs = getattr(qs, operation)(of=['self'])
                    values = list(qs.values('pk'))
                self.assertEqual(values, [{'pk': self.person.pk}])

    def test_for_share_or_update_of_followed_by_values_list(self):
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic():
                    qs = Person.objects.all()
                    qs = getattr(qs, operation)(of=['self'])
                    values = list(qs.values_list('pk'))
                self.assertEqual(values, [(self.person.pk,)])

    def test_for_share_or_update_of_self_when_self_is_not_selected(self):
        """
        select_for_share(of=['self']) or select_for_update(of=['self']) when
        the only columns selected are from related tables.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic():
                    qs = Person.objects.select_related('born')
                    qs = getattr(qs, operation)(of=['self'])
                    values = list(qs.values('born__name'))
                self.assertEqual(values, [{'born__name': self.city1.name}])

    def test_nowait_raises_error_on_block(self):
        """
        If nowait is specified, we expect an error to be raised rather than
        blocking.
        """
        for operation, flag in [
            # FIXME: ('select_for_share', 'has_select_for_share_nowait'),
            ('select_for_update', 'has_select_for_update_nowait'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.blocking_transaction(operation):
                    status = []
                    thread = threading.Thread(
                        target=self.run_select_for_share_or_update,
                        args=(operation, status),
                        kwargs={'nowait': True},
                    )
                    thread.start()
                    time.sleep(1)
                    thread.join()
                self.assertIsInstance(status[-1], DatabaseError)

    def test_skip_locked_skips_locked_rows(self):
        """
        If skip_locked is specified, the locked row is skipped resulting in
        Person.DoesNotExist.
        """
        for operation, flag in [
            # FIXME: ('select_for_share', 'has_select_for_share_skip_locked'),
            ('select_for_update', 'has_select_for_update_skip_locked'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.blocking_transaction(operation):
                    status = []
                    thread = threading.Thread(
                        target=self.run_select_for_share_or_update,
                        args=(operation, status),
                        kwargs={'skip_locked': True},
                    )
                    thread.start()
                    time.sleep(1)
                    thread.join()
                self.assertIsInstance(status[-1], Person.DoesNotExist)

    def test_unsupported_nowait_raises_error(self):
        """
        NotSupportedError is raised if a SELECT...FOR SHARE/UPDATE NOWAIT is
        run on a database backend that supports FOR SHARE/UPDATE but not
        NOWAIT.
        """
        msg = 'NOWAIT is not supported on this database backend.'
        for operation, inc, exc in [
            ('select_for_share', 'has_select_for_share', 'has_select_for_share_nowait'),
            ('select_for_update', 'has_select_for_update', 'has_select_for_update_nowait'),
        ]:
            if not getattr(connection.features, inc) or getattr(connection.features, exc):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.assertRaisesMessage(NotSupportedError, msg):
                    with transaction.atomic():
                        qs = Person.objects.all()
                        qs = getattr(qs, operation)(nowait=True)
                        qs.get()

    def test_unsupported_skip_locked_raises_error(self):
        """
        NotSupportedError is raised if a SELECT...FOR SHARE/UPDATE SKIP LOCKED
        is run on a database backend that supports FOR SHARE/UPDATE but not
        SKIP LOCKED.
        """
        msg = 'SKIP LOCKED is not supported on this database backend.'
        for operation, inc, exc in [
            ('select_for_share', 'has_select_for_share', 'has_select_for_share_skip_locked'),
            ('select_for_update', 'has_select_for_update', 'has_select_for_update_skip_locked'),
        ]:
            if not getattr(connection.features, inc) or getattr(connection.features, exc):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.assertRaisesMessage(NotSupportedError, msg):
                    with transaction.atomic():
                        qs = Person.objects.all()
                        qs = getattr(qs, operation)(skip_locked=True)
                        qs.get()

    def test_unsupported_of_raises_error(self):
        """
        NotSupportedError is raised if a SELECT...FOR SHARE/UPDATE OF... is run
        on a database backend that supports FOR SHARE/UPDATE but not OF.
        """
        for operation, inc, exc, sql in [
            ('select_for_share', 'has_select_for_share', 'has_select_for_share_of', 'FOR SHARE OF'),
            ('select_for_update', 'has_select_for_update', 'has_select_for_update_of', 'FOR UPDATE OF'),
        ]:
            msg = f'{sql} is not supported on this database backend.'
            if not getattr(connection.features, inc) or getattr(connection.features, exc):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.assertRaisesMessage(NotSupportedError, msg):
                    with transaction.atomic():
                        qs = Person.objects.all()
                        qs = getattr(qs, operation)(of=['self'])
                        qs.get()

    @skipIfDBFeature('has_select_for_key_share')
    @skipUnlessDBFeature('has_select_for_share')
    def test_unsuported_key_raises_error(self):
        """
        NotSupportedError is raised if a SELECT...FOR KEY SHARE... is run on a
        database backend that supports FOR SHARE but not KEY.
        """
        msg = 'FOR KEY SHARE is not supported on this database backend.'
        with self.assertRaisesMessage(NotSupportedError, msg):
            with transaction.atomic():
                Person.objects.select_for_share(key=True).get()

    @skipIfDBFeature('has_select_for_no_key_update')
    @skipUnlessDBFeature('has_select_for_update')
    def test_unsuported_no_key_raises_error(self):
        """
        NotSupportedError is raised if a SELECT...FOR NO KEY UPDATE... is run
        on a database backend that supports FOR UPDATE but not NO KEY.
        """
        msg = 'FOR NO KEY UPDATE is not supported on this database backend.'
        with self.assertRaisesMessage(NotSupportedError, msg):
            with transaction.atomic():
                Person.objects.select_for_update(no_key=True).get()

    def test_unrelated_of_argument_raises_error(self):
        """
        FieldError is raised if a non-relation field is specified in of=(...).
        """
        msg = (
            'Invalid field name(s) given in %s(of=(...)): %s. Only relational '
            'fields followed in the query are allowed. Choices are: self, '
            'born, born__country, born__country__entity_ptr.'
        )
        invalid_of = [
            ('nonexistent',),
            ('name',),
            ('born__nonexistent',),
            ('born__name',),
            ('born__nonexistent', 'born__name'),
        ]
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            for of in invalid_of:
                with self.subTest(operation=operation, of=of):
                    with self.assertRaisesMessage(FieldError, msg % (operation, ', '.join(of))):
                        with transaction.atomic():
                            qs = Person.objects.select_related('born__country')
                            qs = getattr(qs, operation)(of=of)
                            person = qs.get()

    def test_related_but_unselected_of_argument_raises_error(self):
        """
        FieldError is raised if a relation field that is not followed in the
        query is specified in of=(...).
        """
        msg = (
            'Invalid field name(s) given in %s(of=(...)): %s. Only relational '
            'fields followed in the query are allowed. Choices are: self, '
            'born, profile.'
        )
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            for name in ['born__country', 'died', 'died__country']:
                with self.subTest(operation=operation, name=name):
                    with self.assertRaisesMessage(FieldError, msg % (operation, name)):
                        with transaction.atomic():
                            qs = Person.objects.select_related('born', 'profile').exclude(profile=None)
                            qs = getattr(qs, operation)(of=[name])
                            person = qs.get()

    def test_model_inheritance_of_argument_raises_error_ptr_in_choices(self):
        msg = (
            'Invalid field name(s) given in %s(of=(...)): name. Only '
            'relational fields followed in the query are allowed. '
            'Choices are: self, %s.'
        )
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.assertRaisesMessage(
                    FieldError,
                    msg % (operation, 'country, country__country_ptr, country__country_ptr__entity_ptr'),
                ):
                    with transaction.atomic():
                        qs = EUCity.objects.select_related('country')
                        qs = getattr(qs, operation)(of=['name'])
                        qs.get()
                with self.assertRaisesMessage(
                    FieldError,
                    msg % (operation, 'country_ptr, country_ptr__entity_ptr'),
                ):
                    with transaction.atomic():
                        qs = EUCountry.objects.all()
                        qs = getattr(qs, operation)(of=['name'])
                        qs.get()

    def test_model_proxy_of_argument_raises_error_proxy_field_in_choices(self):
        msg = (
            'Invalid field name(s) given in %s(of=(...)): name. Only '
            'relational fields followed in the query are allowed. '
            'Choices are: self, country, country__entity_ptr.'
        )
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.assertRaisesMessage(FieldError, msg % operation):
                    with transaction.atomic():
                        qs = CityCountryProxy.objects.select_related('country')
                        qs = getattr(qs, operation)(of=['name'])
                        qs.get()

    def test_reverse_one_to_one_of_arguments(self):
        """
        Reverse OneToOneFields may be included in of=(...) as long as NULLs are
        excluded because LEFT JOIN isn't allowed in SELECT FOR SHARE/UPDATE.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share_of'),
            ('select_for_update', 'has_select_for_update_of'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic():
                    qs = Person.objects.select_related('profile').exclude(profile=None)
                    qs = getattr(qs, operation)(of=['profile'])
                    person = qs.get()
                self.assertEqual(person.profile, self.person_profile)

    @skipUnlessDBFeature('has_select_for_update')
    def test_for_update_after_from(self):
        features_class = connections['default'].features.__class__
        attribute_to_patch = "%s.%s.for_update_after_from" % (features_class.__module__, features_class.__name__)
        with mock.patch(attribute_to_patch, return_value=True):
            with transaction.atomic():
                self.assertIn('FOR UPDATE WHERE', str(Person.objects.filter(name='foo').select_for_update().query))

    def test_select_for_share_or_update_requires_transaction(self):
        """
        A TransactionManagementError is raised when a select_for_share or
        select_for_update query is executed outside of a transaction.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share'),
            ('select_for_update', 'has_select_for_update'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.assertRaisesMessage(
                    transaction.TransactionManagementError,
                    f'{operation} cannot be used outside of a transaction.',
                ):
                    qs = Person.objects.all()
                    qs = getattr(qs, operation)()
                    list(qs)

    def test_select_for_share_or_update_requires_transaction_only_in_execution(self):
        """
        No TransactionManagementError is raised when select_for_share or
        select_for_update is invoked outside of a transaction - only when the
        query is executed.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share'),
            ('select_for_update', 'has_select_for_update'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                qs = Person.objects.all()
                qs = getattr(qs, operation)()
                with self.assertRaisesMessage(
                    transaction.TransactionManagementError,
                    f'{operation} cannot be used outside of a transaction.',
                ):
                    list(qs)

    def test_select_for_share_or_update_with_limit(self):
        other = Person.objects.create(name='Grappeli', born=self.city1, died=self.city2)
        for operation, flag in [
            ('select_for_share', 'supports_select_for_share_with_limit'),
            ('select_for_update', 'supports_select_for_update_with_limit'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with transaction.atomic():
                    qs = Person.objects.order_by('pk')
                    qs = getattr(qs, operation)()
                    qs = list(qs[1:2])
                self.assertEqual(qs[0], other)

    def test_select_for_share_or_update_with_limit_unsupported(self):
        msg = 'LIMIT/OFFSET is not supported with %s on this database backend.'
        for operation, flag in [
            ('select_for_share', 'supports_select_for_share_with_limit'),
            ('select_for_update', 'supports_select_for_update_with_limit'),
        ]:
            if getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.assertRaisesMessage(NotSupportedError, msg % operation):
                    with transaction.atomic():
                        qs = Person.objects.order_by('pk')
                        qs = getattr(qs, operation)()
                        qs = list(qs[1:2])

    def run_select_for_share_or_update(self, operation, status, **kwargs):
        """
        Utility method that runs a SELECT FOR SHARE/UPDATE against all Person
        instances. After the select_for_share or select_for_update, it attempts
        to update the name of the only record, save, and commit.

        This function expects to run in a separate thread.
        """
        raw = kwargs.pop('raw', False)
        status.append('started')
        try:
            # We need to enter transaction management again, as this is done on
            # per-thread basis
            with transaction.atomic():
                if raw:
                    sql = self.build_raw_sql(operation, connection, Person, nowait=True)
                    qs = Person.objects.raw(sql)
                    list(qs)
                    # XXX: Attempt to write also?
                else:
                    qs = Person.objects.all()
                    qs = getattr(qs, operation)(**kwargs)
                    person = qs.get()
                    person.name = 'Fred'
                    person.save()
        except (DatabaseError, Person.DoesNotExist) as e:
            status.append(e)
        finally:
            # This method is run in a separate thread. It uses its own database
            # connection. Close it without waiting for the GC. Connection for
            # raw query cannot be closed on Oracle because cursor is still
            # open.
            if not raw or connection.vendor != 'oracle':
                connection.close()

    @skipUnlessDBFeature('supports_transactions')
    def test_block(self):
        """
        A thread running a select_for_share or select_for_update that accesses
        rows being touched by a similar operation on another connection blocks
        correctly.
        """
        for operation, flag in [
            ('select_for_share', 'has_select_for_share'),
            ('select_for_update', 'has_select_for_update'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.blocking_transaction(operation):
                    # Now, try it using the ORM's select_for_share or
                    # select_for_update facility. Do this in a separate thread.
                    status = []
                    thread = threading.Thread(
                        target=self.run_select_for_share_or_update,
                        args=(operation, status),
                    )

                    # The thread should immediately block, but we'll sleep for
                    # a bit to make sure.
                    thread.start()
                    sanity_count = 0
                    while len(status) != 1 and sanity_count < 10:
                        sanity_count += 1
                        time.sleep(1)
                    if sanity_count >= 10:
                        raise ValueError('Thread did not run and block')

                    # Check the person hasn't been updated. Since this isn't
                    # using FOR SHARE/UPDATE, it won't block.
                    p = Person.objects.get(pk=self.person.pk)
                    self.assertEqual('Reinhardt', p.name)

                # When we end our blocking transaction, our thread should be
                # able to continue.
                thread.join(5.0)

                # Check the thread has finished. Assuming it has, we should
                # find that it has updated the person's name.
                self.assertFalse(thread.is_alive())

                # We must commit the transaction to ensure that MySQL gets a
                # fresh read, since by default it runs in REPEATABLE READ mode
                transaction.commit()

                p = Person.objects.get(pk=self.person.pk)
                self.assertEqual('Fred', p.name)

                # Reset for next sub-test.
                p.name = 'Reinhardt'
                p.save()

    def test_raw_lock_not_available(self):
        """
        Running a raw query which can't obtain a FOR SHARE/UPDATE lock raises
        the correct exception
        """
        for operation, flag in [
            # FIXME: ('select_for_share', 'has_select_for_share'),
            ('select_for_update', 'has_select_for_update'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                with self.blocking_transaction(operation):
                    status = []
                    thread = threading.Thread(
                        target=self.run_select_for_share_or_update,
                        args=(operation, status),
                        kwargs={'raw': True},
                    )
                    thread.start()
                    time.sleep(1)
                    thread.join()
                self.assertIsInstance(status[-1], DatabaseError)

    @override_settings(DATABASE_ROUTERS=[TestRouter()])
    def test_for_share_or_update_on_multidb(self):
        for operation, flag in [
            ('select_for_share', 'has_select_for_share'),
            ('select_for_update', 'has_select_for_update'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation):
                qs = Person.objects.all()
                qs = getattr(qs, operation)()
                self.assertEqual(router.db_for_write(Person), qs.db)

    def test_for_share_or_update_with_get(self):
        for operation, flag in [
            ('select_for_share', 'has_select_for_share'),
            ('select_for_update', 'has_select_for_update'),
        ]:
            if not getattr(connection.features, flag):
                continue  # XXX: Cannot skip subtests, see bpo-35327.
            with self.subTest(operation), transaction.atomic():
                qs = Person.objects.all()
                qs = getattr(qs, operation)()
                person = qs.get(name='Reinhardt')
                self.assertEqual(person.name, 'Reinhardt')

    def test_for_share_or_update_nowait_and_skip_locked(self):
        msg = 'The nowait option cannot be used with skip_locked.'
        for operation in ['select_for_share', 'select_for_update']:
            with self.subTest(operation), self.assertRaisesMessage(ValueError, msg):
                qs = Person.objects.order_by('-id')
                getattr(qs, operation)(nowait=True, skip_locked=True)

    def test_for_share_or_update_ordered(self):
        """
        Subqueries should respect ordering as an ORDER BY clause may be useful
        to specify a row locking order to prevent deadlocks (#27193).
        """
        for operation in ['select_for_share', 'select_for_update']:
            with self.subTest(operation), transaction.atomic():
                qs = Person.objects.order_by('-id')
                qs = getattr(qs, operation)()
                qs = Person.objects.filter(id__in=qs)
                self.assertIn('ORDER BY', str(qs.query))

    def test_for_share_or_update_exclusivity_in_query(self):
        qs = Person.objects.all()
        qs.query.select_for_share = True
        qs.query.select_for_update = True
        msg = 'Cannot use FOR SHARE and FOR UPDATE in the same query.'
        with self.assertRaisesMessage(NotSupportedError, msg):
            list(qs)

    def test_for_share_or_update_exclusivity_in_queryset(self):
        operations = ['select_for_share', 'select_for_update']
        for operation1, operation2 in zip(operations, reversed(operations)):
            msg = f'Cannot call {operation2}() after .{operation1}()'
            with self.subTest(first=operation1, second=operation2):
                qs = Person.objects.all()
                qs = getattr(qs, operation1)()
                with self.assertRaisesMessage(TypeError, msg):
                    getattr(qs, operation2)()
