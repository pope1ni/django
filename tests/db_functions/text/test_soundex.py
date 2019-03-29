from django.db import connection
from django.db.models import CharField
from django.db.models.functions import Soundex
from django.test import TestCase
from django.test.utils import register_lookup

from ..models import Author


class SoundexTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        Author.objects.bulk_create([
            Author(alias='John Smith'),
            Author(alias='Élena'),
            Author(alias='Robert'),
            Author(alias='Rupert'),
            Author(alias='Rubin'),
            Author(alias='Ashcraft'),
            Author(alias='Ashcroft'),
            Author(alias='Tymczak'),
            Author(alias='Pfister'),
            Author(alias='Honeyman'),
            Author(alias='12345'),
            Author(alias='皇帝'),
            Author(alias=''),
            Author(alias=None),
        ])

    def test_basic(self):
        authors = Author.objects.annotate(
            soundex_alias=Soundex('alias'),
        ).values_list('soundex_alias', flat=True).order_by('pk')
        self.assertSequenceEqual(
            authors,
            [
                'J525',
                'L500',
                'R163',
                'R163',
                'R150',
                'A226',  # XXX: 'A261' with MySQL.
                'A226',  # XXX: 'A261' with MySQL.
                'T522',  # XXX: 'T520' with MySQL.
                'P236',
                'H555',  # XXX: 'H500' with MySQL.
                '',
                '',
                '',
                '' if connection.features.interprets_empty_strings_as_nulls else None,
            ],
        )

    def test_transform(self):
        with register_lookup(CharField, Soundex):
            authors = Author.objects.filter(
                alias__soundex='J525',
            ).values_list('alias', flat=True)
            self.assertSequenceEqual(authors, ['John Smith'])
