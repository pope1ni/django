from django.db import connection
from django.db.models import Value
from django.db.models.functions import Concat, Now, RegexpStrIndex
from django.test import TestCase

from ..models import Article, Author


class RegexpStrIndexTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.author1 = Author.objects.create(name='George R. R. Martin')
        cls.author2 = Author.objects.create(name='J. R. R. Tolkien')

    def test_null(self):
        tests = [('alias', Value(r'(R\. ){2}'))]
        if connection.vendor != 'postgresql':
            # Emulated version in PostgreSQL doesn't handle NULL passed to pattern.
            tests += [('name', None)]
        for field, pattern in tests:
            with self.subTest(field=field, pattern=pattern):
                expression = RegexpStrIndex(field, pattern)
                author = Author.objects.annotate(index=expression).get(pk=self.author1.pk)
                self.assertIsNone(author.index)

    def test_simple(self):
        expression = RegexpStrIndex('name', Value(r'(R\. ){2}'))
        queryset = Author.objects.annotate(index=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 8),
            ('J. R. R. Tolkien', 4),
        ], transform=lambda x: (x.name, x.index), ordered=False)

    def test_case_sensitive(self):
        expression = RegexpStrIndex('name', Value(r'(r\. ){2}'))
        queryset = Author.objects.annotate(index=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 0),
            ('J. R. R. Tolkien', 0),
        ], transform=lambda x: (x.name, x.index), ordered=False)

    def test_lookahead(self):
        expression = RegexpStrIndex('name', Value(r'(R\. ){2}(?=Martin)'))
        queryset = Author.objects.annotate(index=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 8),
            ('J. R. R. Tolkien', 0),
        ], transform=lambda x: (x.name, x.index), ordered=False)

    def test_lookbehind(self):
        expression = RegexpStrIndex('name', Value(r'(?<=George )(R\. ){2}'))
        queryset = Author.objects.annotate(index=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 8),
            ('J. R. R. Tolkien', 0),
        ], transform=lambda x: (x.name, x.index), ordered=False)

    def test_substitution(self):
        expression = RegexpStrIndex('name', Value(r'(R\. )\1'))
        queryset = Author.objects.annotate(index=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 8),
            ('J. R. R. Tolkien', 4),
        ], transform=lambda x: (x.name, x.index), ordered=False)

    def test_expression(self):
        expression = RegexpStrIndex(Concat(Value('Author: '), 'name'), Value(r'(R\. ){2}'))
        queryset = Author.objects.annotate(index=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 16),
            ('J. R. R. Tolkien', 12),
        ], transform=lambda x: (x.name, x.index), ordered=False)

    def test_update(self):
        Author.objects.update(age=RegexpStrIndex('name', Value(r'(Martin|Tolkien)')))
        self.assertQuerysetEqual(Author.objects.all(), [
            14,
            10,
        ], transform=lambda x: x.age, ordered=False)


class RegexpStrIndexFlagTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        Article.objects.create(
            title='Chapter One',
            text='First Line.\nSecond Line.\nThird Line.',
            written=Now(),
        )

    def test_dotall_flag(self):
        expression = RegexpStrIndex('text', Value(r'^.*$'), Value('s'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 1)

    def test_multiline_flag(self):
        expression = RegexpStrIndex('text', Value(r'^.*\Z'), Value('m'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 26)

    def test_extended_flag(self):
        if connection.vendor == 'oracle':
            # Oracle doesn't support comments in extended regular expressions.
            pattern = Value(r"""
                ^[^ ]*
                \ Line\.
            """)
        else:
            pattern = Value(r"""
                ^[^ ]*    # Match word at beginning of line.
                \ Line\.  # Another part of the pattern...
            """)
        expression = RegexpStrIndex('text', pattern, Value('x'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 1)

    def test_case_sensitive_flag(self):
        expression = RegexpStrIndex('title', Value(r'chapter'), Value('c'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 0)

    def test_case_insensitive_flag(self):
        expression = RegexpStrIndex('title', Value(r'chapter'), Value('i'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 1)

    def test_case_sensitive_flag_preferred(self):
        expression = RegexpStrIndex('title', Value(r'chapter'), Value('ic'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 0)

    def test_case_insensitive_flag_preferred(self):
        expression = RegexpStrIndex('title', Value(r'Chapter'), Value('ci'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 1)
