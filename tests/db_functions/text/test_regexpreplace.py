from django.db import connection
from django.db.models import Value
from django.db.models.functions import Concat, Now, RegexpReplace
from django.test import TestCase

from ..models import Article, Author


class RegexpReplaceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.author1 = Author.objects.create(name='George R. R. Martin')
        cls.author2 = Author.objects.create(name='J. R. R. Tolkien')

    def test_null(self):
        tests = [('alias', Value(r'(R\. ){2}'), Value(''))]
        if connection.vendor != 'oracle':
            # Oracle returns original string for NULL pattern and treats NULL
            # replacement as empty string returning the string with the pattern
            # removed.
            tests += [
                ('name', None, Value('')),
                ('name', Value(r'(R\. ){2}'), None),
            ]
        expected = '' if connection.features.interprets_empty_strings_as_nulls else None
        for field, pattern, replacement in tests:
            with self.subTest(field=field, pattern=pattern, replacement=replacement):
                expression = RegexpReplace(field, pattern, replacement)
                author = Author.objects.annotate(replaced=expression).get(pk=self.author1.pk)
                self.assertEqual(author.replaced, expected)

    def test_simple(self):
        # The default replacement is an empty string.
        expression = RegexpReplace('name', Value(r'(R\. ){2}'))
        queryset = Author.objects.annotate(without_middlename=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 'George Martin'),
            ('J. R. R. Tolkien', 'J. Tolkien'),
        ], transform=lambda x: (x.name, x.without_middlename), ordered=False)

    def test_case_sensitive(self):
        expression = RegexpReplace('name', Value(r'(r\. ){2}'), Value(''))
        queryset = Author.objects.annotate(same_name=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 'George R. R. Martin'),
            ('J. R. R. Tolkien', 'J. R. R. Tolkien'),
        ], transform=lambda x: (x.name, x.same_name), ordered=False)

    def test_lookahead(self):
        expression = RegexpReplace('name', Value(r'(R\. ){2}(?=Martin)'), Value(''))
        queryset = Author.objects.annotate(altered_name=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 'George Martin'),
            ('J. R. R. Tolkien', 'J. R. R. Tolkien'),
        ], transform=lambda x: (x.name, x.altered_name), ordered=False)

    def test_lookbehind(self):
        expression = RegexpReplace('name', Value(r'(?<=George )(R\. ){2}'), Value(''))
        queryset = Author.objects.annotate(altered_name=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 'George Martin'),
            ('J. R. R. Tolkien', 'J. R. R. Tolkien'),
        ], transform=lambda x: (x.name, x.altered_name), ordered=False)

    def test_substitution(self):
        if connection.vendor == 'oracle':
            # Oracle doesn't support non-capturing groups.
            expression = RegexpReplace('name', Value(r'^(.*(R\. ?){2}) (.*)$'), Value(r'\3, \1'))
        elif connection.vendor == 'mysql' and not connection.mysql_is_mariadb:
            # MySQL uses dollar instead of backslash in replacement.
            expression = RegexpReplace('name', Value(r'^(.*(?:R\. ?){2}) (.*)$'), Value(r'$2, $1'))
        else:
            expression = RegexpReplace('name', Value(r'^(.*(?:R\. ?){2}) (.*)$'), Value(r'\2, \1'))
        queryset = Author.objects.annotate(flipped_name=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 'Martin, George R. R.'),
            ('J. R. R. Tolkien', 'Tolkien, J. R. R.'),
        ], transform=lambda x: (x.name, x.flipped_name), ordered=False)

    def test_expression(self):
        expression = RegexpReplace(Concat(Value('Author: '), 'name'), Value(r'.*: '), Value(''))
        queryset = Author.objects.annotate(same_name=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 'George R. R. Martin'),
            ('J. R. R. Tolkien', 'J. R. R. Tolkien'),
        ], transform=lambda x: (x.name, x.same_name), ordered=False)

    def test_update(self):
        Author.objects.update(name=RegexpReplace('name', Value(r'(R\. ){2}'), Value('')))
        self.assertQuerysetEqual(Author.objects.all(), [
            'George Martin',
            'J. Tolkien',
        ], transform=lambda x: x.name, ordered=False)

    def test_first_occurrence(self):
        expression = RegexpReplace('name', Value(r'R\. '), Value(''))
        queryset = Author.objects.annotate(single_middlename=expression)
        self.assertQuerysetEqual(queryset, [
            ('George R. R. Martin', 'George R. Martin'),
            ('J. R. R. Tolkien', 'J. R. Tolkien'),
        ], transform=lambda x: (x.name, x.single_middlename), ordered=False)


class RegexpReplaceFlagTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        Article.objects.create(
            title='Chapter One',
            text='First Line.\nSecond Line.\nThird Line.',
            written=Now(),
        )

    def test_global_flag(self):
        # MariaDB only supports replacing all occurrences - test always passes.
        expression = RegexpReplace('text', Value(r'Line'), Value('Word'), Value('g'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'First Word.\nSecond Word.\nThird Word.')

    def test_dotall_flag(self):
        expression = RegexpReplace('text', Value(r'\..'), Value(', '), Value('gs'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'First Line, Second Line, Third Line.')

    def test_multiline_flag(self):
        expression = RegexpReplace('text', Value(r' Line\.$'), Value(''), Value('gm'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'First\nSecond\nThird')

    def test_extended_flag(self):
        if connection.vendor == 'oracle':
            # Oracle doesn't support comments in extended regular expressions.
            pattern = Value(r"""
                .
                Line
                \.
            """)
        else:
            pattern = Value(r"""
                .     # Match the space character
                Line  # Match the word "Line"
                \.    # Match the period.
            """)
        expression = RegexpReplace('text', pattern, Value(''), Value('gx'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'First\nSecond\nThird')

    def test_case_sensitive_flag(self):
        expression = RegexpReplace('title', Value(r'chapter'), Value('Section'), Value('c'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'Chapter One')

    def test_case_insensitive_flag(self):
        expression = RegexpReplace('title', Value(r'chapter'), Value('Section'), Value('i'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'Section One')

    def test_case_sensitive_flag_preferred(self):
        expression = RegexpReplace('title', Value(r'chapter'), Value('Section'), Value('ic'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'Chapter One')

    def test_case_insensitive_flag_preferred(self):
        expression = RegexpReplace('title', Value(r'Chapter'), Value('Section'), Value('ci'))
        article = Article.objects.annotate(result=expression).first()
        self.assertEqual(article.result, 'Section One')
