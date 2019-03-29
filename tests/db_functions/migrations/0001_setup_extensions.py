from unittest import mock

from django.db import migrations

try:
    from django.contrib.postgres.operations import (
        CryptoExtension, FuzzyStrMatchExtension,
    )
except ImportError:
    CryptoExtension = mock.Mock()
    FuzzyStrMatchExtension = mock.Mock()


class Migration(migrations.Migration):
    # Required for the SHA and SOUNDEX database functions.
    operations = [CryptoExtension(), FuzzyStrMatchExtension()]
