import unittest
from unittest.mock import Mock

from torch.utils.data import IterDataPipe

from aistore.pytorch.aisio import AISSourceLister
from aistore.sdk.ais_source import AISSource


class TestDataPipes(unittest.TestCase):
    def test_source_lister(self):
        ais_source_1 = Mock(AISSource)
        ais_source_2 = Mock(AISSource)
        source_1_urls = ["url1", "url2"]
        source_2_urls = ["url3", "url4"]
        ais_source_1.list_urls.return_value = source_1_urls
        ais_source_2.list_urls.return_value = source_2_urls
        expected_res = source_1_urls + source_2_urls
        prefix = "obj-prefix-"
        etl_name = "test-etl"

        source_lister = AISSourceLister(
            [ais_source_1, ais_source_2], prefix=prefix, etl_name=etl_name
        )

        self.assertIsInstance(source_lister, IterDataPipe)
        self.assertEqual(expected_res, list(source_lister))
        ais_source_1.list_urls.assert_called_with(prefix=prefix, etl_name=etl_name)
        ais_source_2.list_urls.assert_called_with(prefix=prefix, etl_name=etl_name)
