"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022-2026, NVIDIA CORPORATION. All rights reserved.
"""

import unittest
from unittest.mock import patch, Mock, MagicMock
from aistore.pytorch.map_dataset import AISMapDataset
from aistore.pytorch.iter_dataset import AISIterDataset
from aistore.sdk.enums import Colocation
from aistore.pytorch.multishard_dataset import AISMultiShardStream
from aistore.pytorch.shard_reader import AISShardReader
from aistore.pytorch.batch_iter_dataset import AISBatchIterDataset
from aistore.sdk import Bucket, Object
from tarfile import open, TarInfo, DIRTYPE, SYMTYPE, LNKTYPE, FIFOTYPE
from io import BytesIO, RawIOBase


class TestAISDataset(unittest.TestCase):
    def setUp(self) -> None:
        mock_obj = Mock()
        mock_obj.get_reader.return_value.read_all.return_value = b"mock data"
        self.mock_objects = [
            mock_obj,
            mock_obj,
        ]
        self.mock_bck = Mock(Bucket)

        self.patcher_get_objects_iterator = patch(
            "aistore.pytorch.base_iter_dataset.AISBaseIterDataset._create_objects_iter",
            side_effect=lambda: iter(self.mock_objects),
        )
        self.patcher_get_objects = patch(
            "aistore.pytorch.base_map_dataset.AISBaseMapDataset._create_objects_list",
            return_value=self.mock_objects,
        )
        self.patcher_get_objects_iterator.start()
        self.patcher_get_objects.start()

    def tearDown(self) -> None:
        self.patcher_get_objects_iterator.stop()
        self.patcher_get_objects.stop()

    def test_map_dataset(self):
        self.mock_bck.list_all_objects_iter.return_value = iter(self.mock_objects)

        ais_dataset = AISMapDataset(ais_source_list=self.mock_bck)

        self.assertIsNone(ais_dataset._etl_name)

        self.assertEqual(len(ais_dataset), 2)
        self.assertEqual(ais_dataset[0][1], b"mock data")

    def test_iter_dataset(self):
        ais_iter_dataset = AISIterDataset(ais_source_list=self.mock_bck)
        self.assertIsNone(ais_iter_dataset._etl_name)

        self.assertEqual(len(ais_iter_dataset), 2)

        for _, obj in ais_iter_dataset:
            self.assertEqual(obj, b"mock data")

    def test_multi_shard_stream(self):
        self.patcher = patch(
            "aistore.pytorch.AISMultiShardStream._get_shard_objects_iterator"
        )
        self.mock_get_shard_objects_iterator = self.patcher.start()

        self.data1 = iter([b"data1_1", b"data1_2", b"data1_3"])
        self.data2 = iter([b"data2_1", b"data2_2", b"data2_3"])
        self.data3 = iter([b"data3_1", b"data3_2", b"data3_3"])
        self.mock_get_shard_objects_iterator.side_effect = [
            self.data1,
            self.data2,
            self.data3,
        ]

        self.shards = [MagicMock(), MagicMock(), MagicMock()]

        stream = AISMultiShardStream(data_sources=self.shards)

        expected_results = [
            (b"data1_1", b"data2_1", b"data3_1"),
            (b"data1_2", b"data2_2", b"data3_2"),
            (b"data1_3", b"data2_3", b"data3_3"),
        ]

        results = list(iter(stream))

        self.assertEqual(results, expected_results)

        self.patcher.stop()

    def test_shard_reader(self):
        # Mock get_wds_samples_iter
        self.patcher = patch("aistore.pytorch.AISShardReader._create_objects_iter")
        mock_create_samples_iter = self.patcher.start()

        tar_buffer = BytesIO()
        # Open the tar file in write mode
        with open(fileobj=tar_buffer, mode="w") as tar:
            # Create some dummy content
            content = b"Content of class"

            # Create a TarInfo object to create samples
            tarinfo = TarInfo(name="sample_1.cls")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="sample_1.png")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="sample_1.jpg")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="README")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="data/")
            tarinfo.type = DIRTYPE
            tar.addfile(tarinfo)

        tar_buffer.seek(0)

        mock_shard = Mock()
        mock_shard.name = "test_shard.tar"

        mock_get = Mock()
        mock_shard.get_reader.return_value = mock_get

        mock_get.read_all.return_value = tar_buffer.getvalue()

        mock_create_samples_iter.return_value = [mock_shard]

        # Create shard reader and get results and compare
        shard_reader = AISShardReader(bucket_list=self.mock_bck)

        result = list(iter(shard_reader))

        expected_result = [
            (
                "sample_1",
                {
                    "cls": b"Content of class",
                    "png": b"Content of class",
                    "jpg": b"Content of class",
                },
            ),
        ]

        self.assertEqual(result, expected_result)

        # Ensure the iter is called correctly
        mock_create_samples_iter.assert_called()

        self.patcher.stop()

    def test_shard_reader_len_counts_on_a_forward_only_stream(self):
        """__len__ reads through ObjectReader.as_file(), which cannot seek."""

        class ForwardOnly(RawIOBase):
            def __init__(self, payload):
                self._buffer = BytesIO(payload)
                self.largest_read = 0

            def readable(self):
                return True

            def seekable(self):
                return False

            def seek(self, *args, **kwargs):
                raise OSError("seek not supported")

            def tell(self, *args, **kwargs):
                raise OSError("tell not supported")

            def read(self, size=-1):
                chunk = self._buffer.read(size)
                self.largest_read = max(self.largest_read, len(chunk))
                return chunk

            def readinto(self, buffer):
                data = self.read(len(buffer))
                buffer[: len(data)] = data
                return len(data)

        tar_buffer = BytesIO()
        with open(fileobj=tar_buffer, mode="w") as tar:
            directory = TarInfo("sub")
            directory.type = DIRTYPE
            tar.addfile(directory)
            for name in ("a.cls", "a.jpg", "b.cls", "b.jpg", "c.cls", "c.jpg"):
                content = b"x" * 70000
                info = TarInfo(name)
                info.size = len(content)
                tar.addfile(info, BytesIO(content))
            link = TarInfo("a_link.jpg")
            link.type = SYMTYPE
            link.linkname = "a.jpg"
            tar.addfile(link)

        stream = ForwardOnly(tar_buffer.getvalue())
        shard = Mock(spec=Object)
        shard.get_reader.return_value.as_file.return_value = stream
        self.mock_objects = [shard]
        shard_reader = AISShardReader(bucket_list=self.mock_bck)

        self.assertEqual(len(shard_reader), 3)
        self.assertLess(stream.largest_read, 64 * 1024)
        self.assertTrue(stream.closed)

    def test_shard_reader_len_counts_samples(self):
        """Count sample basenames from regular files that have an extension."""
        tar_buffer = BytesIO()
        content = b"Content of class"

        with open(fileobj=tar_buffer, mode="w") as tar:
            for name in ("sample_1.cls", "sample_1.png", "sample_2.cls", "README"):
                tarinfo = TarInfo(name=name)
                tarinfo.size = len(content)
                tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="data/")
            tarinfo.type = DIRTYPE
            tar.addfile(tarinfo)
            tarinfo = TarInfo(name="sample_3.png")
            tarinfo.type = SYMTYPE
            tarinfo.linkname = "sample_1.png"
            tar.addfile(tarinfo)
            tarinfo = TarInfo(name="sample_4.png")
            tarinfo.type = LNKTYPE
            tarinfo.linkname = "sample_1.png"
            tar.addfile(tarinfo)
            tarinfo = TarInfo(name="sample_5.png")
            tarinfo.type = FIFOTYPE
            tar.addfile(tarinfo)

        shard = Mock(spec=Object)
        shard.name = "test_shard.tar"
        payload = tar_buffer.getvalue()
        shard.get_reader.return_value.read_all.return_value = payload

        def open_shard_stream():
            return BytesIO(payload)

        shard.get_reader.return_value.as_file.side_effect = open_shard_stream

        patcher = patch("aistore.pytorch.AISShardReader._create_objects_iter")
        mock_create_objects_iter = patcher.start()
        self.addCleanup(patcher.stop)
        mock_create_objects_iter.return_value = [shard]

        shard_reader = AISShardReader(bucket_list=self.mock_bck, etl_name="my-etl")

        self.assertEqual(len(shard_reader), 2)
        shard.get_reader.assert_called_once_with()
        shard.get_reader.return_value.read_all.assert_not_called()

        # Iteration still applies the configured ETL.
        mock_create_objects_iter.return_value = [shard]
        self.assertEqual(sum(1 for _ in shard_reader), 2)
        self.assertEqual(
            shard.get_reader.call_args.kwargs["etl"].name,
            "my-etl",
        )

    def test_batch_iter_dataset(self):
        """Test AISBatchIterDataset functionality."""

        # Mock the client
        mock_client = Mock()

        # Create proper mock response items (now using MossOut format)
        mock_moss_out_1 = Mock(err_msg=None)
        mock_moss_out_1.obj_name = "test_obj_1"
        mock_moss_out_2 = Mock(err_msg=None)
        mock_moss_out_2.obj_name = "test_obj_2"

        # Create the response data as a list that can be iterated
        mock_response_data = [
            (mock_moss_out_1, b"batch data 1"),
            (mock_moss_out_2, b"batch data 2"),
        ]

        # Mock the batch object
        mock_batch = Mock()
        mock_batch.get.return_value = iter(mock_response_data)
        mock_client.batch.return_value = mock_batch

        # Create the batch dataset
        batch_dataset = AISBatchIterDataset(
            ais_source_list=self.mock_bck,
            client=mock_client,
        )

        # Test iteration
        results = list(batch_dataset)

        # Verify results
        expected_results = [
            ("test_obj_1", b"batch data 1"),
            ("test_obj_2", b"batch data 2"),
        ]
        self.assertEqual(results, expected_results)

        # Verify batch method was called
        mock_client.batch.assert_called()
        mock_batch.get.assert_called()

    def test_batch_iter_dataset_errors(self):
        client = Mock()
        client.batch.return_value.get.return_value = [
            (Mock(obj_name="missing.txt", err_msg="not found"), b""),
            (Mock(obj_name="empty.txt", err_msg=None), b""),
        ]
        dataset = AISBatchIterDataset(self.mock_bck, client)
        self.assertFalse(dataset.cont_on_err)
        with self.assertRaisesRegex(RuntimeError, "missing.txt: not found"):
            list(dataset)
        self.assertFalse(client.batch.call_args.kwargs["cont_on_err"])

        dataset = AISBatchIterDataset(self.mock_bck, client, cont_on_err=True)
        self.assertTrue(dataset.cont_on_err)
        with self.assertLogs(
            "aistore.pytorch.batch_iter_dataset", level="WARNING"
        ) as logs:
            self.assertEqual(list(dataset), [("empty.txt", b"")])
        self.assertIn("Skipping missing.txt: not found", logs.output[0])
        self.assertTrue(client.batch.call_args.kwargs["cont_on_err"])

    def test_batch_iter_dataset_colocation_default(self):
        """colocation defaults to Colocation.NONE."""
        mock_client = Mock()
        mock_client.batch.return_value = Mock()
        dataset = AISBatchIterDataset(
            ais_source_list=self.mock_bck,
            client=mock_client,
        )
        self.assertEqual(dataset.colocation, Colocation.NONE)

    def test_batch_iter_dataset_colocation_stored(self):
        """colocation value is stored on the dataset."""
        mock_client = Mock()
        mock_client.batch.return_value = Mock()
        for coloc in (Colocation.NONE, Colocation.TARGET_AWARE):
            dataset = AISBatchIterDataset(
                ais_source_list=self.mock_bck,
                client=mock_client,
                colocation=coloc,
            )
            self.assertEqual(dataset.colocation, coloc)

    def test_batch_iter_dataset_colocation_target_and_shard_aware_raises(self):
        """TARGET_AND_SHARD_AWARE raises NotImplementedError when client.batch() is called."""
        from aistore.sdk.batch.batch import Batch

        mock_request_client = Mock()
        with self.assertRaises(NotImplementedError):
            Batch(
                request_client=mock_request_client,
                colocation=Colocation.TARGET_AND_SHARD_AWARE,
            )

    def test_batch_iter_dataset_colocation_passthrough(self):
        """colocation is forwarded to client.batch() on each _process_batch call."""
        mock_client = Mock()
        mock_batch = Mock()
        mock_batch.get.return_value = iter([])
        mock_client.batch.return_value = mock_batch

        for coloc in (Colocation.NONE, Colocation.TARGET_AWARE):
            mock_client.reset_mock()
            mock_batch.get.return_value = iter([])
            dataset = AISBatchIterDataset(
                ais_source_list=self.mock_bck,
                client=mock_client,
                colocation=coloc,
            )
            list(dataset)
            _, kwargs = mock_client.batch.call_args
            self.assertEqual(kwargs["colocation"], coloc)

    def test_iter_dataset_preload_flag_stored(self):
        dataset = AISIterDataset(
            ais_source_list=self.mock_bck, partition_sources_by_worker=True
        )
        self.assertTrue(dataset._partition_sources_by_worker)

    def test_batch_iter_dataset_preload_flag_stored(self):
        mock_client = Mock()
        mock_client.batch.return_value = Mock()
        dataset = AISBatchIterDataset(
            ais_source_list=self.mock_bck,
            client=mock_client,
            partition_sources_by_worker=True,
        )
        self.assertTrue(dataset._partition_sources_by_worker)

    @patch("aistore.pytorch.base_iter_dataset.torch_utils.get_worker_info")
    def test_preload_calls_create_iter_with_source_slice(self, mock_worker_info):
        """With partition_sources_by_worker=True, each worker's _get_worker_iter_info calls
        _create_objects_iter with only that worker's assigned sources."""
        source_a, source_b, source_c, source_d = Mock(), Mock(), Mock(), Mock()
        dataset = AISIterDataset(
            ais_source_list=[source_a, source_b, source_c, source_d],
            partition_sources_by_worker=True,
        )
        mock_worker_info.return_value = Mock(id=1, num_workers=4)

        with patch.object(
            dataset, "_create_objects_iter", return_value=iter([])
        ) as mock_create:
            dataset._reset_iterator()
            _, worker_name = dataset._get_worker_iter_info()

            # Last call must be with sources[1::4] = [source_b]
            mock_create.assert_called_with([source_b])
            self.assertEqual(worker_name, " (Worker 1)")

    @patch("aistore.pytorch.base_iter_dataset.torch_utils.get_worker_info")
    def test_no_preload_does_not_call_create_iter_with_sources(self, mock_worker_info):
        """With partition_sources_by_worker=False, _create_objects_iter is only called once
        (by _reset_iterator) and _get_worker_iter_info uses islice on the object iterator.
        """
        dataset = AISIterDataset(
            ais_source_list=self.mock_bck,
            partition_sources_by_worker=False,
        )
        mock_worker_info.return_value = Mock(id=0, num_workers=2)

        with patch.object(
            dataset,
            "_create_objects_iter",
            side_effect=lambda sources=None: iter(self.mock_objects),
        ) as mock_create:
            dataset._reset_iterator()
            dataset._get_worker_iter_info()

            # Must only be called once (from _reset_iterator), never with a sources slice
            mock_create.assert_called_once_with()
