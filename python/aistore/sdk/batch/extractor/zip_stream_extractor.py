#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import zipfile
from typing import Any, Generator, Optional, Tuple, Union
from io import BytesIO

from overrides import override
from requests import Response

from aistore.sdk.batch.types import MossOut, MossReq, MossResp
from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor
from aistore.sdk.const import EXT_ZIP
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


class ZipStreamExtractor(ArchiveStreamExtractor):
    """
    Class for extracting batch .zip streams from AIStore.

    Integrates with MossReq/MossResp objects to provide proper metadata mapping.
    """

    _supported_fmts = (EXT_ZIP,)

    @override
    def extract(
        self,
        response: Response,
        data_stream: Union[BytesIO, Any],
        moss_req: MossReq,
        moss_resp: Optional[MossResp] = None,
    ) -> Generator[Tuple[MossOut, bytes], None, None]:
        """
        Extract from zip archive stream.

        Note: ZIP format requires random access, so the entire stream
        is loaded into memory before extraction begins.

        Args:
            response (Response): HTTP response
            data_stream (Union[BytesIO, Any]): Archive data
            moss_req (MossReq): Original request
            moss_resp (Optional[MossResp]): Response metadata (None for streaming)

        Yields:
            Tuple[MossOut, bytes]: (MossOut, content) tuples
        """
        # ZIP requires random access - load into memory if needed
        if not hasattr(data_stream, "read"):
            data_stream = BytesIO(data_stream)
        elif moss_req.streaming_get:
            # For streaming mode, read entire response into memory
            data_stream = BytesIO(data_stream.read())

        index = 0
        try:
            with zipfile.ZipFile(data_stream, "r") as zip_file:
                for zip_info in zip_file.infolist():
                    if zip_info.is_dir():
                        continue

                    try:
                        content = zip_file.read(zip_info.filename)

                        # Get MossOut (from response or build from request)
                        moss_out = self._get_moss_out(
                            index, len(content), moss_req, moss_resp
                        )

                        # Check for missing
                        # TODO: FIXME: what to do with missing files?
                        # moss_out.is_missing = zip_info.filename.startswith(GB_MISSING_FILES_DIR)

                        index += 1
                        yield moss_out, content

                    except (
                        zipfile.BadZipFile,
                        zipfile.LargeZipFile,
                        KeyError,
                        OSError,
                    ) as e:
                        # Handle individual file extraction errors
                        self._handle_extraction_error(
                            zip_info.filename, e, moss_req, EXT_ZIP
                        )
                        index += 1
                        continue

        except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError) as e:
            # Handle zip stream errors (corrupt archive, etc.)
            logger.error("Failed to read zip archive: %s", str(e))
            raise RuntimeError("Failed to read zip archive stream") from e
        finally:
            response.close()
