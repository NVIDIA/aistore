#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import tarfile
from typing import Any, Generator, Optional, Tuple, Union
from io import BytesIO

from overrides import override
from requests import Response

from aistore.sdk.batch.types import MossOut, MossReq, MossResp
from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor
from aistore.sdk.const import EXT_TARGZ, EXT_TGZ, EXT_TAR
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


class TarStreamExtractor(ArchiveStreamExtractor):
    """
    Class for extracting batch .tar, .tar.gz, and .tgz streams from AIStore.

    Integrates with Batch API (MossReq/MossResp) to provide proper metadata mapping.
    """

    _supported_fmts = (EXT_TARGZ, EXT_TGZ, EXT_TAR)

    @override
    def extract(
        self,
        response: Response,
        data_stream: Union[BytesIO, Any],
        moss_req: MossReq,
        moss_resp: Optional[MossResp] = None,
    ) -> Generator[Tuple[MossOut, bytes], None, None]:
        """
        Extract from tar archive stream.

        Args:
            response (Response): HTTP response
            data_stream (Union[BytesIO, Any]): Archive data
            moss_req (MossReq): Original request
            moss_resp (Optional[MossResp]): Response metadata (None for streaming)

        Yields:
            Tuple[MossOut, bytes]: (MossOut, content) tuples
        """
        index = 0
        try:
            with tarfile.open(fileobj=data_stream, mode="r|*") as tar:
                for tarinfo in tar:
                    if not tarinfo.isfile():
                        continue

                    try:
                        with tar.extractfile(tarinfo) as file:
                            content = file.read()

                            # Get MossOut (from response or build from request)
                            moss_out = self._get_moss_out(
                                index, len(content), moss_req, moss_resp
                            )

                            # Check for missing
                            # TODO: FIXME: what to do with missing files?
                            # moss_out.is_missing = tarinfo.name.startswith(GB_MISSING_FILES_DIR)

                            index += 1
                            yield moss_out, content

                    except (tarfile.TarError, OSError) as e:
                        # Handle individual file extraction errors
                        self._handle_extraction_error(
                            tarinfo.name, e, moss_req, EXT_TAR
                        )
                        index += 1
                        continue

        except tarfile.TarError as e:
            # Handle tar stream errors (corrupt archive, etc.)
            logger.error("Failed to read tar archive: %s", str(e))
            raise RuntimeError("Failed to read tar archive stream") from e
        finally:
            response.close()
