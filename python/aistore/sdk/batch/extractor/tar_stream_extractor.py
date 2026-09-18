#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

import tarfile
from typing import Any, Generator, Optional, Tuple, Union
from io import BytesIO

from overrides import override
from requests import Response

from aistore.sdk.batch.types import MossOut, MossReq, MossResp
from aistore.sdk.batch.extractor.archive_stream_extractor import ArchiveStreamExtractor
from aistore.sdk.const import EXT_TARGZ, EXT_TGZ, EXT_TAR, KIB
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)

# Amortize stream reads without excessive read-ahead for small archive members
_TAR_BUFFER_SIZE = 64 * KIB


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
        *,
        buffer_size: int = _TAR_BUFFER_SIZE,
    ) -> Generator[Tuple[MossOut, bytes], None, None]:
        """
        Extract from tar archive stream.

        Args:
            response (Response): HTTP response
            data_stream (Union[BytesIO, Any]): Archive data
            moss_req (MossReq): Original request
            moss_resp (Optional[MossResp]): Response metadata (None for streaming)
            buffer_size (int): Positive TAR read buffer size in bytes. Defaults to 64 KiB.

        Yields:
            Tuple[MossOut, bytes]: (MossOut, content) tuples
        """
        index = 0
        try:
            if (
                isinstance(buffer_size, bool)
                or not isinstance(buffer_size, int)
                or buffer_size <= 0
            ):
                raise ValueError("buffer_size must be a positive integer")
            with tarfile.open(
                fileobj=data_stream, mode="r|*", bufsize=buffer_size
            ) as tar:
                for tarinfo in tar:
                    if not tarinfo.isfile():
                        continue

                    try:
                        with tar.extractfile(tarinfo) as file:
                            content = file.read()

                            # Get MossOut (from response or build from request)
                            moss_out = self._get_moss_out(
                                index,
                                len(content),
                                moss_req,
                                moss_resp,
                                member_name=tarinfo.name,
                            )

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
