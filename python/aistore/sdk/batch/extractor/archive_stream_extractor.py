#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Generator, Tuple, Union, Any, Optional
from io import BytesIO
from abc import ABC, abstractmethod
from requests import Response

from aistore.sdk.batch.types import MossOut, MossReq, MossResp
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


class ArchiveStreamExtractor(ABC):
    """
    Parent class for extracting batch archive streams from AIStore.

    Integrates with MossReq/MossResp (Multi-Object Streaming Service) objects to provide
    proper metadata mapping.
    """

    _supported_fmts = tuple()

    @abstractmethod
    def extract(
        self,
        response: Response,
        data_stream: Union[BytesIO, Any],
        moss_req: MossReq,
        moss_resp: Optional[MossResp] = None,
    ) -> Generator[Tuple[MossOut, bytes], None, None]:
        """
        Extract from archive stream and yield individual file contents.

        Sequentially streams the archive to avoid memory-intensive buffering.

        Args:
            response (Response): HTTP response object containing connection for stream
            data_stream (Union[BytesIO, Any]): Archive data stream or bytes
            moss_req (MossReq): Request that fetched the archive
            moss_resp (Optional[MossResp]): Response metadata (None if streaming mode)

        Yields:
            Tuple[MossOut, bytes]: Response metadata and file content in bytes

        Raises:
            RuntimeError: If stream extraction fails
        """

    def get_supported_formats(self) -> Tuple[str, ...]:
        """
        Get formats supported by this extractor.

        Returns:
            Tuple[str]: Tuple of support formats
        """
        return self._supported_fmts

    def _get_moss_out(
        self,
        index: int,
        content_length: int,
        moss_req: MossReq,
        moss_resp: Optional[MossResp] = None,
    ) -> MossOut:
        """
        Get MossOut for the current file being extracted.

        In multipart mode (when moss_resp is provided), uses the actual response metadata.
        In streaming mode, infers MossOut from the request as streaming mode streams
        only content (no metadata).

        Args:
            index (int): Index of the file in the batch
            content_length (int): Length of file content in bytes (used to set size in streaming mode)
            moss_req (MossReq): Original batch request
            moss_resp (Optional[MossResp]): Response metadata (None for streaming mode)

        Returns:
            MossOut: Response metadata for this file
        """
        if moss_resp:
            # Multipart mode: use actual response
            return moss_resp.out[index]

        # Streaming mode: infer from request
        moss_in = moss_req.moss_in[index]
        return MossOut(
            obj_name=moss_in.obj_name,
            archpath=moss_in.archpath or "",
            bucket=moss_in.bck or "",
            provider=moss_in.provider or "",
            size=content_length,
        )

    def _handle_extraction_error(
        self,
        filename: str,
        error: Exception,
        moss_req: MossReq,
        archive_type: str,
    ) -> None:
        """
        Handle individual file extraction errors.

        If cont_on_err is enabled, logs the error and allows continuation.
        Otherwise, raises a RuntimeError.

        Args:
            filename (str): Name of the file that failed to extract
            error (Exception): The exception that occurred
            moss_req (MossReq): Original batch request
            archive_type (str): Type of archive (e.g., 'tar', 'zip')

        Raises:
            RuntimeError: If cont_on_err is False
        """
        if moss_req.cont_on_err:
            logger.error(
                "Failed to extract %s: %s. Continuing due to cont_on_err=True",
                filename,
                str(error),
            )
            return

        raise RuntimeError(
            f"Failed to extract {filename} from {archive_type} archive"
        ) from error
