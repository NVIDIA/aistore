import logging
import time
import json
from typing import Dict, Union
from pathlib import Path
from dateutil.parser import isoparse

from aistore.sdk.const import (
    HTTP_METHOD_POST,
    URL_PATH_DSORT,
    HTTP_METHOD_GET,
    DEFAULT_DSORT_WAIT_TIMEOUT,
    HTTP_METHOD_DELETE,
    DSORT_ABORT,
    DSORT_UUID,
)
from aistore.sdk.dsort.framework import DsortFramework
from aistore.sdk.dsort.ekm import ExternalKeyMap, EKM_FILE_NAME
from aistore.sdk.dsort.types import JobInfo
from aistore.sdk.bucket import Bucket
from aistore.sdk.errors import Timeout
from aistore.sdk.wait_result import WaitResult
from aistore.sdk.request_client import RequestClient
from aistore.sdk.utils import validate_file, probing_frequency


class Dsort:
    """
    Class for managing jobs for the dSort extension: https://github.com/NVIDIA/aistore/blob/main/docs/cli/dsort.md
    """

    def __init__(self, client: RequestClient, dsort_id: str = ""):
        self._client = client
        self._dsort_id = dsort_id

    @property
    def dsort_id(self) -> str:
        """
        Return dSort job id
        """
        return self._dsort_id

    def start(self, spec: Union[str, Path, DsortFramework]) -> str:
        """
        Start a dSort job with a provided spec file location or defined framework

        Args:
            spec (Union[str, Path, DsortFramework]): Path to the spec file or a DsortFramework instance

        Returns:
            dSort job ID
        """
        if isinstance(spec, (Path, str)):
            validate_file(spec)
            dsort_framework = DsortFramework.from_file(spec)
            spec = dsort_framework.to_spec()
        elif isinstance(spec, DsortFramework):
            dsort_framework = spec
            spec = dsort_framework.to_spec()

            # If output format is an ExternalKeyMap, generate and PUT the ekm file before starting dsort
            output_format = dsort_framework.output_shards.format
            if isinstance(output_format, ExternalKeyMap):
                input_bck_name = dsort_framework.input_shards.bck.name
                input_bck = Bucket(name=input_bck_name, client=self._client)
                ekm_file_obj = input_bck.object(EKM_FILE_NAME)
                ekm_file_obj.get_writer().put_content(
                    json.dumps(output_format.as_dict()).encode("utf-8")
                )
                spec["ekm_file"] = ekm_file_obj.get_url()
                spec["ekm_file_sep"] = ""
        else:
            raise ValueError("spec must be a Path or a DsortFramework instance")

        self._dsort_id = self._client.request(
            HTTP_METHOD_POST, path=URL_PATH_DSORT, json=spec
        ).text
        return self._dsort_id

    def abort(self):
        """
        Abort a dSort job
        """
        qparam = {DSORT_UUID: [self._dsort_id]}
        self._client.request(
            HTTP_METHOD_DELETE, path=f"{URL_PATH_DSORT}/{DSORT_ABORT}", params=qparam
        )

    def get_job_info(self) -> Dict[str, JobInfo]:
        """
        Get info for a dsort job
        Returns:
            Dictionary of job info for all jobs associated with this dsort
        """
        qparam = {DSORT_UUID: [self._dsort_id]}
        return self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_DSORT,
            res_model=Dict[str, JobInfo],
            params=qparam,
        )

    # TODO: Refactor and remove pylint disable
    # pylint: disable=too-many-nested-blocks
    def wait(
        self,
        timeout: int = DEFAULT_DSORT_WAIT_TIMEOUT,
        verbose: bool = True,
    ) -> WaitResult:
        """
        Wait for a dSort job to finish

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

        Returns:
            WaitResult: Outcome of the wait operation

        Raises:
            requests.RequestException: "There was an ambiguous exception that occurred while handling..."
            requests.ConnectionError: Connection error
            requests.ConnectionTimeout: Timed out connecting to AIStore
            requests.ReadTimeout: Timed out waiting response from AIStore
            errors.Timeout: Timeout while waiting for the job to finish
        """
        logger = logging.getLogger(f"{__name__}.wait")
        logger.disabled = not verbose
        passed = 0
        sleep_time = probing_frequency(timeout)

        while True:
            job_info_dict = self.get_job_info()

            if passed > timeout:
                logger.error(
                    "Timeout waiting for dsort job '%s' after %ds. Job info: %s",
                    self._dsort_id,
                    timeout,
                    job_info_dict,
                )
                raise Timeout(f"dsort job '{self._dsort_id}'", f"after {timeout}s")

            if not job_info_dict:
                time.sleep(sleep_time)
                passed += sleep_time
                continue

            infos = list(job_info_dict.values())
            aborted = any(i.metrics.aborted for i in infos)
            finished = all(i.metrics.shard_creation.finished for i in infos)

            if not (finished or aborted):
                time.sleep(sleep_time)
                passed += sleep_time
                continue

            error = None
            for info in infos:
                if info.metrics.errors:
                    error = next((e for e in info.metrics.errors if e), None)
                    if error:
                        break

            end_time = None
            if finished:
                times = [isoparse(i.finish_time) for i in infos if i.finish_time]
                end_time = max(times) if times else None

            return WaitResult(
                job_id=self._dsort_id,
                success=finished and not aborted,
                error=error,
                end_time=end_time,
            )
