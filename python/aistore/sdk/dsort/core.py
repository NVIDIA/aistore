import logging
import time
import json
from typing import Dict, Union
from pathlib import Path

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
                ekm_file_obj.put_content(
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

    def wait(
        self,
        timeout: int = DEFAULT_DSORT_WAIT_TIMEOUT,
        verbose: bool = True,
    ):
        """
        Wait for a dSort job to finish

        Args:
            timeout (int, optional): The maximum time to wait for the job, in seconds. Default timeout is 5 minutes.
            verbose (bool, optional): Whether to log wait status to standard output

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
            if passed > timeout:
                raise Timeout("dsort job to finish")
            finished = True
            for job_info in self.get_job_info().values():
                if job_info.metrics.aborted:
                    logger.info("DSort job '%s' aborted", self._dsort_id)
                    return
                # Shard creation is the last phase, so check if it's finished
                finished = job_info.metrics.shard_creation.finished and finished
            if finished:
                logger.info("DSort job '%s' finished", self._dsort_id)
                return
            logger.info("Waiting on dsort job '%s'...", self._dsort_id)
            time.sleep(sleep_time)
            passed += sleep_time
