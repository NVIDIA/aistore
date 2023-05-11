import json
import logging
import time
from typing import Dict

from aistore.sdk.const import (
    HTTP_METHOD_POST,
    URL_PATH_DSORT,
    HTTP_METHOD_GET,
    DEFAULT_DSORT_WAIT_TIMEOUT,
    HTTP_METHOD_DELETE,
    DSORT_ABORT,
    DSORT_UUID,
)
from aistore.sdk.dsort_types import DsortMetrics
from aistore.sdk.errors import Timeout
from aistore.sdk.utils import validate_file, probing_frequency


class Dsort:
    """
    Class for managing jobs for the dSort extension: https://github.com/NVIDIA/aistore/blob/master/docs/cli/dsort.md
    """

    def __init__(self, client: "Client", dsort_id: str = ""):
        self._client = client
        self._dsort_id = dsort_id

    @property
    def dsort_id(self) -> str:
        """
        Return dSort job id
        """
        return self._dsort_id

    def start(self, spec_file: str) -> str:
        """
        Start a dSort job with a provided spec file location
        Returns:
            dSort job ID
        """
        validate_file(spec_file)
        with open(spec_file, "r", encoding="utf-8") as file_data:
            spec = json.load(file_data)
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

    def metrics(self) -> Dict[str, DsortMetrics]:
        """
        Get metrics for a dsort job
        Returns:
            Dictionary of metrics for jobs associated with this dsort job
        """
        qparam = {DSORT_UUID: [self._dsort_id]}
        return self._client.request_deserialize(
            HTTP_METHOD_GET,
            path=URL_PATH_DSORT,
            res_model=Dict[str, DsortMetrics],
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
            for metric in self.metrics().values():
                if metric.aborted:
                    logger.info("DSort job '%s' aborted", self._dsort_id)
                    return
                # Shard creation is the last phase, so check if it's finished
                finished = metric.shard_creation.finished and finished
            if finished:
                logger.info("DSort job '%s' finished", self._dsort_id)
                return
            logger.info("Waiting on dsort job '%s'...", self._dsort_id)
            time.sleep(sleep_time)
            passed += sleep_time
