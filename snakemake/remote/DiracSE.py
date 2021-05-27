__author__ = "Ryunosuke O'Neil"
__copyright__ = "Copyright 2021, Ryunosuke O'Neil"
__email__ = "r.oneil@cern.ch"
__license__ = "MIT"

import os
import re
import shutil
import subprocess as sp
from datetime import datetime
import time

from snakemake.remote import AbstractRemoteObject, AbstractRemoteProvider
from snakemake.exceptions import WorkflowError
from snakemake.common import lazy_property
from snakemake.logging import logger
from snakemake.utils import os_sync

DIRAC_ENV_WRAPPER = ""
if shutil.which("lb-dirac"):
    # the Dirac environment is provided by
    # LbEnv (the LHCb environment)
    DIRAC_ENV_WRAPPER = "lb-dirac"
elif not shutil.which("dirac-dms-lfn-metadata"):
    raise WorkflowError(
        "Neither lb-dirac or any dirac commands were found "
        "in the environment. "
        "Dirac can not be used."
    )


class RemoteProvider(AbstractRemoteProvider):

    supports_default = True
    allows_directories = True

    def __init__(
        self,
        *args,
        keep_local=False,
        stay_on_remote=False,
        is_default=False,
        retry=2,
        **kwargs,
    ):
        super(RemoteProvider, self).__init__(
            *args,
            keep_local=keep_local,
            stay_on_remote=stay_on_remote,
            is_default=is_default,
            **kwargs,
        )
        self.retry = retry

    @property
    def default_protocol(self):
        """The protocol that is prepended to the path when no protocol is specified."""
        return "LFN://"

    @property
    def available_protocols(self):
        """List of valid protocols for this remote provider."""
        return ["LFN://"]


class RemoteObject(AbstractRemoteObject):
    mtime_re = re.compile(r"^\s*ModificationDate : (.+)$", flags=re.MULTILINE)
    size_re = re.compile(r"^\s*Size : ([0-9]+).*$", flags=re.MULTILINE)
    defaultSE = "CERN-USER"
    diracEnvWrapper = DIRAC_ENV_WRAPPER

    def __init__(self, *args, keep_local=False, provider=None, **kwargs):
        super(RemoteObject, self).__init__(
            *args, keep_local=keep_local, provider=provider, **kwargs
        )

    def _dirac(self, cmd, *args, retry=None, raise_workflow_error=True):
        if retry is None:
            retry = self.provider.retry
        _cmd = [self.diracEnvWrapper] + [cmd] + list(args)
        for i in range(retry + 1):
            try:
                logger.debug(_cmd)
                return sp.run(
                    _cmd, check=True, stderr=sp.PIPE, stdout=sp.PIPE
                ).stdout.decode()
            except sp.CalledProcessError as e:
                if i == retry:
                    if raise_workflow_error:
                        raise WorkflowError(
                            "Error calling {}:\n{}".format(
                                " ".join(cmd), e.stderr.decode()
                            )
                        )
                    else:
                        raise e
                else:
                    # try again after some seconds
                    time.sleep(1)
                    continue

    def _lfn(self):
        path = self.remote_file()
        return path.replace("LFN://", "")  # FIXME!

    def _lfn_metadata(self):
        try:
            result = self._dirac(
                "dirac-dms-lfn-metadata",
                self._lfn(),
                raise_workflow_error=False,
            )
        except sp.CalledProcessError as e:
            raise WorkflowError(
                "Error calling lb-dirac dirac-dms-lfn-metadata:\n{}".format(
                    e.stderr.decode()
                )
            )
        # TODO: parse?
        return result

    # === Implementations of abstract class members ===

    def exists(self):
        # dirac-dms-lfn-metadata [LFN]
        result = self._lfn_metadata()
        # exit code 0 means that the file is present
        return "Failed :" not in result

    def mtime(self):
        # assert self.exists()
        stat = self._lfn_metadata()
        mtime = self.mtime_re.search(stat).group(1)
        date = datetime.strptime(mtime, "%Y-%m-%d %H:%M:%S")
        return date.timestamp()

    def size(self):
        # assert self.exists()
        stat = self._lfn_metadata()
        size = self.size_re.search(stat).group(1)
        return int(size)

    def download(self):
        # dirac-dms-get-file -D [local directory] [LFN]

        # if "Downloading file",
        # if "Successful" not in output, the file does not exist
        # if "Failed" in output, the file does not exist or something went
        # wrong.
        # if "Successful :" and "[LFN]", "[local path]" in the output,
        # and the path actually exists on disk, then we are good to go.
        lfn = self._lfn()
        path = self.local_file()
        directory = os.path.dirname(path)
        try:
            result = self._dirac(
                "dirac-dms-get-file",
                "-D",
                directory,
                self._lfn(),
                raise_workflow_error=False,
            )
        except sp.CalledProcessError as e:
            raise WorkflowError(
                "Error calling lb-dirac dirac-dms-get-file:\n{}".format(
                    e.stderr.decode()
                )
            )

        assert (
            "Downloading file to" in result
        ), "dirac-dms-get-file did not even start downloading"

        if "Failed :" in result:
            raise WorkflowError(
                "Error calling lb-dirac dirac-dms-get-file:\n{}".format(result)
            )

        os_sync()

        local_file_exist = os.path.exists(path)
        if (
            "Successful :" in result
            and f"{lfn} : {path}" in result
            and local_file_exist
        ):
            return path

        return None

    def upload(self):
        # dirac-dms-add-file [LFN] [LocalPath] [defaultSE = CERN-USER]

        source = os.path.abspath(self.local_file())

        try:
            result = self._dirac(
                "dirac-dms-add-file",
                self._lfn(),
                source,
                self.defaultSE,
                raise_workflow_error=False,
            )
        except sp.CalledProcessError as e:
            raise WorkflowError(
                "Error calling lb-dirac dirac-dms-add-file:\n{}".format(
                    e.stderr.decode()
                )
            )
        assert "Successful : " in result, "Dirac file upload failed."

        return "Successful : " in result

    @property
    def list(self):
        # strip filename from pattern
        # dirac-dms-list-directory [LFN]
        # match the list of returned files

        # TODO implement listing of remote files with patterns
        raise NotImplementedError()

    def host(self):
        return self.defaultSE
