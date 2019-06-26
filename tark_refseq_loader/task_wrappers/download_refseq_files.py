"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
import subprocess

import luigi
from luigi.contrib.lsf import LSFJobTask

import requests


class DownloadRefSeqSourceFile(LSFJobTask):

    downloaded_file = luigi.Parameter()
    ftp_url = luigi.Parameter()

    task_namespace = 'DownloadRefSeqSourceFile'

    def output(self):
        return luigi.LocalTarget(
            self.download_file
        )

    def work(self):
        """
        Worker function to download the file from refseq ftp source
        Parameters
        ----------
        download_dir : str
            Location of the download dir
        file_to_download : str
            File to download
        ftp_root : str
            Refseq ftp path
        """

        if not os.path.exists(os.path.dirname(self.download_file)):
            os.makedirs(self.download_file)

        with requests.get(self.ftp_url, stream=True) as refseq_stream:
            refseq_stream.raise_for_status()
            with open(self.downloaded_file, 'wb') as dl_file:
                for chunk in refseq_stream.iter_content(chunk_size=8192):
                    if chunk:
                        dl_file.write(chunk)


class UnzipRefSeqFile(LSFJobTask):

    zipped_file = luigi.Parameter()
    unzipped_file = luigi.Parameter()

    task_namespace = 'UnzipRefSeqFile'

    def output(self):
        return luigi.LocalTarget(
            self.unzipped_file
        )

    def work(self):
        with open(self.unzipped_file, 'w') as fp:
            subprocess.run(
                [
                    "gunzip", "-c",
                    self.zipped_file
                ],
                stdout=fp
            )
