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
# from luigi import WrapperTask

import wget


class DownloadRefSeqSourceFile(LSFJobTask):

    download_dir = luigi.Parameter()
    file_to_download = luigi.Parameter()
    ftp_root = luigi.Parameter()

    task_namespace = 'DownloadRefSeqSourceFile'

    def output(self):
        return luigi.LocalTarget(
            self.download_dir + '/' + self.file_to_download
        )

    def run(self):
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

        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

        file_url = self.ftp_root + '/' + self.file_to_download
        wget.download(file_url, self.download_dir)


class UnzipRefSeqFile(LSFJobTask):

    download_dir = luigi.Parameter()
    file_to_download = luigi.Parameter()
    ftp_root = luigi.Parameter()
    task_namespace = 'UnzipRefSeqFile'

    def output(self):
        base = os.path.basename(self.file_to_download)
        downloaded_file_url_unzipped = self.download_dir + '/' + os.path.splitext(base)[0]
        return luigi.LocalTarget(downloaded_file_url_unzipped)

    def run(self):
        downloaded_file = self.download_dir + '/' + self.file_to_download
        subprocess.Popen(
            [
                "gunzip",
                downloaded_file
            ]
        )
