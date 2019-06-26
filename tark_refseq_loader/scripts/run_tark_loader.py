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
import sys
import argparse
import luigi

from tark_refseq_loader.task_wrappers.download_refseq_files import DownloadRefSeqSourceFile
from tark_refseq_loader.task_wrappers.download_refseq_files import UnzipRefSeqFile


# How to run?
# As local scheduler
# time PYTHONPATH='.' python scripts/run_tark_loader.py --download_dir='/hps/nobackup2/production/ensembl/xxx/refseq_download_92' @IgnorePep8

# Start the server
# luigid --background --pidfile /tmp/pid/tark-loader.pid --logdir /tmp/logs/ --state-path /tmp/state/ @IgnorePep8
# Run the loader
# time PYTHONPATH='.' python scripts/run_tark_loader.py --download_dir='/hps/nobackup2/production/ensembl/prem/refseq_download_92' @IgnorePep8

SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=4096"
MEMORY_FLAG_ALIGNMENT = "4096"
RESOURCE_FLAG_MERGE = "mem=4096"
MEMORY_FLAG_MERGE = "4096"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False


class LoadRefSeq(luigi.Task):
    """
    Pipeline for loading refseq source in to Tark database
    """
    download_dir = luigi.Parameter()
    user_python_path = luigi.Parameter()
    task_namespace = 'DownloadRefSeqSourceFiles'

    ftp_root = 'https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/latest_assembly_versions/GCF_000001405.39_GRCh38.p13'  # @IgnorePep8
    gff_file = 'GCF_000001405.39_GRCh38.p13_genomic.gff.gz'
    fasta_file = 'GCF_000001405.39_GRCh38.p13_rna.fna.gz'
    protein_file = 'GCF_000001405.39_GRCh38.p13_protein.faa.gz'

    files_to_download = [gff_file, fasta_file, protein_file]

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the RefSeq output files
        """
        gff_file_base = os.path.basename(self.gff_file)
        fasta_file_base = os.path.basename(self.fasta_file)
        protein_file_base = os.path.basename(self.protein_file)
        downloaded_gff_file_unzipped = os.path.join(
            self.download_dir,
            os.path.splitext(gff_file_base)[0]
        )
        downloaded_fasta_file_unzipped = os.path.join(
            self.download_dir,
            os.path.splitext(fasta_file_base)[0]
        )
        downloaded_prot_file_unzipped = os.path.join(
            self.download_dir,
            os.path.splitext(protein_file_base)[0]
        )
        return [
            luigi.LocalTarget(downloaded_gff_file_unzipped),
            luigi.LocalTarget(downloaded_fasta_file_unzipped),
            luigi.LocalTarget(downloaded_prot_file_unzipped)
        ]

    def run(self):
        download_jobs = []
        unzip_jobs = []
        for file_ in self.files_to_download:
            downloaded_file_zipped = os.path.join(self.download_dir, file_)

            download = DownloadRefSeqSourceFile(
                downloaded_file=downloaded_file_zipped,
                ftp_url=self.ftp_root + '/' + file_,
                n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
                job_name_flag="download", save_job_info=SAVE_JOB_INFO,
                extra_bsub_args=self.user_python_path
            )
            download_jobs.append(download)

            downloaded_file_unzipped = os.path.join(
                self.download_dir,
                os.path.splitext(os.path.basename(file_))[0]
            )
            unzip = UnzipRefSeqFile(
                zipped_file=downloaded_file_zipped,
                unzipped_file=downloaded_file_unzipped,
                n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
                job_name_flag="unzip", save_job_info=SAVE_JOB_INFO,
                extra_bsub_args=self.user_python_path
            )
            unzip_jobs.append(unzip)

        yield download_jobs
        yield unzip_jobs


if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(
        description="RefSeq Loader Pipeline Wrapper")
    PARSER.add_argument("--download_dir", default="/tmp", help="Path to where the downloaded files should be saved")
    PARSER.add_argument("--python_path", default=sys.executable, help="")
    PARSER.add_argument("--shared_tmp_dir", help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()
    SHARED_TMP_DIR = ARGS.shared_tmp_dir

    luigi.build(
        [
            LoadRefSeq(
                download_dir=ARGS.download_dir,
                user_python_path=ARGS.python_path
            )
        ],
        local_scheduler=True,
        workers=25
    )
