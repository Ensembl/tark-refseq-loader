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

class LoadRefSeq(luigi.Task):
    """
    Pipeline for loading refseq source in to Tark database
    """
    download_dir = luigi.Parameter()
    task_namespace = 'DownloadRefSeqSourceFiles'

    ftp_root = 'http://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/latest_assembly_versions/GCF_000001405.39_GRCh38.p13'  # @IgnorePep8
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
        downloaded_protein_file_unzipped = os.path.join(
            self.download_dir,
            os.path.splitext(protein_file_base)[0]
        )
        return [
            luigi.LocalTarget(downloaded_gff_file_unzipped),
            luigi.LocalTarget(downloaded_fasta_file_unzipped),
            luigi.LocalTarget(downloaded_protein_file_unzipped)
        ]

    def run(self):
        download_jobs = []
        unzip_jobs = []
        for file_ in self.files_to_download:
            download = DownloadRefSeqSourceFile(
                download_dir=self.download_dir,
                file_to_download=file_,
                ftp_root=self.ftp_root)
            download_jobs.append(download)

            unzip = UnzipRefSeqFile(
                download_dir=self.download_dir,
                file_to_download=file_,
                ftp_root=self.ftp_root
            )
            unzip_jobs.append(unzip)

        yield download_jobs
        yield unzip_jobs


if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(
        description="RefSeq Loader Pipeline Wrapper")
    PARSER.add_argument("--download_dir", default="/tmp", help="Path to where the downloaded files should be saved")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    luigi.build(
        [
            LoadRefSeq(
                download_dir=ARGS.download_dir,
            )
        ],
        workers=25, local_scheduler=True)
