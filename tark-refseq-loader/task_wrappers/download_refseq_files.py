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
import luigi
import os
import wget
import subprocess
from handlers.refseq.confighandler import ConfigHandler


class DownloadRefSeqSourceFile(luigi.Task):

    download_dir = luigi.Parameter()
    file_to_download = luigi.Parameter()
    ftp_root = luigi.Parameter()

    task_namespace = 'DownloadRefSeqSourceFile'

    def output(self):
        return luigi.LocalTarget(self.download_dir + '/' + self.file_to_download)

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


class UnzipRefSeqFile(luigi.Task):

    download_dir = luigi.Parameter()
    file_to_download = luigi.Parameter()
    ftp_root = luigi.Parameter()
    task_namespace = 'UnzipRefSeqFile'

    def requires(self):
        return DownloadRefSeqSourceFile(
                download_dir=self.download_dir,
                file_to_download=self.file_to_download,
                ftp_root=self.ftp_root)

    def output(self):
        base = os.path.basename(self.file_to_download)
        downloaded_file_url_unzipped = self.download_dir + '/' + os.path.splitext(base)[0]
        return luigi.LocalTarget(downloaded_file_url_unzipped)

    def run(self):
        downloaded_file = self.download_dir + '/' + self.file_to_download
        subprocess.Popen(["gunzip",
                          downloaded_file
                          ])


class DownloadRefSeqSourceFiles(luigi.WrapperTask):
    """
    Wrapper Task to download refseq gff files
    """

    download_dir = luigi.Parameter()
    task_namespace = 'DownloadRefSeqSourceFiles'

    assembly_id = ConfigHandler().getInstance().get_section_config()["assembly_id"]
    assembly_name = ConfigHandler().getInstance().get_section_config()["assembly_name"]
    source = ConfigHandler().getInstance().get_section_config()["source"]
    shortname = ConfigHandler().getInstance().get_section_config()["shortname"]
    description = ConfigHandler().getInstance().get_section_config()["description"]
    print("Assembly ID " + str(assembly_id))
    print("Assembly Name " + str(assembly_name))
    print("source name " + str(source))
    print("shortname " + str(shortname))
    print("description  " + str(description))

    ftp_root = ConfigHandler().getInstance().get_section_config()["ftp_root"]
    gff_file = ConfigHandler().getInstance().get_section_config()["gff_file"]
    fasta_file = ConfigHandler().getInstance().get_section_config()["fasta_file"]
    protein_file = ConfigHandler().getInstance().get_section_config()["protein_file"]

    print("ftp_root {}".format(ftp_root))
    print("gff_file {}".format(gff_file))
    print("fasta_file {}".format(fasta_file))
    print("protein_file {}".format(protein_file))

#     ftp_root = 'https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/all_assembly_versions/GCF_000001405.38_GRCh38.p12/'
#     #ftp_root = 'http://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/latest_assembly_versions/GCF_000001405.38_GRCh38.p12'  # @IgnorePep8
#     gff_file = 'GCF_000001405.38_GRCh38.p12_genomic.gff.gz'
#     fasta_file = 'GCF_000001405.38_GRCh38.p12_rna.fna.gz'
#     protein_file = 'GCF_000001405.38_GRCh38.p12_protein.faa.gz'

    files_to_download = [gff_file, fasta_file, protein_file]
    # files_to_download = [gff_file]

    def complete(self):
        complete_list = []
        for file_ in self.files_to_download:
            base = os.path.basename(file_)
            downloaded_file_url_zipped = self.download_dir + '/' + file_
            downloaded_file_url_unzipped = self.download_dir + '/' + os.path.splitext(base)[0]

            if os.path.exists(downloaded_file_url_zipped) and os.path.exists(downloaded_file_url_unzipped):
                complete_list.append(True)

        if len(complete_list) == len(self.files_to_download):
            return True
        else:
            return False

    def requires(self):
        for file_ in self.files_to_download:
            yield DownloadRefSeqSourceFile(
                download_dir=self.download_dir,
                file_to_download=file_,
                ftp_root=self.ftp_root)

            yield UnzipRefSeqFile(
                download_dir=self.download_dir,
                file_to_download=file_,
                ftp_root=self.ftp_root
                     )
