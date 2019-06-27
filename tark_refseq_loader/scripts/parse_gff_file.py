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
import re

import luigi

from tark_refseq_loader.handlers.refseq.databasehandler import DatabaseHandler
from tark_refseq_loader.handlers.refseq.databasehandler import FeatureHandler
from tark_refseq_loader.handlers.refseq.confighandler import ConfigHandler

from tark_refseq_loader.task_wrappers.refseq_parser import ParseRecord

SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=4096"
MEMORY_FLAG_ALIGNMENT = "4096"
RESOURCE_FLAG_MERGE = "mem=4096"
MEMORY_FLAG_MERGE = "4096"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False


class ParseGffFileWrapper(luigi.Task):
    """
    Wrapper Task to parse gff file
    """

    download_dir = luigi.Parameter()
    dryrun = luigi.BoolParameter()
    limit_chr = luigi.Parameter()
    user_python_path = luigi.Parameter()

    gff_file = 'GCF_000001405.39_GRCh38.p13_genomic.gff'
    fasta_file = 'GCF_000001405.39_GRCh38.p13_rna.fna'
    protein_file = 'GCF_000001405.39_GRCh38.p13_protein.faa'

    def output(self):
        """
        """
        pass

    def run(self):
        downloaded_files = {}
        downloaded_files['gff'] = os.path.join(self.download_dir, self.gff_file)
        downloaded_files['fasta'] = os.path.join(self.download_dir, self.fasta_file)
        downloaded_files['protein'] = os.path.join(self.download_dir, self.protein_file)

        # Examine for available regions
        # examiner = GFF.GFFExaminer()

        # load the parent tables
        parent_ids = None
        # use for debugging only

        if not self.dryrun:
            mydb_config = ConfigHandler().getInstance().get_section_config(section_name="DATABASE")
            dbh = DatabaseHandler(
                db_config=mydb_config,
                mypool_name="mypool_parentids"
            )

            # print(dbh)
            feature_handler = FeatureHandler(dbc=dbh.get_connection())
            parent_ids = feature_handler.populate_parent_tables()

        # print(downloaded_files['gff'])

        # You could examine the file to get the possible chr, initialising it
        # to save some time
        # ... with open(downloaded_files['gff']) as gff_handle_examiner:
        #         possible_limits = examiner.available_limits(gff_handle_examiner)
        #         chromosomes = sorted(possible_limits["gff_id"].keys())
        chromosomes = [
            ('NC_000001.11',),
            ('NC_000002.12',),
            ('NC_000003.12',),
            ('NC_000004.12',),
            ('NC_000005.10',),
            ('NC_000006.12',),
            ('NC_000007.14',),
            ('NC_000008.11',),
            ('NC_000009.12',),
            ('NC_000010.11',),
            ('NC_000011.10',),
            ('NC_000012.12',),
            ('NC_000013.11',),
            ('NC_000014.9',),
            ('NC_000015.10',),
            ('NC_000016.10',),
            ('NC_000017.11',),
            ('NC_000018.10',),
            ('NC_000019.10',),
            ('NC_000020.11',),
            ('NC_000021.9',),
            ('NC_000022.11',),
            ('NC_000023.11',),
            ('NC_000024.10',),
            ('NC_012920.1',)
        ]
        limits = dict()
        # for testing
        filter_regions = None
        parse_jobs = []
        for chrom_tuple in chromosomes:
            chrom = chrom_tuple[0]
            if not chrom.startswith("NC_"):
                continue
            # print(chrom_tuple)

            seq_region = self.get_seq_region_from_refseq_accession(chrom)

            # Restrict only for filter_region
            if self.limit_chr is not None:
                if ',' in self.limit_chr:
                    filter_regions = self.limit_chr.split(',')
                else:
                    filter_regions = [self.limit_chr]

                if str(seq_region) not in filter_regions:
                    # print(" Skipping " + str(seq_region))
                    continue

            limits["gff_id"] = chrom_tuple

            parse_job = ParseRecord(
                download_dir=self.download_dir,
                downloaded_files=downloaded_files,
                seq_region=str(seq_region),
                parent_ids=parent_ids,
                limits=limits,
                dryrun=self.dryrun,
                n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
                job_name_flag="parser", save_job_info=SAVE_JOB_INFO,
                extra_bsub_args=self.user_python_path
            )
            parse_jobs.append(parse_job)

        yield parse_jobs

    def get_seq_region_from_refseq_accession(self, refseq_accession):
        matchObj = re.match( r'NC_(\d+)\.\d+', refseq_accession, re.M|re.I)  # @IgnorePep8

        if matchObj and matchObj.group(1):
            chrom = int(matchObj.group(1))
            if chrom == 23:
                return "X"
            elif chrom == 24:
                return "Y"
            elif chrom == 12920:
                return "MT"
            else:
                return chrom

if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(
        description="RefSeq Loader Pipeline Wrapper")
    PARSER.add_argument("--download_dir", default="/tmp", help="Path to where the downloaded files should be saved")
    PARSER.add_argument("--dryrun", default=".", help="Load to db or not")
    PARSER.add_argument("--workers", default="4", help="Workers")
    PARSER.add_argument("--limit_chr", default=None, help="Limit the chr")
    PARSER.add_argument("--python_path", default=sys.executable, help="")
    PARSER.add_argument("--shared_tmp_dir", help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()
    SHARED_TMP_DIR = ARGS.shared_tmp_dir

    luigi.build(
        [
            ParseGffFileWrapper(
                download_dir=ARGS.download_dir,
                dryrun=ARGS.dryrun,
                limit_chr=ARGS.limit_chr,
                user_python_path=ARGS.python_path
            )
        ],
        workers=ARGS.workers,
        local_scheduler=True
        # parallel_scheduling=True,
        # no_lock=True
    )
