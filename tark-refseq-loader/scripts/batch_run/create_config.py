### This script is used to generate the config file needed to load a refseq version into Tark. ###
### It is especially useful when loading multiple versions in a single run ###

import sys
from datetime import datetime


def format_date(date_str):
    # Ensure the string is exactly 8 characters long (YYYYMMDD)
    if len(date_str) == 8:
        # Slice the string into year, month, and day components
        year = date_str[0:4]
        month = date_str[4:6]
        day = date_str[6:8]

        # Format the date as 'YYYY-MM-DD'
        formatted_date = f"{year}-{month}-{day}"
        return formatted_date
    else:
        raise ValueError("Date must be in YYYYMMDD format and exactly 8 characters long.")


def create_config_file(annotation_release):
    cleaned_annotation_release = annotation_release.strip(', ')
    # Extract the shortname by replacing '.' with '_'
    shortname = cleaned_annotation_release.replace('.', '_')
    
    # Extract the release date and convert it to 'YYYY-MM-DD'
    release_date_str = cleaned_annotation_release.split('.')[1]

    release_date = datetime.strptime(release_date_str.strip(), '%Y%m%d').strftime('%Y-%m-%d')
    # release_date = format_date(release_date_str)

    # Define the description and ftp_path using the original annotation_release
    description = f"Refseq Homo sapiens Annotation Release {cleaned_annotation_release}"
    ftp_path = cleaned_annotation_release

    # Configuration content
    config_content = f"""[DEFAULT]
source=2
shortname={shortname}
release_date={release_date}
assembly_name=GRCh38
assembly_id=1
description={description}
ftp_root=https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/annotation_releases/110/GCF_000001405.40_GRCh38.p14/
gff_file=GCF_000001405.40_GRCh38.p14_genomic.gff.gz
fasta_file=GCF_000001405.40_GRCh38.p14_rna.fna.gz
protein_file=GCF_000001405.40_GRCh38.p14_protein.faa.gz

[DATABASE]
host = mysql-ens-tark-rel
port = 4650
user = ensrw
pass = <<<REMEMBER TO ADD PASSWORD>>>
database = ensembl_tark_e75_to_e111_bio"""

    # Write the content to a file
    with open('/hps/software/users/ensembl/repositories/dare/tark_loader_e99/tark-refseq-loader/tark-refseq-loader/conf/refseq_source.ini', 'w') as file:
        file.write(config_content)

if __name__ == "__main__":
    # Check if there is an input argument provided
    if len(sys.argv) < 2:
        print("Please provide the annotation release as an argument.")
        sys.exit(1)
    
    # The input argument should be in the form '109.20190905'
    annotation_release = sys.argv[1]

    print(annotation_release)
    
    # Call the function to create the file
    create_config_file(annotation_release)

