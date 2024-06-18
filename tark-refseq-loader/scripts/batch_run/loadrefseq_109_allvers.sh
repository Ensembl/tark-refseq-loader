#!/bin/bash
# This script helps to load multiple refseq version in a single run.
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=110:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=5   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=16G   # memory per node
#SBATCH -J "refseq_109ss"   # job name
#SBATCH -o "refseq_109ss.out"   # job output file
#SBATCH -e "refseq_109ss.err"   # job error file

error_handler() {
    echo "An error occurred. Exiting the script."
    echo "Debug info: Last command executed was '$BASH_COMMAND'"
    exit 1
}

# Set the script to exit on error and trap the error
set -e
trap 'error_handler' ERR


# mysql -u ensrw -h mysql-ens-tark-rel.ebi.ac.uk -p'ADD-CORRECT-PASSWORD' -P 4650 ensembl_tark_e75_to_e111_bio < /nfs/production/flicek/ensembl/applications/dare/tark_dumps/ensembl_tark_e75_to_e111_105_2022_bio.sql


release_dates=("109.20190607" "109.20190905" "109.20191205" "109.20200228" "109.20200522" "109.20200815" "109.20201120" "109.20210226" "109.20210514" "109.20211119")
USERNAME='dare'

source /hps/software/users/ensembl/repositories/${USERNAME}/tark_loader_e99/appenv/bin/activate

export PYTHONPATH="/hps/software/users/ensembl/repositories/${USERNAME}/tark_loader_e99/tark-refseq-loader/tark-refseq-loader:$PYTHONPATH"

for release_date in "${release_dates[@]}"
do
  echo "Loading release: $release_date"
  echo "creating conf file..."
  python create_config.py $release_date
  mkdir -p "/hps/nobackup/flicek/ensembl/apps/${USERNAME}/refseq_download_${release_date}"
  if [ -d "/hps/nobackup/flicek/ensembl/apps/${USERNAME}/refseq_download_${release_date}/status_logs" ]; then
      echo "Folder exists"
      # Folder exists
      rm -rf "/hps/nobackup/flicek/ensembl/apps/${USERNAME}/refseq_download_${release_date}/status_logs"
      echo -e "\n"
  fi

  download_dir="/hps/nobackup/flicek/ensembl/apps/${USERNAME}/refseq_download_${release_date}"
  
  time -p python scripts/run_tark_loader.py --download_dir="$download_dir"
  time -p python scripts/parse_gff_file.py --download_dir="$download_dir" --workers=4 --dryrun=False
  echo "Loading done for release date ${release_date}"
  echo "Dumping database"
  mysqldump -u ensrw -h mysql-ens-tark-rel.ebi.ac.uk -p'ADD-CORRECT-PASSWORD' -P 4650 ensembl_tark_e75_to_e111_bio > "/nfs/production/flicek/ensembl/applications/${USERNAME}/tark_dumps/ensembl_tark_e75_to_e111_${release_date}_bio.sql"

done


echo "Done!"


deactivate


mysqldump -u ensrw -h mysql-ens-tark-rel.ebi.ac.uk -p'ADD-CORRECT-PASSWORD' -P 4650 ensembl_tark_e75_to_e111_bio > /nfs/production/flicek/ensembl/applications/${USERNAME}/tark_dumps/ensembl_tark_e75_to_e111_GCF_000001405_20231007_bio.sql