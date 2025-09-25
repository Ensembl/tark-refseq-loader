#!/bin/bash

#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=03:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=1   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem=4G   # memory per node
#SBATCH -J "refseq_downloader"   # job name
#SBATCH -o "refseq_downloader.out"   # job output file
#SBATCH -e "refseq_downloader.err"   # job error file

source <<<activate a new virtual environment created with virtualenv>>>

export PYTHONPATH="/hps/software/users/ensembl/repositories/dare/tark_loader_e99/tark-refseq-loader/tark-refseq-loader:$PYTHONPATH"

mkdir -p /hps/nobackup/flicek/ensembl/apps/dare/refseq_download_109.20190905

time -p python scripts/run_tark_loader.py --download_dir='/hps/nobackup/flicek/ensembl/apps/dare/refseq_download_109.20190905'

deactivate