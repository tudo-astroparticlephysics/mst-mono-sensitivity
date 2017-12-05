from klaas.scripts.train_energy_regressor import main
import os
from click.testing import CliRunner
import sys
import pickle
import pandas as pd




conf_name = "E_req_NT125_MD50_ND10000"
file_name = "PT7_BT2"
if len(sys.argv) == 3:
    conf_name = sys.argv[1]
    file_name = sys.argv[2]
os.system("mkdir -p E_results")
os.system("mkdir -p E_Performance")


os.system("klaas_train_energy_regressor -v conf/" + conf_name + ".yaml processes_hdf5/" + file_name + ".hdf5 E_results/" + file_name + "_" + conf_name + ".hdf5 E_results/" + file_name + "_" + conf_name + ".pkl")
#os.system("klaas_apply_energy_regressor -y -n 20 -v conf/" + conf_name + ".yaml processes_hdf5/" + file_name + ".hdf5 E_results/apply_" + file_name + "_" + conf_name + ".hdf5 E_results/" + file_name + "_" + conf_name + ".pkl")
os.system("klaas_plot_regressor_performance -o E_Performance/Performance_" + file_name + "_" + conf_name + ".pdf conf/" + conf_name + ".yaml E_results/" + file_name + "_" + conf_name + ".hdf5 E_results/" + file_name + "_" + conf_name + ".pkl")
#os.system("fact_plot_bias_resolution E_results/" + file_name + "_" + conf_name + ".hdf5 -c conf/" + conf_name + ".yaml -o E_Performance/Performance_" + file_name + "_" + conf_name + ".pdf --threshold 0.04")
