from klaas.scripts.train_energy_regressor import main
import os
from click.testing import CliRunner
import sys

conf_name = "E_req"
file_name = "PT7_BT2"
if len(sys.argv) == 3:
    conf_name = sys.argv[1]
    file_name = sys.argv[2]
os.system("mkdir -p E_result")

print(conf_name)
print(file_name)

#print("klaas_train_energy_regressor -v conf/" + conf_name + ".yaml hdf5/" + file_name + ".hdf5 E_result/" + file_name + "_" + conf_name + ".hdf5 E_result/" + file_name + "_" + conf_name + ".pkl")
#print("klaas_apply_energy_regressor -n 20 -v conf/" + conf_name + ".yaml hdf5/" + file_name + ".hdf5 E_result/" + file_name + "_" + conf_name + ".pkl")
os.system("klaas_train_energy_regressor -v conf/" + conf_name + ".yaml hdf5/" + file_name + ".hdf5 E_result/" + file_name + "_" + conf_name + ".hdf5 E_result/" + file_name + "_" + conf_name + ".pkl")
os.system("klaas_apply_energy_regressor -y -n 20 -v conf/" + conf_name + ".yaml hdf5/" + file_name + ".hdf5 E_result/" + file_name + "_" + conf_name + ".pkl")
