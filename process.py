from klaas.scripts.train_energy_regressor import main
import os
from click.testing import CliRunner


os.system("mkdir -p E_result")
#os.system("klaas_train_energy_regressor conf/E_req.yaml hdf5/PT7_BT2.hdf5 E_result/PT7_BT2.hdf5 E_result/PT7_BT2.pkl")
os.system("klaas_train_energy_regressor -v conf/E_req.yaml data.hdf5 E_result/PT7_BT2.hdf5 E_result/PT7_BT2.pkl")
os.system("klaas_apply_energy_regressor -n 20 -v conf/E_req.yaml data.hdf5 E_result/PT7_BT2.pkl")
