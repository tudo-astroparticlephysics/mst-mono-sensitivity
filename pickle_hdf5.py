import pickle
import pandas as pd
import os
import numpy as np
from fact.io import to_h5py


def pickle_to_hdf5(i, j):
    Keylist = ['size', 'cen_x', 'cen_y', 'lenght', 'width', 'r', 'phi', 'psi', 'miss', 'skewness', 'kurtosis', 'mc_E', 'mc_altitude', 'mc_azimuth', 'mc_core_x', 'mc_core_y', 'mc_h_first_int', 'mc_azimuth_raw', 'mc_altitude_raw', 'mc_azimuth_cor', 'mc_altitude_cor', 'mc_time_slice', 'mc_refstep', 'camera_rotation_angle', 'tel_id', 'mc_gamma_proton', 'Reinheit', 'Effizienz', 'Genauigkeit', 'TP', 'FP', 'TN', 'FN']
    os.system("mkdir -p hdf5")
    file_n = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    start = True
    err = False
    Anzahl = 0
    for k in file_n:
        print("\t\t" + str(k))
        try:
            Ergebnisse = pickle.load(open("results/F" + str(k) + "_PT" + str(i) + "_BT" + str(j) + "_ergebnisse.pickle", "rb"))
        except:
            continue
        for l in range(len(Ergebnisse)):
            Ergebniss = {}
            for key in Keylist:
                if key not in Ergebnisse[l]:
                    err = True
                    break
                Ergebniss[key] = np.array(Ergebnisse[l][key])
            if err:
                print("\t\t\tError")
                Anzahl += 1
                continue
            df = pd.DataFrame(Ergebniss)
            if start:
                to_h5py("hdf5/PT" + str(i) + "_BT" + str(j) + ".hdf5", df, key='events', mode='w')
                start = False
            else:
                to_h5py("hdf5/PT" + str(i) + "_BT" + str(j) + ".hdf5", df, key='events', mode='a')
        print(Anzahl)


Liste = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
for i in Liste:
    print(str(i))
    for j in Liste:
        print("\t" + str(j))
        pickle_to_hdf5(i, j)
