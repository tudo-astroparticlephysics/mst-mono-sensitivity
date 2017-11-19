import pickle
import pandas as pd
import os
import numpy as np
from fact.io import to_h5py
import h5py
import multiprocessing


def pi_to_hdf(i, j):
    #print(str(i) + "\t" + str(j))
    Keylist = ['size', 'cen_x', 'cen_y', 'lenght', 'width', 'r', 'phi', 'psi', 'miss', 'skewness', 'kurtosis', 'mc_E', 'mc_altitude', 'mc_azimuth', 'mc_core_x', 'mc_core_y', 'mc_h_first_int', 'mc_azimuth_raw', 'mc_altitude_raw', 'mc_azimuth_cor', 'mc_altitude_cor', 'mc_time_slice', 'mc_refstep', 'tel_id', 'mc_gamma_proton', 'Reinheit', 'Effizienz', 'Genauigkeit', 'TP', 'FP', 'TN', 'FN']
    Keylist_o = ["mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_spectral_index", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_time_slice", "mc_refstep", "tel_id", "mc_gamma_proton"]

    os.system("mkdir -p processes_hdf5")
    file_n = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    start = True
    err = False
    Anzahl = 0
    index = 0
    with pd.HDFStore('processes_hdf5/PT' + str(i) + '_BT' + str(j) + '.hdf5', 'w') as store:
        for k in file_n:
            try:
                Ergebnisse = pickle.load(open("preprocess_pickle/F" + str(k) + "_PT" + str(i) + "_BT" + str(j) + "_ergebnisse.pickle", "rb"))["info"]
            except:
                continue
            print(str(i) + "\t" + str(j) + "\t" + str(k))
            for l in range(len(Ergebnisse)):
                #print(str(l) + "/" + str(len(Ergebnisse)))
                if 'mc_E' not in Ergebnisse[l]:
                    Anzahl += 1
                    continue
                for m in Ergebnisse[l]['mc_E']:
                    Ergebniss = {}
                    if "o" == i:
                        for key in Keylist_o:
                            if key not in Ergebnisse[l]:
                                err = True
                                break
                            Ergebniss[key] = np.array(Ergebnisse[l][key])
                    else:
                        for key in Keylist:
                            if key not in Ergebnisse[l]:
                                err = True
                                break
                            Ergebniss[key] = np.array(Ergebnisse[l][key])
                    if err:
                        print("\t\t\tError")
                        Anzahl += 1
                        continue
                    df = pd.DataFrame(Ergebniss, index=[index])
                    store.append('events', df)
                    index += 1
                '''
                if start:
                    to_h5py("processes_hdf5/PT" + str(i) + "_BT" + str(j) + ".hdf5", df, key='events', mode='w')
                    start = False
                else:
                    to_h5py("processes_hdf5/PT" + str(i) + "_BT" + str(j) + ".hdf5", df, key='events', mode='a')
                '''
        print("Index: " + str(i) + "\t" + str(j) + "\t" + str(index))
    return True


#pi_to_hdf(7, 2)
pool = multiprocessing.Pool(processes=6)
async_result = []

Liste = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
for i in Liste:
    for j in Liste:
        if j > i:
            continue
        async_result.append(pool.apply_async(pi_to_hdf, (i, j,)))
async_result.append(pool.apply_async(pi_to_hdf, ("o", "o",)))
pool.close()

for i in range(len(async_result)):
    print(str(i + 1) + "/" + str(len(async_result)))
    geschaftt = async_result[i].get()
