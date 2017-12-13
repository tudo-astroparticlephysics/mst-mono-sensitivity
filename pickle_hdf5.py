import pickle
import pandas as pd
import os
import numpy as np
from fact.io import to_h5py
import h5py
import multiprocessing


def File_array(PT, BT):
    Files_temp = os.popen('find preprocess_pickle -name "*' + str(PT) + '*' + str(BT) + '*.pickle"').read().split('\n')
    Files = []
    for i in Files_temp:
        if "pickle" in i:
            Files.append(i)
    return Files



def pi_to_hdf(i, j):
    #print(str(i) + "\t" + str(j))
    Keylist = ["size", "cen_x", "cen_y", "length", "width", "r", "phi", "psi", "miss", "skewness", "kurtosis", "mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_time_slice", "mc_refstep", "tel_id", "mc_gamma_proton"]
    Keylist_o = ["mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_time_slice", "mc_refstep", "tel_id", "mc_gamma_proton"]

    os.system("mkdir -p processes_hdf5")
    Files = File_array(i, j)
    start = True
    err = False
    Anzahl = 0
    index = 0
    with pd.HDFStore('processes_hdf5/PT' + str(i) + '_BT' + str(j) + '.hdf5', 'w') as store:
        Ergebnisse_in_hdf = {}
        index_array = []
        for filename in Files:
            try:
                Ergebnisse = pickle.load(open(filename, "rb"))["info"]
            except:
                continue
            print(str(i) + "\t" + str(j))

            for l in range(len(Ergebnisse)):
                #print(str(l) + "/" + str(len(Ergebnisse)))
                if 'mc_E' not in Ergebnisse[l]:
                    Anzahl += 1
                    continue
                for m in range(len(Ergebnisse[l]['mc_E'])):
                    Ergebniss = {}
                    if "o" == i:
                        for key in Keylist_o:
                            if key not in Ergebnisse_in_hdf:
                                Ergebnisse_in_hdf[key] = []
                            Ergebnisse_in_hdf[key].append(float(Ergebnisse[l][key][m]))
                    else:
                        for key in Keylist:
                            if key not in Ergebnisse_in_hdf:
                                Ergebnisse_in_hdf[key] = []
                            Ergebnisse_in_hdf[key].append(float(Ergebnisse[l][key][m]))
                    index_array.append(index)
                    index += 1
        for key in Ergebnisse_in_hdf:
            Ergebnisse_in_hdf[key] = np.array(Ergebnisse_in_hdf[key])
        df = pd.DataFrame(Ergebnisse_in_hdf, index=index_array)
        store.append('events', df)

        print("Index: " + str(i) + "\t" + str(j) + "\t" + str(index))
    return True


pool = multiprocessing.Pool(processes=6)
async_result = []

Liste = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
for i in Liste:
    if i != 7:
        continue
    for j in Liste:
        if j > i:
            continue
        if j != 2:
            continue
        async_result.append(pool.apply_async(pi_to_hdf, (i, j,)))
async_result.append(pool.apply_async(pi_to_hdf, ("o", "o",)))
pool.close()

for i in range(len(async_result)):
    print(str(i + 1) + "/" + str(len(async_result)))
    geschaftt = async_result[i].get()
