import pickle
import pandas as pd
import os


def pickle_to_hdf5(i, j):
    os.system("mkdir -p hdf5")
    with pd.HDFStore("hdf5/PT"+str(i) + "_BT"+str(j)+".hdf5", "w") as store:
        file_n = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        for k in file_n:
            try:
                Ergebnisse = pickle.load(open("results/F" + str(k) + "_PT" + str(i) + "_BT" + str(j) + "_ergebnisse.pickle", "rb"))
            except:
                continue
            df = pd.DataFrame(Ergebnisse)
            store.append("events", df)
            if k==1:
                break
    print(pd.read_hdf("data.hdf5"))

Liste = [0,1,2,3,4,5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
for i in Liste:
    for j in Liste:
        pickle_to_hdf5(i,j)
