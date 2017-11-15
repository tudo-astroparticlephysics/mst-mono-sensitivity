import pickle
import os
import numpy as np
from matplotlib import use
use('Agg')
import matplotlib.pyplot as plt


for i in range(1, 4):
    Ergebnisse = pickle.load(open("E_results/PT7_BT2_E_req_" + str(10**i) + ".pickle", "rb"))
    print(Ergebnisse.keys())
    histogramm = []
    for j in range(len(Ergebnisse['gamma_energy_prediction'])):
        wert = (Ergebnisse['gamma_energy_prediction'][j] - Ergebnisse['mc_E'][j]) / Ergebnisse['mc_E'][j]
        if wert is not None:
            if wert >= 0 and wert <= 30:
                histogramm.append(wert)
            elif wert < 0:
                histogramm.append(wert)
    print(len(histogramm))
    n, bins, patches = plt.hist(np.array(histogramm), 50)
    plt.xlabel('Smarts')
    plt.ylabel('Probability')
    # plt.grid(True)
    plt.tight_layout()
    plt.savefig('Bilder/E_' + str(10**i) + '.pdf')
    plt.clf()
