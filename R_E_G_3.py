import os
import pickle
import numpy as np
import os
from matplotlib import use
use('Agg')
import matplotlib.pyplot as plt
from matplotlib import cm

max_energie = 0
min_energie = 0


def plot_sen(rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, x, y, i, j):
    global max_energie
    global min_energie
    file_n = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    for k in file_n:
        print("\t\t" + str(k))
        try:
            Ergebnisse = pickle.load(open("results/F" + str(k) + "_PT" + str(i) + "_BT" + str(j) + "_ergebnisse.pickle", "rb"))
        except:
            return rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, x, y

        rein = []
        eff = []
        gen = []
        Energie = []
        for event_id in range(len(Ergebnisse)):
            event = Ergebnisse[event_id]
            if len(event.keys()) == 0:
                continue
            for l in range(len(event["mc_E"])):
                Energie.append(event["mc_E"][l].value)
                rein.append(event["Reinheit"][l])
                eff.append(event["Effizienz"][l])
                gen.append(event["Genauigkeit"][l])

        if max(Energie) > max_energie:
            max_energie = max(Energie)
        if min(Energie) < min_energie or min_energie == 0:
            min_energie = min(Energie)

        rein_g.append(np.mean(np.array(rein)))
        rein_delta_g.append(np.var(np.array(rein)))
        eff_g.append(np.mean(np.array(eff)))
        eff_delta_g.append(np.var(np.array(eff)))
        gen_g.append(np.mean(np.array(gen)))
        gen_delta_g.append(np.var(np.array(gen)))
        x.append(int(i))
        y.append(int(j))

    return rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, x, y


def plot_sensetivity_3():
    Liste = [0,1,2,3,4,5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

    rein_g = []
    rein_delta_g = []
    eff_g = []
    eff_delta_g = []
    gen_g = []
    gen_delta_g = []
    x = []
    y = []

    for i in Liste:
        print(str(i))
        for j in Liste:
            if j > i:
                continue
            print("\t" + str(j))
            rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, x, y = plot_sen(rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, x, y, str(i), str(j))

    os.system('mkdir -p TP')
    pickle.dump(rein_g, open("TP/Reinheit.pickle", "wb"))
    pickle.dump(rein_delta_g, open("TP/Reinheit_Delta.pickle", "wb"))
    pickle.dump(eff_g, open("TP/Effizienz.pickle", "wb"))
    pickle.dump(eff_delta_g, open("TP/Effizienz_Delta.pickle", "wb"))
    pickle.dump(gen_g, open("TP/Genauigkeit.pickle", "wb"))
    pickle.dump(gen_delta_g, open("TP/Genauigkeit_Delta.pickle", "wb"))
    pickle.dump(x, open("TP/x.pickle", "wb"))
    pickle.dump(y, open("TP/y.pickle", "wb"))

    plt.scatter(x, y, marker='o', c=rein_g)
    plt.colorbar()
    plt.title("Reinheit")
    plt.xlabel(r"Threshold")
    plt.ylabel(r"Threshold nachbarn")
    plt.tight_layout()
    plt.savefig('Bilder/R_Threshold.pdf')
    plt.clf()

    plt.scatter(x, y, marker='o', c=eff_g)
    plt.colorbar()
    plt.title("Effizienz")
    plt.xlabel(r"Threshold")
    plt.ylabel(r"Threshold nachbarn")
    plt.tight_layout()
    plt.savefig('Bilder/E_Threshold.pdf')
    plt.clf()

    plt.scatter(x, y, marker='o', c=gen_g)
    plt.colorbar()
    plt.title("Genauigkeit")
    plt.xlabel(r"Threshold")
    plt.ylabel(r"Threshold nachbarn")
    plt.tight_layout()
    plt.savefig('Bilder/G_Threshold.pdf')
    plt.clf()


plot_sensetivity_3()
