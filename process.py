import os
import pickle
import numpy as np
import matplotlib.pyplot as plt

max_energie = 0
min_energie = 0


def plot_sen(rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, nummer, name):
    global max_energie
    global min_energie
    Ergebnisse = pickle.load(open(name + "_ergebnisse.pickle", "rb"))
    Bins = 10
    rein = []
    eff = []
    gen = []
    Energie = []
    for event_id in range(len(Ergebnisse)):
        event = Ergebnisse[event_id]
        if len(event.keys()) == 0:
            continue
        for i in range(len(event["mc_E"])):
            Energie.append(event["mc_E"][i].value)
            rein.append(event["Reinheit"][i])
            eff.append(event["Effizienz"][i])
            gen.append(event["Genauigkeit"][i])

    if max(Energie) > max_energie:
        max_energie = max(Energie)
    if min(Energie) < min_energie or min_energie == 0:
        min_energie = min(Energie)
    Delta_E = (max_energie - min_energie) / (Bins - 1)
    E_array = []
    E_Delta = []
    rein_Array = []
    rein_Delta = []
    eff_Array = []
    eff_Delta = []
    gen_Array = []
    gen_Delta = []

    print('Bins Beginn')
    for i in range(Bins):
        rein_Temp = []
        eff_Temp = []
        gen_Temp = []
        beginn_E = min(Energie) + i * Delta_E
        end_E = min(Energie) + (i + 1) * Delta_E
        for j in range(len(Energie)):
            if Energie[j] >= beginn_E and Energie[j] <= end_E:
                rein_Temp.append(rein[j])
                eff_Temp.append(eff[j])
                gen_Temp.append(gen[j])

        E_array.append(beginn_E + Delta_E / 2)
        #E_Delta.append(Delta_E / 2)
        E_Delta.append(0)
        if eff_Temp == []:
            rein_Array.append(0)
            rein_Delta.append(0)
            eff_Array.append(0)
            eff_Delta.append(0)
            gen_Array.append(0)
            gen_Delta.append(0)
        else:
            rein_Temp = np.array(rein_Temp)
            rein_Array.append(np.mean(rein_Temp))
            rein_Delta.append(np.var(rein_Temp))
            eff_Temp = np.array(eff_Temp)
            eff_Array.append(np.mean(eff_Temp))
            eff_Delta.append(np.var(eff_Temp))
            gen_Temp = np.array(gen_Temp)
            gen_Array.append(np.mean(gen_Temp))
            gen_Delta.append(np.var(gen_Temp))
            # Abweichung_Delta.append(0)

    print('Bins End')
    plt.figure(1)
    plt.errorbar(E_array, rein_Array, yerr=rein_Delta, xerr=E_Delta, label=name, fmt='x')

    plt.figure(2)
    plt.errorbar(E_array, eff_Array, yerr=eff_Delta, xerr=E_Delta, label=name, fmt='x')

    plt.figure(3)
    plt.errorbar(E_array, gen_Array, yerr=gen_Delta, xerr=E_Delta, label=name, fmt='x')

    rein_g.append(np.mean(np.array(rein)))
    rein_delta_g.append(np.var(np.array(rein)))
    eff_g.append(np.mean(np.array(eff)))
    eff_delta_g.append(np.var(np.array(eff)))
    gen_g.append(np.mean(np.array(gen)))
    gen_delta_g.append(np.var(np.array(gen)))
    nummer.append(int(name))

    return rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, nummer


def plot_sensetivity():
    Liste = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

    rein_g = []
    rein_delta_g = []
    eff_g = []
    eff_delta_g = []
    gen_g = []
    gen_delta_g = []
    nummer = []

    for i in Liste:
        rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, nummer = plot_sen(rein_g, rein_delta_g, eff_g, eff_delta_g, gen_g, gen_delta_g, nummer, str(i))

    plt.figure(1)
    plt.title("Reinheit")
    plt.xlabel(r"$E_{mc}$ / TeV")
    plt.ylabel(r"Reinheit")
    plt.tight_layout()
    plt.legend(loc='best')
    plt.savefig('Reinheit_E.pdf')

    plt.figure(2)
    plt.title("Effizienz")
    plt.xlabel(r"$E_{mc}$ / TeV")
    plt.ylabel(r"Effizienz")
    plt.tight_layout()
    plt.legend(loc='best')
    plt.savefig('Effizienz_E.pdf')

    plt.figure(3)
    plt.title("Genauigkeit")
    plt.xlabel(r"$E_{mc}$ / TeV")
    plt.ylabel(r"Genauigkeit")
    plt.tight_layout()
    plt.legend(loc='best')
    plt.savefig('Genauigkeit_E.pdf')
    plt.clf()

    plt.errorbar(nummer, rein_g, yerr=rein_delta_g, fmt='x', label='Reinheit')
    plt.errorbar(nummer, eff_g, yerr=eff_delta_g, fmt='x', label='Effizienz')
    plt.errorbar(nummer, gen_g, yerr=gen_delta_g, fmt='x', label='Genauigkeit')
    plt.title("Reinheit, Effizienz, Genauigkeit")
    plt.xlabel(r"Threshold")
    # plt.ylabel(r"Genauigkeit")
    plt.tight_layout()
    plt.legend(loc='best')
    plt.savefig('R_E_G_Threshold.pdf')
    plt.clf()


plot_sensetivity()
