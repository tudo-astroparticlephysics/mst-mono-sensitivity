import pickle
import os
import numpy as np
from matplotlib import use
use('Agg')
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from matplotlib.colors import LogNorm
from uncertainties import ufloat


bins_def = 3 * np.logspace(-3, 2, 51)
bins_def[50] = 350


def File_array(PT, BT):
    Files_temp = os.popen('find preprocess_pickle -name "*' + PT + '*' + BT + '*.pickle"').read().split('\n')
    Files = []
    for i in Files_temp:
        if PT in i:
            Files.append(i)
    return Files


def Plot_E(Files, Name, alle):
    Energie_array = []
    for filename in Files:
        try:
            if alle:
                E_info = pickle.load(open(filename, "rb"))["E_events"]
                for l in E_info:
                    Energie_array.append(l)
            else:
                E_info = pickle.load(open(filename, "rb"))["E_cut"]
                for l in E_info:
                    Energie_array.append(l)
        except:
            pass
    print(Name + "\t" + str(len(Energie_array)))
    Energie_array = np.array(Energie_array)
    plt.xscale('log')
    plt.yscale('log')
    n, bins, patches = plt.hist(Energie_array, bins=bins_def, facecolor='green')
    plt.xlabel('E/TeV')
    plt.ylabel('Anzahl')
    plt.title(r'Energie Verteiltung')
    plt.tight_layout()
    plt.savefig('Bilder/' + Name + '.pdf')
    plt.clf()
    return n, Energie_array


def Verteiltung_over_production(E_produktion, Energie_array, bins_def, Name):
    print(Name + "\t" + str(len(Energie_array)))
    plt.xscale('log')
    plt.yscale('log')
    n, bins, patches = plt.hist(E_produktion, bins=bins_def, facecolor='green')
    n, bins, patches = plt.hist(Energie_array, bins=bins_def, facecolor='blue')
    plt.xlabel('E/TeV')
    plt.ylabel('Anzahl')
    plt.title(r'Energie Verteiltung')
    plt.tight_layout()
    plt.savefig('Bilder/' + Name + '.pdf')
    plt.clf()

def return_num_etall(Files):
    num_shower = 0
    for filename in Files:
        pickel_file = pickle.load(open(filename, "rb"))["mc_header"]
        num_shower += pickel_file["mc_num_showers"]*pickel_file["mc_num_use"]
    return num_shower


def Plot_cog(Files, Name):
    cor_x = []
    cor_y = []
    for filename in Files:
        Ergebnisse = pickle.load(open(filename, "rb"))["info"]
        for l in range(len(Ergebnisse)):
            cor_x.append(Ergebnisse[l]["mc_core_x"][0])
            cor_y.append(Ergebnisse[l]["mc_core_y"][0])
    plt.hist2d(np.array(cor_x), np.array(cor_y), bins=40, norm=LogNorm())
    plt.colorbar()
    plt.xlabel("COR_x")
    plt.ylabel("COR_Y")
    plt.tight_layout()
    # plt.legend(loc='best')
    plt.savefig('Bilder/' + Name + '.pdf')
    plt.clf()


def create_spektrum(n_cut, n_produkt, Name, E_mittel):
    Ergebnis = []
    for i in range(len(n_produkt)):
        if n_cut[i] == 0 or n_produkt[i] == 0:
            Ergebnis.append(0)
            continue
        Ergebnis.append(n_cut[i] / n_produkt[i])
    plt.plot(E_mittel, Ergebnis, ".")
    plt.xscale('log')
    plt.xlabel('E/TeV')
    plt.ylabel('Effektiv')
    plt.title(r'Energie Verteiltung')

    plt.tight_layout()
    plt.savefig('Bilder/' + Name + '.pdf')
    plt.clf()


def cal_flaeche(Files):
    for filename in Files:
        return 2 * np.pi * pickle.load(open(filename, "rb"))["mc_header"]["mc_core_range_Y"]**2

################################################################################
#                         Calculate E and COG spektrum                         #
################################################################################


Files = File_array("o", "o")
n_all, Energie_array = Plot_E(Files, "Verteilung_E", True)
Plot_cog(Files, "Verteilung_COR")


################################################################################
#                         Calculate E and COG spektrum                         #
################################################################################


Files = File_array("7", "2")
n_cut, Energie_array = Plot_E(Files, "Verteilung_E_cut", False)
Plot_cog(Files, "Verteilung_COR_cut")

num_shower = return_num_etall(Files)
################################################################################
#                             Produktions spektrum                             #
################################################################################
N = []
for i in range(len(bins_def) - 1):
    N.append(-(1 / bins_def[i] - 1 / bins_def[i + 1]))

multiplicator = num_shower * 10 / sum(N)
N = np.array(N) * multiplicator
E_produktion = []
E_mittel = []
for i in range(len(N)):
    Energie_rein = (bins_def[i] + bins_def[i + 1]) / 2
    E_mittel.append(Energie_rein)
    for j in range(int(N[i])):
        E_produktion.append(Energie_rein)
n_produkt, bins, patches = plt.hist(E_produktion, bins=bins_def, facecolor='green')
plt.xscale('log')
plt.yscale('log')
plt.xlabel('E/TeV')
plt.ylabel('Anzahl')
plt.title(r'Energie Verteiltung')
plt.tight_layout()
plt.savefig('Bilder/Produktionsspektrum_E.pdf')
plt.clf()

Verteiltung_over_production(E_produktion, Energie_array, bins_def, "Verteiltung_Production")


create_spektrum(n_cut, n_produkt, "Produktionsspektrum",E_mittel)
create_spektrum(n_cut, n_all, "Produktionsspektrum_all",E_mittel)
create_spektrum(n_all, n_produkt, "Produktionsspektrum_all_prod",E_mittel)


# Calculate eff. Area


Files = File_array("7", "2")
Flaeche = cal_flaeche(Files)


Anzahl_gesamt = 0
Anzahl_clean = 0

for filename in Files:
    Ergebnisse = pickle.load(open(filename, "rb"))
    Anzahl_gesamt += Ergebnisse["mc_header"]["mc_num_showers"]
    Anzahl_clean += len(Ergebnisse["E_cut"])

print("gesmante_anzahl_an_Schauern:\t\t" + str(Anzahl_gesamt))
print("gesmante_anzahl_an_Schauern_mit_num_use:\t" + str(num_shower))
print("gesmante_anzahl_an_Schauern_produziert:\t" + str(len(E_produktion)))
print("gesmante_anzahl_an_Schauern_nach_Cleaning:\t" + str(Anzahl_clean))
print("Rate_gesichtet:\t" + str(Anzahl_clean / Anzahl_gesamt))
print("Gesamte_Fläche:\t" + str(Flaeche))
print("effektive_Fläche:\t" + str(Flaeche * Anzahl_clean / Anzahl_gesamt))

'''
Verteilung_E	71574
Verteilung_E_cut	71570


Anzahl_shower_produziert:       1000000
Shower Anzahl:  71570
Verhältnis:     0.07157
Fläche_ges:     39,269,908.16987241
eff_Fläche:      2,810,547.3277177685
'''
