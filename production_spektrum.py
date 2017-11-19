import pickle
import os
import numpy as np
from matplotlib import use
use('Agg')
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from matplotlib.colors import LogNorm


def Energy(x, a, b):
    return a * x**(-2) + b


Files_temp = os.popen('find preprocess_pickle -name "*o*o*.pickle"').read().split('\n')
Files = []
for i in Files_temp:
    if "o" in i:
        Files.append(i)


Energie_array = []
cor_x = []
cor_y = []

for filename in Files:
    Ergebnisse = pickle.load(open(filename, "rb"))["info"]
    for l in range(len(Ergebnisse)):
        if 'mc_E' not in Ergebnisse[l]:
            continue
        Energie_array.append(Ergebnisse[l]["mc_E"][0])
        cor_x.append(Ergebnisse[l]["mc_core_x"][0])
        cor_y.append(Ergebnisse[l]["mc_core_y"][0])


print(str(min(cor_x)) + "\t" + str(max(cor_x)))
print(str(min(cor_y)) + "\t" + str(max(cor_y)))


plt.xscale('log')
binss = np.logspace(-3, 3, 50)
n, bins, patches = plt.hist(np.array(Energie_array), bins=np.logspace(-3, 3, 50), facecolor='green')
plt.xlabel('E/TeV')
plt.ylabel('Anzahl')
plt.title(r'Energie Verteiltung')

plt.tight_layout()
plt.savefig('Bilder/Verteilung_E.pdf')
plt.clf()


plt.hist2d(np.array(cor_x), np.array(cor_y), bins=40, norm=LogNorm())
plt.colorbar()
plt.xlabel("COR_x")
plt.ylabel("COR_Y")
plt.tight_layout()
# plt.legend(loc='best')
plt.savefig('Bilder/Verteilung_COR.pdf')
plt.clf()


x = []
y = []
for i in range(17, len(binss) - 8):
    E_mid = (binss[i] + binss[i + 1]) / 2
    x.append(E_mid)
    y.append(n[i])


params, covariance = curve_fit(Energy, x, y)
E_theo = np.linspace(min(x), max(x))


plt.plot(E_theo, Energy(E_theo, *params), 'c-', label='Fit')
plt.plot(x, y, 'r.', label='Daten', markersize=2)

plt.xscale('log')
plt.xlabel(r"$E$ / TeV")
plt.ylabel(r"$dN/dE$")
plt.tight_layout()
plt.legend(loc='best')
plt.savefig('Bilder/Produktionsspektrum.pdf')
plt.clf()
