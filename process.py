import os
import pickle

variabelen = ['h_size', 'h_cen_x', 'h_cen_y', 'h_lenght', 'h_width', 'h_r', 'h_phi', 'h_psi', 'h_miss', 'h_skewness', 'h_kurtosis', 'mc_E', 'mc_altitude', 'mc_azimuth', 'mc_core_x', 'mc_core_y', 'mc_h_first_int', 'mc_azimuth_raw', 'mc_altitude_raw', 'mc_azimuth_cor', 'mc_altitude_cor', 'mc_time_slice', 'mc_refstep', 'camera_rotation_angle', 'tel_id', 'mc_gamma_proton', 'mc_number_photon_electron', 'mc_calibration', 'mc_pedestal', 'mc_reference_pulse_shape', 'tel_pos', 'pix_pos']
Files = os.popen('find . -name "*_ergebnisse.pickle"').read().split('\n')
for var in variabelen:
    aktuelle_Value = "-100"
    aktuelle_Value2 = []
    immer_gleich = True
    for i in range(len(Files)):
        save_info = {}
        if i == len(Files) - 1:
            continue
        Filename = Files[i]


        ergebnisse = pickle.load(open(Filename, "rb"))
        for j in ergebnisse[var]:
            if aktuelle_Value == "-100":
                if var in variabelen2:
                    for k in j:
                        aktuelle_Value2.append(k)
                    continue
                aktuelle_Value = j
            else:
                if var in variabelen2:
                    if len(aktuelle_Value2) != len(j):
                        immer_gleich = False
                        break
                    for k in range(len(j)):
                        if j[k] != aktuelle_Value2[k]:
                            immer_gleich = False
                            break
                    if immer_gleich:
                        continue
                    else:
                        break
                if aktuelle_Value != j:
                    immer_gleich = False
                    break
        if immer_gleich is False:
            break
    print(var + "\t" + str(immer_gleich))
