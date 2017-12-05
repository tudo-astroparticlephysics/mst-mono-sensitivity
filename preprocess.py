from ctapipe.calib.camera.r1 import HessioR1Calibrator
from ctapipe.calib.camera.dl0 import CameraDL0Reducer
from ctapipe.calib.camera import CameraDL1Calibrator
from ctapipe.image.charge_extractors import LocalPeakIntegrator
from ctapipe.image import tailcuts_clean, hillas_parameters
from ctapipe.io.hessio import hessio_event_source
import pickle
import os
from multiprocessing import Pool
import sys
import pandas as pd
import numpy as np
import time


def string_len(string, length):
    string = str(string)
    ersetzung = " "
    if length == 2:
        ersetzung = "0"
    if len(string) < length:
        string = ersetzung + string
    return string


def get_num_events(Filename, right_tel):
    try:
        source = hessio_event_source(Filename, allowed_tels=right_tel)
    except:
        os.exit(1)

    num_events = 0
    for event in source:
        num_events += 1
    return num_events


def set_right_tel(Filename):
    right_tel = []
    source = hessio_event_source(Filename)
    for event in source:
        for tel_id in event.r0.tels_with_data:
            if event.inst.subarray.tels[tel_id].optics.tel_subtype == "" and event.inst.subarray.tels[tel_id].optics.tel_type == "MST" and event.inst.subarray.tels[tel_id].optics.mirror_type == "DC":
                if tel_id not in right_tel:
                    right_tel.append(tel_id)
    return right_tel


def calibration(event, r1, dl0, dl1):
    r1.calibrate(event)
    dl0.reduce(event)
    dl1_calibrator.calibrate(event)


def set_hillas(event_info, hillas):
    arten = ["size", "cen_x", "cen_y", "length", "width", "r", "phi", "psi", "miss", "skewness", "kurtosis"]
    #              ,       m,       m,        m,       m,   m,   rad,   rad,      m
    for i in range(len(hillas)):
        art = arten[i]
        if art not in event_info:
            event_info[art] = []
        try:
            event_info[art].append(hillas[i].value)
        except:
            event_info[art].append(hillas[i])
    return event_info


def set_mc(event_info, event, tel_id):
    arten = ["mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_time_slice", "mc_refstep", "tel_id", "mc_gamma_proton"]
    #           Tev,           rad,          rad,           m,           m,                m,                 ,                  ,                 ,                  ,                ,             ,         ,                  ]
    for i in arten:
        if i not in event_info:
            event_info[i] = []
    event_info["mc_E"].append(event.mc.energy.value)
    event_info["mc_altitude"].append(event.mc.alt.value)
    event_info["mc_azimuth"].append(event.mc.az.value)
    event_info["mc_core_x"].append(event.mc.core_x.value)
    event_info["mc_core_y"].append(event.mc.core_y.value)
    event_info["mc_h_first_int"].append(event.mc.h_first_int.value)
    event_info["mc_azimuth_raw"].append(event.mc.tel[tel_id].azimuth_raw)
    event_info["mc_altitude_raw"].append(event.mc.tel[tel_id].altitude_raw)
    event_info["mc_azimuth_cor"].append(event.mc.tel[tel_id].azimuth_cor)
    event_info["mc_altitude_cor"].append(event.mc.tel[tel_id].altitude_cor)
    event_info["mc_time_slice"].append(event.mc.tel[tel_id].time_slice)
    event_info["mc_refstep"].append(event.mc.tel[tel_id].meta['refstep'])
    # event_info["camera_rotation_angle"].append(event.inst.camera_rotation_angle[tel_id])
    event_info["tel_id"].append(tel_id)
    event_info["mc_gamma_proton"].append(event.mc.shower_primary_id)
    return event_info


def set_mc_header(event, mc_header):
    arten = ["mc_spectral_index", "mc_obsheight", "mc_num_showers", "mc_num_use", "mc_core_pos_mode", "mc_core_range_X", "mc_core_range_Y", "mc_alt_range_Min", "mc_alt_range_Max", "mc_az_range_Min", "mc_az_range_Max", "mc_viewcone_Min", "mc_viewcone_Max", "mc_E_range_Min", "mc_E_range_Max", "mc_diffuse", "mc_injection_height"]
    for i in arten:
        if i not in mc_header:
            mc_header[i] = 0
    mc_header["mc_spectral_index"] = event.mc.spectral_index
    mc_header["mc_obsheight"] = event.mc.obsheight
    mc_header["mc_num_showers"] = event.mc.num_showers
    mc_header["mc_num_use"] = event.mc.num_use
    mc_header["mc_core_pos_mode"] = event.mc.core_pos_mode
    mc_header["mc_core_range_X"] = event.mc.core_range_X
    mc_header["mc_core_range_Y"] = event.mc.core_range_Y
    mc_header["mc_alt_range_Min"] = event.mc.alt_range_Min
    mc_header["mc_alt_range_Max"] = event.mc.alt_range_Max
    mc_header["mc_az_range_Min"] = event.mc.az_range_Min
    mc_header["mc_az_range_Max"] = event.mc.az_range_Max
    mc_header["mc_viewcone_Min"] = event.mc.viewcone_Min
    mc_header["mc_viewcone_Max"] = event.mc.viewcone_Max
    mc_header["mc_E_range_Min"] = event.mc.E_range_Min
    mc_header["mc_E_range_Max"] = event.mc.E_range_Max
    mc_header["mc_diffuse"] = event.mc.mc_diffuse
    mc_header["mc_injection_height"] = event.mc.injection_height
    return mc_header


def set_tp(save_info, cleaning_mask, mc):
    arten = ["Reinheit", "Effizienz", "Genauigkeit", "TP", "FP", "TN", "FN"]
    for i in arten:
        if i not in save_info:
            save_info[i] = []
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    for i in range(len(cleaning_mask)):
        if cleaning_mask[i] == False:
            if mc[i] == 0:
                tn += 1
            else:
                fn += 1
        else:
            if mc[i] == 0:
                fp += 1
            else:
                tp += 1
    save_info["Reinheit"].append(tp / (tp + fp))
    save_info["Effizienz"].append(tp / (tp + fn))
    save_info["Genauigkeit"].append((tp + tn) / (tp + tn + fp + fn))

    save_info["TP"].append(tp)
    save_info["FP"].append(fp)
    save_info["TN"].append(tn)
    save_info["FN"].append(fn)
    return save_info


def process_event(event, r1, dl0, dl1, infos_save, E_info, num_tel_active):
    Keylist = ["size", "cen_x", "cen_y", "length", "width", "r", "phi", "psi", "miss", "skewness", "kurtosis", "Reinheit", "Effizienz", "Genauigkeit"]
    calibration(event, r1, dl0, dl1)

    eins_drinne = {}
    num_tel_active_now = 0
    for tel_id in event.r0.tels_with_data:
        num_tel_active_now += 1
        err = False
        image = event.dl1.tel[tel_id].image[0]
        geom = event.inst.subarray.tel[tel_id].camera

        for i in infos_save.keys():
            for j in infos_save[i].keys():
                if i not in eins_drinne.keys():
                    eins_drinne[i] = {}
                if j not in eins_drinne[i].keys():
                    eins_drinne[i][j] = False
                if i == "o":
                    event_info = {}
                    event_info = set_mc(event_info, event, tel_id)
                    infos_save[i][j].append(event_info)
                    eins_drinne[i][j] = True
                    continue

                cleaning_mask = tailcuts_clean(geom, image, picture_thresh=i, boundary_thresh=j)
                if len(image[cleaning_mask]) == 0:
                    continue
                clean = image.copy()
                clean[~cleaning_mask] = 0.0
                event_info = {}
                hillas = hillas_parameters(geom, image=clean)
                event_info = set_hillas(event_info, hillas)
                event_info = set_mc(event_info, event, tel_id)
                event_info = set_tp(event_info, cleaning_mask, event.mc.tel[tel_id].photo_electron_image)
                for key in Keylist:
                    if key not in event_info:
                        err = True
                    else:
                        drinne = False
                        wert = event_info[key][0]
                        if wert >= 0:
                            drinne = True
                        elif wert < 0:
                            drinne = True
                        if drinne is False:
                            err = True
                if err:
                    continue
                infos_save[i][j].append(event_info)
                eins_drinne[i][j] = True

    for i in eins_drinne.keys():
        for j in eins_drinne[i].keys():
            if i not in E_info.keys():
                E_info[i] = {}
            if j not in E_info[i].keys():
                E_info[i][j] = []
            if eins_drinne[i][j]:
                E_info[i][j].append(event.mc.energy.value)
    num_tel_active.append(num_tel_active_now)
    return infos_save, E_info, num_tel_active


# Get Teleskope_id of the MST Teleskope
right_tel = []
try:
    right_tel = pickle.load(open("right_tel.pickle", "rb"))
except:
    right_tel = []

# Set Filename and number of File
Filename = "../Master_Daten/gammas/gamma_20deg_0deg_run35613___cta-prod2_desert-1640m-Aar.simtel.gz"
nummer = str(0)
if len(sys.argv) == 3:
    nummer = sys.argv[1]
    Filename = sys.argv[2]
print(len(right_tel))
if right_tel == [] or True:
    right_tel = set_right_tel(Filename)
    pickle.dump(right_tel, open("right_tel.pickle", "wb"))

print(len(right_tel))

# Set Calibrator
r1 = HessioR1Calibrator(None, None)
dl0 = CameraDL0Reducer(None, None)
#integrator = LocalPeakIntegrator(config=None, tool=None)
#integrator.window_shift = 5
#integrator.window_width = 30
dl1_calibrator = CameraDL1Calibrator(
    config=None,
    tool=None,
    extractor=None,
)

infos_save = {}
infos_save["o"] = {}
infos_save["o"]["o"] = []
infos_save[7] = {}
infos_save[7][2] = []
mc_header = {}
num_tel_active = []

os.system('mkdir -p preprocess_pickle')



num_events = get_num_events(Filename, right_tel)

try:
    source = hessio_event_source(Filename, allowed_tels=right_tel)
except:
    os.exit(1)


E_Event = []
E_info = {}
anzahl = 0
start_time = time.time()
for event in source:
    E_Event.append(event.mc.energy.value)
    if anzahl == 0:
        mc_header = set_mc_header(event, mc_header)
        print(mc_header)
    infos_save, E_info, num_tel_active = process_event(event, r1, dl0, dl1_calibrator, infos_save, E_info, num_tel_active)

    # print Process time
    anzahl += 1
    if anzahl % 50 == 0:
        zeit = (time.time() - start_time) * (num_events - anzahl) / anzahl
        stunde = int(zeit / (60 * 60))
        zeit = zeit - stunde * (60 * 60)
        minute = int(zeit / 60)
        zeit = int(zeit - minute * 60) + 1
        if zeit == 60:
            zeit = 0
            minute += 1
        if minute == 60:
            minute = 0
            stunde += 1
        print(string_len(nummer, 2) + "\t" + string_len(anzahl, 5) + "/" + string_len(num_events, 5) + "\t" + string_len(stunde, 2) + ":" + string_len(minute, 2) + ":" + string_len(zeit, 2))
pickle.dump(num_tel_active, open("preprocess_pickle/F" + str(nummer) + "_num_tel_active.pickle", "wb"))

print(sum(num_tel_active))
# save Data
for i in infos_save.keys():
    for j in infos_save[i].keys():
        data_container = {"info": infos_save[i][j], "E_events": E_Event, "E_cut": E_info[i][j], "mc_header": mc_header}
        pickle.dump(data_container, open("preprocess_pickle/F" + str(nummer) + "_PT" + str(i) + "_BT" + str(j) + "_ergebnisse.pickle", "wb"))
