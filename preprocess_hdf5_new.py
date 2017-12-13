from ctapipe.calib.camera.r1 import HessioR1Calibrator
from ctapipe.calib.camera.dl0 import CameraDL0Reducer
from ctapipe.calib.camera.dl1 import CameraDL1Calibrator
from ctapipe.image.charge_extractors import LocalPeakIntegrator
from ctapipe.image import tailcuts_clean, hillas_parameters
from ctapipe.io.hessio import hessio_event_source
from ctapipe.io.containers import DataContainer
from ctapipe.instrument import TelescopeDescription, SubarrayDescription
from astropy.coordinates import Angle
import pickle
import os
from multiprocessing import Pool
import sys
import pandas as pd
import numpy as np
import time
import h5py
from tqdm import tqdm
from astropy import units as u


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

def set_event_info(event, f_e, i):
    event.trig.gps_time = f_e["trigger_gps_time"][i]
    event.mc.energy = f_e["mc_E"][i] * u.TeV
    event.mc.alt = Angle(f_e["mc_altitude"][i], u.rad)
    event.mc.az = Angle(f_e["mc_azimuth"][i], u.rad)
    event.mc.core_x = f_e["mc_core_x"][i] * u.m
    event.mc.core_y = f_e["mc_core_y"][i] * u.m
    event.mc.shower_primary_id = f_e["mc_gamma_proton"][i]
    event.mc.h_first_int = f_e["mc_h_first_int"][i]
    return event

def set_tel_info(event, f_tel):
    for i in range(len(f_tel["tel_id"])):
        tel_id = f_tel["tel_id"][i]
        event.inst.mirror_dish_area[tel_id] = f_tel["mirror_dish_area"][i] * u.m**2
        event.inst.num_pixels[tel_id] = f_tel["num_pixels"][i]
        event.inst.num_channels[tel_id] = f_tel["num_channels"][i]
        event.inst.optical_foclen[tel_id] = f_tel["optical_foclen"][i] * u.m
        event.inst.mirror_numtiles[tel_id] = f_tel["mirror_numtiles"][i]
        event.inst.tel_pos[tel_id] = f_tel["tel_pos"][i]
        if f_tel["geom"][i] == 0:
            event.inst.pixel_pos[tel_id] = f_tel["pixel_pos1"][i] * u.m
        else:
            event.inst.pixel_pos[tel_id] = f_tel["pixel_pos2"][i] * u.m
        tel = TelescopeDescription.guess(*event.inst.pixel_pos[tel_id], event.inst.optical_foclen[tel_id])
        tel.optics.mirror_area = event.inst.mirror_dish_area[tel_id]
        tel.optics.num_mirror_tiles = event.inst.mirror_numtiles[tel_id]
        event.inst.subarray.tels[tel_id] = tel
        event.inst.subarray.positions[tel_id] = f_tel["tel_pos"][i] * u.m
    return event

def get_picture_multi(f_image, img_type, index, tel_id):
    if img_type == 1:
        return f_image["image1"][index], tel_id
    else:
        actuelle_time = time.time()
        return f_image["image2"][index], tel_id

def set_events_info(event, f_es, i, f_image):

    tel_id = f_es["tel_id"][i]
    event.mc.tel[tel_id].azimuth_raw = f_es["mc_azimuth_raw"][i]
    event.mc.tel[tel_id].altitude_raw = f_es["mc_altitude_raw"][i]
    event.mc.tel[tel_id].azimuth_cor = f_es["mc_azimuth_cor"][i]
    event.mc.tel[tel_id].altitude_cor = f_es["mc_altitude_cor"][i]
    event.mc.tel[tel_id].time_slice = f_es["mc_time_slice"][i]
    event.mc.tel[tel_id].meta['refstep'] = f_es["mc_refstep"][i]
    actuelle_time = time.time()
    event.dl1.tel[tel_id].image = []
    if f_es["img_type"][i] == 1:
        event.dl1.tel[tel_id].image = f_image["image1"][f_es["img_index"][i]]
        event.mc.tel[tel_id].photo_electron_image = f_image["photo_electron_image1"][f_es["img_index"][i]]
        event.mc.tel[tel_id].reference_pulse_shape = f_image["reference_pulse_shape1"][f_es["img_index"][i]]
    else:
        event.dl1.tel[tel_id].image = f_image["image2"][f_es["img_index"][i]]
        event.mc.tel[tel_id].photo_electron_image = f_image["photo_electron_image2"][f_es["img_index"][i]]
        event.mc.tel[tel_id].reference_pulse_shape = f_image["reference_pulse_shape2"][f_es["img_index"][i]]


    event.dl0.tels_with_data.append(tel_id)
    return event

def calibration(event, dl1):
    dl1.calibrate(event)


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
    arten = ["mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_time_slice", "mc_refstep", "tel_id", "mc_gamma_proton", "trigger_gps_time"]
    #           Tev,           rad,          rad,           m,           m,                m,                 ,                  ,                 ,                  ,                ,             ,         ,                  ]
    for i in arten:
        if i not in event_info:
            event_info[i] = []
    event_info["mc_E"].append(event.mc.energy.value)
    event_info["mc_altitude"].append(event.mc.alt.value)
    event_info["mc_azimuth"].append(event.mc.az.value)
    event_info["mc_core_x"].append(event.mc.core_x.value)
    event_info["mc_core_y"].append(event.mc.core_y.value)
    event_info["mc_h_first_int"].append(event.mc.h_first_int)
    event_info["mc_azimuth_raw"].append(event.mc.tel[tel_id].azimuth_raw)
    event_info["mc_altitude_raw"].append(event.mc.tel[tel_id].altitude_raw)
    event_info["mc_azimuth_cor"].append(event.mc.tel[tel_id].azimuth_cor)
    event_info["mc_altitude_cor"].append(event.mc.tel[tel_id].altitude_cor)
    event_info["mc_time_slice"].append(event.mc.tel[tel_id].time_slice)
    event_info["mc_refstep"].append(event.mc.tel[tel_id].meta['refstep'])
    # event_info["camera_rotation_angle"].append(event.inst.camera_rotation_angle[tel_id])
    event_info["tel_id"].append(tel_id)
    event_info["mc_gamma_proton"].append(event.mc.shower_primary_id)
    event_info["trigger_gps_time"].append(event.trig.gps_time)
    return event_info


def set_mc_header(mc_info):
    mc_header = {}
    for key in mc_info:
        mc_header[key] = mc_info[key][0]
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


def process_event(event, dl1, infos_save, E_info):
    Keylist = ["size", "cen_x", "cen_y", "length", "width", "r", "phi", "psi", "miss", "skewness", "kurtosis", "Reinheit", "Effizienz", "Genauigkeit"]
    #calibration(event, dl1)

    eins_drinne = {}

    err = False
    for tel_id in event.dl0.tels_with_data:
        image = event.dl1.tel[tel_id].image[0]
        geom = event.inst.subarray.tel[tel_id].camera
        for i in infos_save.keys():
            for j in infos_save[i].keys():
                if i == "o":
                    event_info = {}
                    event_info = set_mc(event_info, event, tel_id)
                    infos_save[i][j].append(event_info)
                    if i not in eins_drinne:
                        eins_drinne[i] = {}
                    if j not in eins_drinne[i]:
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
                if i not in eins_drinne:
                    eins_drinne[i] = {}
                if j not in eins_drinne[i]:
                    eins_drinne[i][j] = True

    for i in eins_drinne.keys():
        for j in eins_drinne[i].keys():
            E_info[i][j].append(event.mc.energy.value)

    return infos_save, E_info


# Set Filename and number of File
Filename = "../Master_Daten/PROD3/LaPalma/gamma/gamma_20deg_0deg_run2___cta-prod3-lapalma3-2147m-LaPalma_szip_0_dl1.hdf5"
nummer = str(0)
if len(sys.argv) == 3:
    nummer = sys.argv[1]
    Filename = sys.argv[2]

f = h5py.File(Filename, 'r+')
f_es = f.get('events')
f_e = f.get('event')
f_image = f.get('image')
os.system('mkdir -p preprocess_pickle')

# Set Calibrator
#integrator = LocalPeakIntegrator(config=None, tool=None)
#integrator.window_shift = 5
#integrator.window_width = 30
dl1_calibrator = CameraDL1Calibrator(
    config=None,
    tool=None,
    extractor=None,
)

infos_save = {}
E_info = {}


A = [1,2,3,4,5,6,7,8,9,10,11,12]
for i in A:
    infos_save[i] = {}
    E_info[i] = {}
    for j in A:
        infos_save[i][j] = []
        E_info[i][j] = []

infos_save["o"] = {}
infos_save["o"]["o"] = []
E_info["o"] = {}
E_info["o"]["o"] = []
mc_header = set_mc_header(f.get('mc_header'))
num_tel_active = []
E_Event = []


anzahl = 0
event_id = -1
tel_active_now = 0
event = DataContainer()
event.meta['origin'] = "hessio"
event.inst.subarray = SubarrayDescription("MonteCarloArray")
set_tel_info(event, f.get('tel_info'))
infos_multi = {"img_type": [], "img_index": [], "tel_id": []}
pbar = tqdm(total=len(f_es["tel_id"]))

for i in range(len(f_es["tel_id"])):
    if f_es["event_id"][i] != event_id or i == len(f_es["tel_id"]) - 1:
        if i == len(f_es["tel_id"]) - 1:
            event = set_events_info(event, f_es, i, f_image)
        if anzahl != 0:
            num_tel_active.append(tel_active_now)
            infos_save, E_info = process_event(event, dl1_calibrator, infos_save, E_info)
        if i != len(f_es["tel_id"]) - 1:
            event_id = f_es["event_id"][i]
            E_Event.append(f_e["mc_E"][event_id])
            tel_active_now = 1
            set_event_info(event, f_e, event_id)
            event.dl0.tels_with_data = []
    else:
        tel_active_now += 1
    event = set_events_info(event, f_es, i, f_image)

    # print Process time
    anzahl += 1
    if anzahl % 25 == 0:
        pbar.update(25)
pbar.update(anzahl % 25)
pbar.close()



pickle.dump(num_tel_active, open("preprocess_pickle/F" + str(nummer) + "_num_tel_active.pickle", "wb"))

# save Data
for i in infos_save.keys():
    for j in infos_save[i].keys():
        data_container = {"info": infos_save[i][j], "E_events": E_Event, "E_cut": E_info[i][j], "mc_header": mc_header}
        pickle.dump(data_container, open("preprocess_pickle/F" + str(nummer) + "_PT" + str(i) + "_BT" + str(j) + "_ergebnisse.pickle", "wb"))
