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
    arten = ["size", "cen_x", "cen_y", "lenght", "width", "r", "phi", "psi", "miss", "skewness", "kurtosis"]
    for i in range(len(hillas)):
        art = arten[i]
        if art not in event_info:
            event_info[art] = []
        event_info[art].append(hillas[i])
    return event_info


def set_mc(event_info, event, tel_id):
    arten = ["mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_altitude_raw", "mc_time_slice", "mc_refstep", "camera_rotation_angle", "tel_id", "mc_gamma_proton"]
    for i in arten:
        if i not in event_info:
            event_info[i] = []
    event_info["mc_E"].append(event.mc.energy)
    event_info["mc_altitude"].append(event.mc.alt)
    event_info["mc_azimuth"].append(event.mc.az)
    event_info["mc_core_x"].append(event.mc.core_x)
    event_info["mc_core_y"].append(event.mc.core_y)
    event_info["mc_h_first_int"].append(event.mc.h_first_int)
    event_info["mc_azimuth_raw"].append(event.mc.tel[tel_id].azimuth_raw)
    event_info["mc_altitude_raw"].append(event.mc.tel[tel_id].altitude_raw)
    event_info["mc_azimuth_cor"].append(event.mc.tel[tel_id].azimuth_cor)
    event_info["mc_altitude_cor"].append(event.mc.tel[tel_id].altitude_cor)
    event_info["mc_time_slice"].append(event.mc.tel[tel_id].time_slice)
    event_info["mc_refstep"].append(event.mc.tel[tel_id].meta['refstep'])
    event_info["camera_rotation_angle"].append(event.inst.camera_rotation_angle[tel_id])
    event_info["mc_gamma_proton"].append(event.mc.shower_primary_id)
    event_info["tel_id"].append(tel_id)
    return event_info


def add_tp(save_info, cleaning_mask, mc):
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


def process_event(event, r1, dl0, dl1, thresh, boundary):
    event_info = {}

    calibration(event, r1, dl0, dl1)
    for tel_id in event.r0.tels_with_data:
        image = event.dl1.tel[tel_id].image[0]
        geom = event.inst.subarray.tel[tel_id].camera
        cleaning_mask = tailcuts_clean(geom, image, picture_thresh=thresh, boundary_thresh=boundary)
        if len(image[cleaning_mask]) == 0:
            continue
        clean = image.copy()
        clean[~cleaning_mask] = 0.0
        hillas = hillas_parameters(geom, image=clean)
        event_info = set_hillas(event_info, hillas)
        event_info = set_mc(event_info, event, tel_id)
        event_info = add_tp(event_info, cleaning_mask, event.mc.tel[tel_id].photo_electron_image)
        break
    return [event_info]


right_tel = []
try:
    right_tel = pickle.load(open("right_tel.pickle", "rb"))
except:
    right_tel = []

Filename = "../Master_Daten/gammas/gamma_20deg_0deg_run35613___cta-prod2_desert-1640m-Aar.simtel.gz"
nummer = str(0)
if len(sys.argv) == 3:
    nummer = sys.argv[1]
    Filename = sys.argv[2]
if right_tel == []:
    right_tel = set_right_tel(Filename)
    pickle.dump(right_tel, open("right_tel.pickle", "wb"))

# Set Calibrator
r1 = HessioR1Calibrator(None, None)
dl0 = CameraDL0Reducer(None, None)
integrator = LocalPeakIntegrator(config=None, tool=None)
integrator.window_shift = 5
integrator.window_width = 30
dl1_calibrator = CameraDL1Calibrator(
    config=None,
    tool=None,
    extractor=None,
)

Liste = [0, 1, 2, 3, 4]
Liste2 = [5,6,7,8,9,10,11,12,13,14,15,16,17]

os.system('mkdir -p results')
for i in Liste2:
    for j in Liste:
        if j > i:
            continue
        save_info = []
        try:
            source = hessio_event_source(Filename, allowed_tels=right_tel)
        except:
            os.exit(1)

        for event in source:
            event_info = process_event(event, r1, dl0, dl1_calibrator, i, j)
            save_info.append(event_info[0])

        pickle.dump(save_info, open("results/F" + str(nummer) + "_PT" + str(i) + "_BT" + str(j) + "_ergebnisse.pickle", "wb"))
