from ctapipe.calib.camera.r1 import HessioR1Calibrator
from ctapipe.calib.camera.dl0 import CameraDL0Reducer
from ctapipe.calib.camera import CameraDL1Calibrator
from ctapipe.image.charge_extractors import LocalPeakIntegrator
from ctapipe.image import tailcuts_clean
from ctapipe.image import hillas_parameters_4 as hillas_parameters
from ctapipe.io.hessio import hessio_event_source
import pickle
import os



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


def process_event(event, r1, dl0, dl1):
    event_info = {}
    calibration(event, r1, dl0, dl1)
    for tel_id in event.r0.tels_with_data:
        image = event.dl1.tel[tel_id].image[0]
        geom = event.inst.subarray.tel[tel_id].camera
        cleaning_mask = tailcuts_clean(geom, image, picture_thresh=10, boundary_thresh=7)
        if len(image[cleaning_mask]) == 0:
            continue
        clean = image.copy()
        clean[~cleaning_mask] = 0.0
        hillas = hillas_parameters(geom, image=clean)
        event_info = set_hillas(event_info, hillas)
        event_info = set_mc(event_info, event, tel_id)
    return event_info


right_tel = []
try:
    right_tel = pickle.load(open("right_tel.pickle", "rb"))
except:
    right_tel = []

Filename = "../Master_Daten/gammas/gamma_20deg_0deg_run35613___cta-prod2_desert-1640m-Aar.simtel.gz"

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


save_info = []
try:
    source = hessio_event_source(Filename, allowed_tels=right_tel)
except:
    os.exit(1)
for event in source:
    save_info.append(process_event(event, r1, dl0, dl1_calibrator))

pickle.dump(save_info, open(str(i) + "_ergebnisse.pickle", "wb"))