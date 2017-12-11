from ctapipe.io.hessio import hessio_event_source
from ctapipe.calib.camera.r1 import HessioR1Calibrator
from ctapipe.calib.camera.dl0 import CameraDL0Reducer
from ctapipe.calib.camera import CameraDL1Calibrator

import pandas as pd
import sys
import os
import pickle
import numpy as np
import h5py
import time
from tqdm import tqdm
from astropy import units as u

from matplotlib import use
use('Agg')
import matplotlib.pyplot as plt


def string_len(string, length):
    string = str(string)
    ersetzung = " "
    if length == 2:
        ersetzung = "0"
    if len(string) < length:
        string = ersetzung + string
    return string


def get_num_filename(argv):
    Filename = "../Master_Daten/PROD3/LaPalma/gamma/gamma_20deg_0deg_run2___cta-prod3-lapalma3-2147m-LaPalma.simtel.gz"
    nummer = "6"
    if len(sys.argv) == 3:
        nummer = sys.argv[1]
        Filename = sys.argv[2]
    return nummer, Filename


def set_right_tel(Filename, File_extension):
    right_tel = []
    try:
        g
        return pickle.load(open("right_tel.pickle", "rb"))
    except:
        right_tel = []
    source = hessio_event_source(Filename, max_events=1)
    Anzahl = 0
    anzahl_cut = 0
    Camera_infos = {}
    for event in source:
        for tel_id in event.inst.telescope_ids:
            Name = event.inst.subarray.tels[tel_id].optics.tel_type + "_" + event.inst.subarray.tels[tel_id].optics.mirror_type + "_" + str(event.inst.subarray.tel[tel_id].camera)
            # Name = "bla"
            if Name not in Camera_infos:
                Camera_infos[Name] = {"x": [], "y": [], "Anzahl": []}
            x_pos = event.inst.subarray.positions[tel_id][0].value
            y_pos = event.inst.subarray.positions[tel_id][1].value
            drinne = False
            for i in range(len(Camera_infos[Name]["x"])):
                if x_pos == Camera_infos[Name]["x"][i] and y_pos == Camera_infos[Name]["y"][i]:
                    Camera_infos[Name]["Anzahl"][i] += 1
                    drinne = True
                    break
            if drinne is False:
                Camera_infos[Name]["x"].append(event.inst.subarray.positions[tel_id][0].value)
                Camera_infos[Name]["y"].append(event.inst.subarray.positions[tel_id][1].value)
                Camera_infos[Name]["Anzahl"].append(1)
            if event.inst.subarray.tels[tel_id].optics.tel_type == "MST" and event.inst.subarray.tels[tel_id].optics.mirror_type == "DC":
                right_tel.append(tel_id)

        '''
        for tel_id in event.r0.tels_with_data:
            Anzahl += 1
            if event.inst.subarray.tels[tel_id].optics.tel_type == "MST":# and event.inst.subarray.tels[tel_id].optics.mirror_type == "DC":
                if tel_id not in right_tel:
                    right_tel.append(tel_id)
                anzahl_cut += 1
        '''
    '''
    alpha = 1
    for key in Camera_infos.keys():
        print(key)
        # plt.scatter(x, y, marker='o', c=rein_g)
        print(Camera_infos[key]["Anzahl"])
        plt.scatter(Camera_infos[key]["x"], Camera_infos[key]["y"], marker='o', c=Camera_infos[key]["Anzahl"])
        alpha -= 0.25
    plt.colorbar()
    plt.title(Filename.split("/")[len(Filename.split("/")) - 1])
    plt.xlabel(r"x/m")
    plt.ylabel(r"y/m")
    plt.tight_layout()
    plt.legend(loc='best')
    plt.savefig('Bilder/Map' + File_extension + '.pdf')
    plt.clf()
    '''
    print(len(right_tel))
    print(Anzahl)
    print(anzahl_cut)
    pickle.dump(right_tel, open("right_tel.pickle", "wb"))
    return right_tel


def get_num_events(Filename, right_tel):
    try:
        source = hessio_event_source(Filename, allowed_tels=right_tel)
    except:
        os.exit(1)

    num_events = 0
    anzahl_gesamt = 0
    image1 = 0
    image2 = 0
    for event in source:
        num_events += 1
        for tel_id in event.r0.tels_with_data:
            anzahl_gesamt += 1
            camera_art = str(event.inst.subarray.tel[tel_id].camera)
            if camera_art == "FlashCam":
                image1 += 1
            else:
                image2 += 1
        if num_events == 25:
            break
    print(num_events)
    print(anzahl_gesamt)
    print(image1)
    print(image2)
    return num_events, image1, image2


def set_mc_header(event, hdf5):
    gr_mc_header = hdf5.create_group("mc_header")
    gr_mc_header.create_dataset("mc_spectral_index", data=np.array([event.mc.spectral_index]))
    gr_mc_header.create_dataset("mc_obsheight", data=np.array([event.mc.obsheight]))
    gr_mc_header.create_dataset("mc_num_showers", data=np.array([event.mc.num_showers]))
    gr_mc_header.create_dataset("mc_num_use", data=np.array([event.mc.num_use]))
    gr_mc_header.create_dataset("mc_core_pos_mode", data=np.array([event.mc.core_pos_mode]))
    gr_mc_header.create_dataset("mc_core_range_X", data=np.array([event.mc.core_range_X]))
    gr_mc_header.create_dataset("mc_core_range_Y", data=np.array([event.mc.core_range_Y]))
    gr_mc_header.create_dataset("mc_alt_range_Min", data=np.array([event.mc.alt_range_Min]))
    gr_mc_header.create_dataset("mc_alt_range_Max", data=np.array([event.mc.alt_range_Max]))
    gr_mc_header.create_dataset("mc_az_range_Min", data=np.array([event.mc.az_range_Min]))
    gr_mc_header.create_dataset("mc_az_range_Max", data=np.array([event.mc.az_range_Max]))
    gr_mc_header.create_dataset("mc_viewcone_Min", data=np.array([event.mc.viewcone_Min]))
    gr_mc_header.create_dataset("mc_viewcone_Max", data=np.array([event.mc.viewcone_Max]))
    gr_mc_header.create_dataset("mc_E_range_Min", data=np.array([event.mc.E_range_Min]))
    gr_mc_header.create_dataset("mc_E_range_Max", data=np.array([event.mc.E_range_Max]))
    gr_mc_header.create_dataset("mc_diffuse", data=np.array([event.mc.mc_diffuse]))
    gr_mc_header.create_dataset("mc_injection_height", data=np.array([event.mc.injection_height]))
    gr_mc_header.create_dataset("B_total", data=np.array([event.mc.B_total]))
    gr_mc_header.create_dataset("B_inclination", data=np.array([event.mc.B_inclination]))
    gr_mc_header.create_dataset("B_declination", data=np.array([event.mc.B_declination]))
    gr_mc_header.create_dataset("atmosphere", data=np.array([event.mc.atmosphere]))
    gr_mc_header.create_dataset("corsika_iact_options", data=np.array([event.mc.corsika_iact_options]))
    gr_mc_header.create_dataset("corsika_low_E_model", data=np.array([event.mc.corsika_low_E_model]))
    gr_mc_header.create_dataset("corsika_high_E_model", data=np.array([event.mc.corsika_high_E_model]))
    gr_mc_header.create_dataset("corsika_bunchsize", data=np.array([event.mc.corsika_bunchsize]))
    gr_mc_header.create_dataset("corsika_wlen_min", data=np.array([event.mc.corsika_wlen_min]))
    gr_mc_header.create_dataset("corsika_wlen_max", data=np.array([event.mc.corsika_wlen_max]))
    gr_mc_header.create_dataset("corsika_low_E_detail", data=np.array([event.mc.corsika_low_E_detail]))
    gr_mc_header.create_dataset("corsika_high_E_detail", data=np.array([event.mc.corsika_high_E_detail]))

def set_tel_info(event, right_tel, hdf5):
    '''
    dataset = hdf5.create_dataset('tel_info', (len(right_tel),),
                                  dtype=[('subtype', 'i4'),
                                         ('type', 'i4'),
                                         ('mirror_type', 'i4'),
                                         ('geom', 'i4'),
                                         ('tel_id', 'i4')])
    '''
    Array = {"subtype": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "type": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "mirror_type": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "geom": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "tel_id": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "mirror_dish_area": np.zeros(len(right_tel), dtype=np.dtype('float64')), "num_pixels": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "num_channels": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "optical_foclen": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "mirror_numtiles": np.zeros(len(right_tel), dtype=np.dtype('uint8'))}
    Image = {"image1": np.ones((len(right_tel), 1855, 1855), dtype=bool), "image2": np.ones((len(right_tel), 1764, 1764), dtype=bool), "pixel_pos1": np.ones((len(right_tel), 2, 1855), dtype=np.dtype('float64')), "pixel_pos2": np.ones((len(right_tel), 2, 1764), dtype=np.dtype('float64')), "tel_pos": np.ones((len(right_tel), 3), dtype=np.dtype('float64'))}

    #subarray
    for i in range(len(right_tel)):
        tel_id = right_tel[i]
        Array["mirror_dish_area"][i] = event.inst.mirror_dish_area[tel_id].value
        #Array["camera_rotation_angle"][i] = event.inst.camera_rotation_angle[tel_id]
        Array["num_pixels"][i] = event.inst.num_pixels[tel_id]
        Array["num_channels"][i] = event.inst.num_channels[tel_id]
        Array["optical_foclen"][i] = event.inst.optical_foclen[tel_id].value
        Array["mirror_numtiles"][i] = event.inst.mirror_numtiles[tel_id]
        Image["tel_pos"][i] = event.inst.tel_pos[tel_id].value
        if event.inst.subarray.tels[tel_id].optics.tel_subtype == '':
            Array["subtype"][i] = 0
        else:
            Array["subtype"][i] = 1

        if event.inst.subarray.tels[tel_id].optics.tel_type == 'MST':
            Array["type"][i] = 0
        elif event.inst.subarray.tels[tel_id].optics.tel_type == 'LST':
            Array["type"][i] = 1
        else:
            Array["type"][i] = 2

        if event.inst.subarray.tels[tel_id].optics.mirror_type == 'DC':
            Array["mirror_type"][i] = 0
        else:
            Array["mirror_type"][i] = 1
        if str(event.inst.subarray.tel[tel_id].camera) == 'NectarCam':
            Array["geom"][i] = 0
            Image["image1"][i] = event.inst.subarray.tel[tel_id].camera.neighbor_matrix
            Image["pixel_pos1"][i] = event.inst.pixel_pos[tel_id].value
        else:
            Array["geom"][i] = 1
            Image["image2"][i] = event.inst.subarray.tel[tel_id].camera.neighbor_matrix
            Image["pixel_pos2"][i] = event.inst.pixel_pos[tel_id].value
        Array["tel_id"][i] = tel_id
    gr = hdf5.create_group("tel_info")
    for key in Array.keys():
        gr.create_dataset(key, data=Array[key])

    for key in Image.keys():
        gr.create_dataset(key, data=Image[key], compression="gzip", compression_opts=9)

def set_event_info(event, event_info):
    event_info["mc_E"].append(event.mc.energy.value)
    event_info["mc_altitude"].append(event.mc.alt.value)
    event_info["mc_azimuth"].append(event.mc.az.value)
    event_info["mc_core_x"].append(event.mc.core_x.value)
    event_info["mc_core_y"].append(event.mc.core_y.value)
    event_info["mc_h_first_int"].append(event.mc.h_first_int.value)
    event_info["mc_gamma_proton"].append(event.mc.shower_primary_id)
    event_info["trigger_gps_time"].append(event.trig.gps_time.value)
    return event_info

def set_mc(event, tel_id, event_id, events_info):
    # arten = ["mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_time_slice", "mc_refstep", "tel_id", "mc_gamma_proton"]
    #            Tev,           rad,          rad,           m,           m,                m,                 ,                  ,                 ,                  ,                ,             ,         ,                  ]
    events_info["mc_azimuth_raw"].append(event.mc.tel[tel_id].azimuth_raw)
    events_info["mc_altitude_raw"].append(event.mc.tel[tel_id].altitude_raw)
    events_info["mc_azimuth_cor"].append(event.mc.tel[tel_id].azimuth_cor)
    events_info["mc_altitude_cor"].append(event.mc.tel[tel_id].altitude_cor)
    events_info["mc_time_slice"].append(event.mc.tel[tel_id].time_slice)
    events_info["mc_refstep"].append(event.mc.tel[tel_id].meta['refstep'])
    events_info["event_id"].append(event_id)
    # events_info["camera_rotation_angle"].append(event.inst.camera_rotation_angle[tel_id])
    events_info["tel_id"].append(tel_id)
    return events_info



def transfer_Data_to_hdf5(Filename, right_tel, num_events, image1, image2, compression, compression_opts, new_data_name=None):
    try:
        source = hessio_event_source(Filename, allowed_tels=right_tel)
    except:
        os.exit(1)

    r1 = HessioR1Calibrator(None, None)
    dl0 = CameraDL0Reducer(None, None)
    dl1_calibrator = CameraDL1Calibrator(
        config=None,
        tool=None,
        extractor=None,
    )


    if new_data_name == None:
        new_data_name = Filename.replace(".simtel.gz", "_" + compression + "_" + str(compression_opts) + "_dl1.hdf5")
    if_first = True
    # np.concatenate np.vstack np.hstack np.append
    image1_index = 0
    image2_index = 0
    chunk_size = 1000
    image_infos = {"image1": np.zeros((image1,1, 1764), dtype=np.dtype("uint16")), "image2": np.zeros((image2, 2,1855), dtype=np.dtype("uint16")), "reference_pulse_shape1": np.zeros((image1, 1, 480), dtype=np.dtype("float64")), "reference_pulse_shape2": np.zeros((image2, 2, 250), dtype=np.dtype("float64")), "photo_electron_image1": np.zeros((image1,1764), dtype=np.dtype("uint16")), "photo_electron_image2": np.zeros((image2,1855), dtype=np.dtype("uint16"))}



    events_info = {"mc_azimuth_raw": [], "mc_altitude_raw": [], "mc_azimuth_cor": [], "mc_altitude_cor": [], "mc_time_slice": [], "mc_refstep": [], "tel_id": [], "img_type": [], "img_index": [], "event_id": []}
    event_info = {"mc_E": [], "mc_altitude": [], "mc_azimuth": [], "mc_core_x": [], "mc_core_y": [], "mc_h_first_int": [], "mc_gamma_proton": [], "trigger_gps_time":[]}
    new_image = 0
    hdf5 = h5py.File(new_data_name, 'w')
    shape_array = []
    anzahl = 0
    start_time = time.time()
    # for event in tqdm(source):
    pbar = tqdm(total=num_events)

    for event in source:
        r1.calibrate(event)
        dl0.reduce(event)
        dl1_calibrator.calibrate(event)
        if if_first:
            set_mc_header(event, hdf5)
            set_tel_info(event, right_tel, hdf5)
            if_first = False
        event_info = set_event_info(event, event_info)
        for tel_id in event.r0.tels_with_data:
            events_info = set_mc(event, tel_id, len(event_info["mc_E"]) - 1, events_info)
            new_image = [event.dl1.tel[tel_id].image]
            new_image = np.array(new_image, dtype=np.dtype("uint16"))
            if new_image.shape[1] == 1:
                events_info["img_type"].append(1)
                events_info["img_index"].append(image1_index)
                image_infos["image1"][image1_index] = new_image
                image_infos["reference_pulse_shape1"][image1_index] = np.array([event.mc.tel[tel_id].reference_pulse_shape], dtype=np.dtype("float64"))
                image_infos["photo_electron_image1"][image1_index] = np.array([event.mc.tel[tel_id].photo_electron_image], dtype=np.dtype("float64"))

                image1_index += 1

                '''

                MST
                DC
                FlashCam
                '''
            else:
                events_info["img_type"].append(2)
                events_info["img_index"].append(image2_index)
                image_infos["image2"][image2_index] = new_image
                image_infos["reference_pulse_shape2"][image2_index] = np.array([event.mc.tel[tel_id].reference_pulse_shape], dtype=np.dtype("float64"))
                image_infos["photo_electron_image2"][image2_index] = np.array([event.mc.tel[tel_id].photo_electron_image], dtype=np.dtype("float64"))

                image2_index += 1

                '''

                MST
                DC
                NectarCam
                '''
        anzahl += 1
        if anzahl % 25 == 0:
            pbar.update(25)

    pbar.update(anzahl % 25)
    pbar.close()

    gr = hdf5.create_group("events")
    for key in events_info.keys():
        if key == "tel_id" or key == "img_type" or key == "img_index" or key == "event_id":
            gr.create_dataset(key, data=np.array(events_info[key], dtype=np.dtype('uint8')), compression="gzip", compression_opts=9)
        else:
            gr.create_dataset(key, data=np.array(events_info[key], dtype=np.dtype('float64')), compression="gzip", compression_opts=9)

    gr_event = hdf5.create_group("event")
    for key in event_info.keys():
        if key == "mc_gamma_proton":
            gr_event.create_dataset(key, data=np.array(event_info[key], dtype=np.dtype('uint8')), compression="gzip", compression_opts=9)
        else:
            gr_event.create_dataset(key, data=np.array(event_info[key], dtype=np.dtype('float64')), compression="gzip", compression_opts=9)

    gr_image = hdf5.create_group("image")
    actuelle_time = time.time()
    if compression == "gzip":
        gr_image.create_dataset('image1', data=image_infos["image1"], compression="gzip", compression_opts=compression_opts)
    elif compression == "no":
        gr_image.create_dataset('image1', data=image_infos["image1"])
    else:
        gr_image.create_dataset('image1', data=image_infos["image1"], compression=compression)
    print("image1\t" + compression + "_" + str(compression_opts) + "\t" + str(time.time() - actuelle_time))
    actuelle_time = time.time()
    if compression == "gzip":
        gr_image.create_dataset('image2', data=image_infos["image2"], compression="gzip", compression_opts=compression_opts)
    elif compression == "no":
        gr_image.create_dataset('image2', data=image_infos["image2"])
    else:
        gr_image.create_dataset('image2', data=image_infos["image2"], compression=compression)
    print("image2\t" + compression + "_" + str(compression_opts) + "\t" + str(time.time() - actuelle_time))
    actuelle_time = time.time()
    gr_image.create_dataset('reference_pulse_shape1', data=image_infos["reference_pulse_shape1"], compression=compression)
    gr_image.create_dataset('reference_pulse_shape2', data=image_infos["reference_pulse_shape2"], compression=compression)
    gr_image.create_dataset('photo_electron_image1', data=image_infos["photo_electron_image1"], compression=compression)
    gr_image.create_dataset('photo_electron_image2', data=image_infos["photo_electron_image2"], compression=compression)
    print("ref\t" + compression + "_" + str(compression_opts) + "\t" + str(time.time() - actuelle_time))

    hdf5.close()
    return new_data_name


def main(argv):
    Nummer, Filename = get_num_filename(argv)
    # Filename = "../Master_Daten/PROD3/LaPalma/gamma_20deg_0deg_run1___cta-prod3-lapalma3-2147m-LaPalma.simtel.gz"
    right_tel = set_right_tel(Filename, Nummer + "_")
    # num_events, image1, image2 = get_num_events(Filename, right_tel)
    num_events = 2185
    image1 = 12494
    image2 = 13517

    comp = {"szip": [0], "lzf": [0], "gzip": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], "no": [0]}

    for compression in comp:
        for compression_opts in comp[compression]:
            print()
            new_data_name = transfer_Data_to_hdf5(Filename, right_tel, num_events, image1, image2, compression, compression_opts)
            actuelle_time = time.time()
            with h5py.File(new_data_name, 'r+') as f:
                group = f.get('image')
                for i in group.keys():
                    info = group[i][20]
            print("read\t" + compression + "_" + str(compression_opts) + "\t" + str(time.time() - actuelle_time))


if __name__ == "__main__":
    main(sys.argv)
