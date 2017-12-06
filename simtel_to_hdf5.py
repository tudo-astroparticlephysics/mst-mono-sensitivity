from ctapipe.io.hessio import hessio_event_source
from ctapipe.calib.camera.r1 import HessioR1Calibrator
from ctapipe.calib.camera.dl0 import CameraDL0Reducer

import pandas as pd
import sys
import os
import pickle
import numpy as np
import h5py
import time
from tqdm import tqdm

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
    nummer = "2"
    if len(sys.argv) == 3:
        nummer = sys.argv[1]
        Filename = sys.argv[2]
    return nummer, Filename


def set_right_tel(Filename):
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
            Name = "bla"
            if Name not in Camera_infos:
                Camera_infos[Name] = {"x":[], "y":[], "Anzahl":[]}
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
    alpha = 1
    for key in Camera_infos.keys():
        #plt.scatter(x, y, marker='o', c=rein_g)
        print(Camera_infos[key]["Anzahl"])
        plt.scatter(Camera_infos[key]["x"], Camera_infos[key]["y"], marker='o', c=Camera_infos[key]["Anzahl"])
        alpha -= 0.25
    plt.colorbar()
    plt.title("Map Teleskopes")
    plt.xlabel(r"x/m")
    plt.ylabel(r"y/m")
    plt.tight_layout()
    plt.legend(loc='best')
    plt.savefig('Bilder/Map.pdf')
    plt.clf()
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
    dataset = hdf5.create_dataset('mc_header', (1,),
                                  dtype=[('mc_spectral_index', '<f8'),
                                         ('mc_obsheight', '<f8'),
                                         ('mc_num_showers', '<f8'),
                                         ('mc_num_use', '<f8'),
                                         ('mc_core_pos_mode', '<f8'),
                                         ('mc_core_range_X', '<f8'),
                                         ('mc_core_range_Y', '<f8'),
                                         ('mc_alt_range_Min', '<f8'),
                                         ('mc_alt_range_Max', '<f8'),
                                         ('mc_az_range_Min', '<f8'),
                                         ('mc_az_range_Max', '<f8'),
                                         ('mc_viewcone_Min', '<f8'),
                                         ('mc_viewcone_Max', '<f8'),
                                         ('mc_E_range_Min', '<f8'),
                                         ('mc_E_range_Max', '<f8'),
                                         ('mc_diffuse', '<f8'),
                                         ('mc_injection_height', '<f8')])
    dataset["mc_spectral_index"] = event.mc.spectral_index
    dataset["mc_obsheight"] = event.mc.obsheight
    dataset["mc_num_showers"] = event.mc.num_showers
    dataset["mc_num_use"] = event.mc.num_use
    dataset["mc_core_pos_mode"] = event.mc.core_pos_mode
    dataset["mc_core_range_X"] = event.mc.core_range_X
    dataset["mc_core_range_Y"] = event.mc.core_range_Y
    dataset["mc_alt_range_Min"] = event.mc.alt_range_Min
    dataset["mc_alt_range_Max"] = event.mc.alt_range_Max
    dataset["mc_az_range_Min"] = event.mc.az_range_Min
    dataset["mc_az_range_Max"] = event.mc.az_range_Max
    dataset["mc_viewcone_Min"] = event.mc.viewcone_Min
    dataset["mc_viewcone_Max"] = event.mc.viewcone_Max
    dataset["mc_E_range_Min"] = event.mc.E_range_Min
    dataset["mc_E_range_Max"] = event.mc.E_range_Max
    dataset["mc_diffuse"] = event.mc.mc_diffuse
    dataset["mc_injection_height"] = event.mc.injection_height


def set_mc(event, tel_id, events_info):
    #arten = ["mc_E", "mc_altitude", "mc_azimuth", "mc_core_x", "mc_core_y", "mc_h_first_int", "mc_azimuth_raw", "mc_altitude_raw", "mc_azimuth_cor", "mc_altitude_cor", "mc_time_slice", "mc_refstep", "tel_id", "mc_gamma_proton"]
    #            Tev,           rad,          rad,           m,           m,                m,                 ,                  ,                 ,                  ,                ,             ,         ,                  ]
    events_info["mc_E"].append(event.mc.energy.value)
    events_info["mc_altitude"].append(event.mc.alt.value)
    events_info["mc_azimuth"].append(event.mc.az.value)
    events_info["mc_core_x"].append(event.mc.core_x.value)
    events_info["mc_core_y"].append(event.mc.core_y.value)
    events_info["mc_h_first_int"].append(event.mc.h_first_int.value)
    events_info["mc_gamma_proton"].append(event.mc.shower_primary_id)

    events_info["mc_azimuth_raw"].append(event.mc.tel[tel_id].azimuth_raw)
    events_info["mc_altitude_raw"].append(event.mc.tel[tel_id].altitude_raw)
    events_info["mc_azimuth_cor"].append(event.mc.tel[tel_id].azimuth_cor)
    events_info["mc_altitude_cor"].append(event.mc.tel[tel_id].altitude_cor)
    events_info["mc_time_slice"].append(event.mc.tel[tel_id].time_slice)
    events_info["mc_refstep"].append(event.mc.tel[tel_id].meta['refstep'])
    # events_info["camera_rotation_angle"].append(event.inst.camera_rotation_angle[tel_id])
    events_info["tel_id"].append(tel_id)
    return events_info


def set_tel_info(event, right_tel, hdf5):
    '''
    dataset = hdf5.create_dataset('tel_info', (len(right_tel),),
                                  dtype=[('subtype', 'i4'),
                                         ('type', 'i4'),
                                         ('mirror_type', 'i4'),
                                         ('geom', 'i4'),
                                         ('tel_id', 'i4')])
    '''
    Array = {"subtype": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "type": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "mirror_type": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "geom": np.zeros(len(right_tel), dtype=np.dtype('uint8')), "tel_id": np.zeros(len(right_tel), dtype=np.dtype('uint8'))}
    Image = {"image1": np.ones((len(right_tel), 1855, 1855), dtype=bool), "image2": np.ones((len(right_tel), 1764, 1764), dtype=bool)}
    for i in range(len(right_tel)):
        tel_id = right_tel[i]
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
        else:
            Array["geom"][i] = 1
            Image["image2"][i] = event.inst.subarray.tel[tel_id].camera.neighbor_matrix
        Array["tel_id"][i] = tel_id
    gr = hdf5.create_group("tel_info")
    for key in Array.keys():
        gr.create_dataset(key, data=Array[key])

    for key in Image.keys():
        gr.create_dataset(key, data=Image[key], compression="gzip", compression_opts=9)


def transfer_Data_to_hdf5(Filename, right_tel, num_events, image1, image2, compression, compression_opts, new_data_name=None):
    try:
        source = hessio_event_source(Filename, allowed_tels=right_tel)
    except:
        os.exit(1)

    if new_data_name == None:
        new_data_name = Filename.replace(".simtel.gz", "_" + compression + "_" + str(compression_opts) + ".hdf5")
    if_first = True
    # np.concatenate np.vstack np.hstack np.append
    image1_index = 0
    image2_index = 0
    chunk_size = 1000
    image_infos = {"image1": np.zeros((image1, 1, 1764, 25), dtype=np.dtype("uint16")), "image2": np.zeros((image2, 2, 1855, 64), dtype=np.dtype("uint16"))}
    events_info = {"mc_E": [], "mc_altitude": [], "mc_azimuth": [], "mc_core_x": [], "mc_core_y": [], "mc_h_first_int": [], "mc_azimuth_raw": [], "mc_altitude_raw": [], "mc_azimuth_cor": [], "mc_altitude_cor": [], "mc_time_slice": [], "mc_refstep": [], "tel_id": [], "mc_gamma_proton": [], "img_type": [], "img_index": []}
    new_image = 0
    hdf5 = h5py.File(new_data_name, 'w')
    shape_array = []
    anzahl = 0
    start_time = time.time()
    # for event in tqdm(source):
    pbar = tqdm(total=num_events)

    for event in source:
        if if_first:
            set_mc_header(event, hdf5)
            set_tel_info(event, right_tel, hdf5)
            if_first = False
        for tel_id in event.r0.tels_with_data:
            events_info = set_mc(event, tel_id, events_info)
            new_image = [event.r0.tel[tel_id].adc_samples]
            new_image = np.array(new_image, dtype=np.dtype("uint16"))

            if new_image.shape[1] == 1:
                events_info["img_type"].append(1)
                events_info["img_index"].append(image1_index)
                image_infos["image1"][image1_index] = new_image
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
        if key == "tel_id" or key == "mc_gamma_proton":
            gr.create_dataset(key, data=np.array(events_info[key], dtype=np.dtype('uint8')), compression="gzip", compression_opts=9)
        else:
            gr.create_dataset(key, data=np.array(events_info[key], dtype=np.dtype('float64')), compression="gzip", compression_opts=9)

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
    hdf5.close()
    return new_data_name


def main(argv):
    Nummer, Filename = get_num_filename(argv)
    right_tel = set_right_tel(Filename)
    #num_events, image1, image2 = get_num_events(Filename, right_tel)
    num_events = 2185
    image1 = 12494
    image2 = 13517
    '''
    comp = {"lzf": [0], "szip": [0], "gzip": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}
    comp = {"no": [0]}
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
    '''
if __name__ == "__main__":
    main(sys.argv)
