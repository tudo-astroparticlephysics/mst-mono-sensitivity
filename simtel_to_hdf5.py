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


def string_len(string, length):
    string = str(string)
    ersetzung = " "
    if length == 2:
        ersetzung = "0"
    if len(string) < length:
        string = ersetzung + string
    return string


def get_num_filename(argv):
    Filename = "../Master_Daten/gammas/gamma_20deg_0deg_run35613___cta-prod2_desert-1640m-Aar.simtel.gz"
    nummer = str(0)
    if len(sys.argv) == 3:
        nummer = sys.argv[1]
        Filename = sys.argv[2]
    return nummer, Filename


def set_right_tel(Filename):
    right_tel = []
    try:
        return pickle.load(open("right_tel.pickle", "rb"))
    except:
        right_tel = []
    source = hessio_event_source(Filename)
    Anzahl = 0
    anzahl_cut = 0
    for event in source:
        for tel_id in event.r0.tels_with_data:
            Anzahl += 1
            if event.inst.subarray.tels[tel_id].optics.tel_type == "MST":
                if tel_id not in right_tel:
                    right_tel.append(tel_id)
                anzahl_cut += 1
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
    image_1 = 0
    image_2 = 0
    image_3 = 0
    for event in source:
        num_events += 1
        for tel_id in right_tel:
            camera_art = str(event.inst.subarray.tel[tel_id].camera)
            if camera_art == "FlashCam":
                image_1 += 1
            elif camera_art == "SCTCam":
                image_2 += 1
            else:
                image_3 += 1
    return num_events, image_1, image_2, image_3


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
    dataset = hdf5.create_dataset('tel_info', (len(right_tel),),
                                  dtype=[('subtype', 'i4'),
                                         ('type', 'i4'),
                                         ('mirror_type', 'i4'),
                                         ('geom', 'i4'),
                                         ('tel_id', 'i4')])
    Array = {"subtype": [], "type": [], "mirror_type": [], "geom": [], "tel_id": []}
    for tel_id in right_tel:
        if event.inst.subarray.tels[tel_id].optics.tel_subtype == '':
            Array["subtype"].append(0)
        else:
            Array["subtype"].append(1)

        if event.inst.subarray.tels[tel_id].optics.tel_type == 'MST':
            Array["type"].append(0)
        elif event.inst.subarray.tels[tel_id].optics.tel_type == 'LST':
            Array["type"].append(1)
        else:
            Array["type"].append(2)

        if event.inst.subarray.tels[tel_id].optics.mirror_type == 'DC':
            Array["mirror_type"].append(0)
        else:
            Array["mirror_type"].append(1)

        if str(event.inst.subarray.tel[tel_id].camera) == 'NectarCam':
            Array["geom"].append(0)
        else:
            Array["geom"].append(1)
        Array["tel_id"].append(tel_id)
    dataset["subtype"] = Array["subtype"]
    dataset["type"] = Array["type"]
    dataset["mirror_type"] = Array["mirror_type"]
    dataset["geom"] = Array["geom"]
    dataset["tel_id"] = Array["tel_id"]


def transfer_Data_to_hdf5(Filename, right_tel, num_events, image_1, image_2, image_3, new_data_name=None):
    try:
        source = hessio_event_source(Filename, allowed_tels=right_tel)
    except:
        os.exit(1)

    if new_data_name == None:
        new_data_name = Filename.replace(".simtel.gz", ".hdf5")

    if_first = True
    # np.concatenate np.vstack np.hstack np.append
    image1_index = 0
    image2_index = 0
    image3_index = 0
    chunk_size = 1000
    dset_1 = ""
    dset_2 = ""
    dset_3 = ""
    image_infos = {"image": np.zeros((chunk_size, 1, 1764, 25)), "image_2": np.zeros((chunk_size, 1, 11328, 64)), "image_3": np.zeros((chunk_size, 2, 1855, 64))}
    events_info = {"mc_E": [], "mc_altitude": [], "mc_azimuth": [], "mc_core_x": [], "mc_core_y": [], "mc_h_first_int": [], "mc_azimuth_raw": [], "mc_altitude_raw": [], "mc_azimuth_cor": [], "mc_altitude_cor": [], "mc_time_slice": [], "mc_refstep": [], "tel_id": [], "mc_gamma_proton": [], "img_type": [], "img_index": []}
    new_image = 0
    hdf5 = h5py.File(new_data_name, 'w')
    shape_array = []
    anzahl = 0
    start_time = time.time()
    for event in source:
        if if_first:
            set_mc_header(event, hdf5)
            set_tel_info(event, right_tel, hdf5)
            if_first = False
        for tel_id in event.r0.tels_with_data:
            events_info = set_mc(event, tel_id, events_info)
            new_image = [event.r0.tel[tel_id].adc_samples]
            new_image = np.array(new_image, dtype=np.dtype("int32"))

            if new_image.shape[1] == 1 and new_image.shape[2] == 1764:
                events_info["img_type"].append(1)
                events_info["img_index"].append(image1_index)
                image_infos["image"][image1_index % chunk_size] = new_image
                image1_index += 1
                if image1_index == chunk_size:
                    dset_1 = hdf5.create_dataset('image1', data=image_infos["image"], chunks=(chunk_size, 1, 1764, 25), maxshape=(None, 1, 1764, 25), compression="gzip", compression_opts=9)
                    hdf5.close()
                    ohsD

                    image_infos["image"] = np.zeros((chunk_size, 1, 1764, 25))
                elif image1_index % chunk_size == 0:
                    dset_1.resize(dset_1.shape[0] + chunk_size, axis=0)
                    dset_1[-chunk_size:] = image_infos["image"]
                    image_infos["image"] = np.zeros((chunk_size, 1, 1764, 25))

                '''

                MST
                DC
                FlashCam
                '''
            elif new_image.shape[1] == 1:
                events_info["img_type"].append(2)
                events_info["img_index"].append(image2_index)
                image_infos["image_2"][image2_index % chunk_size] = new_image
                image2_index += 1
                if image2_index == chunk_size:
                    dset_2 = hdf5.create_dataset('image2', data=image_infos["image_2"], chunks=(chunk_size, 1, 11328, 64), maxshape=(None, 1, 11328, 64), compression="gzip", compression_opts=9)
                    image_infos["image_2"] = np.zeros((chunk_size, 1, 11328, 64))
                elif image2_index % chunk_size == 0:
                    dset_2.resize(dset_2.shape[0] + chunk_size, axis=0)
                    dset_2[-chunk_size:] = image_infos["image_2"]
                    image_infos["image_2"] = np.zeros((100, 1, 11328, 64))
                '''
                SCT
                MST
                SC
                SCTCam
                '''
            else:
                events_info["img_type"].append(3)
                events_info["img_index"].append(image3_index)
                image_infos["image_3"][image3_index % chunk_size] = new_image
                image3_index += 1
                if image3_index == chunk_size:
                    dset_3 = hdf5.create_dataset('image3', data=image_infos["image_3"], chunks=(chunk_size, 2, 1855, 64), maxshape=(None, 2, 1855, 64), compression="gzip", compression_opts=9)
                    image_infos["image_3"] = np.zeros((chunk_size, 2, 1855, 64))
                elif image3_index % chunk_size == 0:
                    dset_3.resize(dset_3.shape[0] + chunk_size, axis=0)
                    dset_3[-chunk_size:] = image_infos["image_3"]
                    image_infos["image_3"] = np.zeros((100, 2, 1855, 64))

                '''

                MST
                DC
                NectarCam
                '''
        # if anzahl == 10:
        #    break
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
            print(string_len("02", 2) + "\t" + string_len(anzahl, 5) + "/" + string_len(num_events, 5) + "\t" + str(image1_index) + "," + str(image2_index) + "," + str(image3_index) + "\t" + string_len(stunde, 2) + ":" + string_len(minute, 2) + ":" + string_len(zeit, 2))
    dataset = hdf5.create_dataset('events', (len(events_info["mc_E"]),),
                                  dtype=[('mc_E', '<f8'),
                                         ('mc_altitude', '<f8'),
                                         ('mc_azimuth', '<f8'),
                                         ('mc_core_x', '<f8'),
                                         ('mc_core_y', '<f8'),
                                         ('mc_h_first_int', '<f8'),
                                         ('mc_azimuth_raw', '<f8'),
                                         ('mc_altitude_raw', '<f8'),
                                         ('mc_azimuth_cor', '<f8'),
                                         ('mc_altitude_cor', '<f8'),
                                         ('mc_time_slice', '<f8'),
                                         ('mc_refstep', '<f8'),
                                         ('tel_id', 'i2'),
                                         ('mc_gamma_proton', 'i2')])
    dataset["mc_E"] = events_info["mc_E"]
    dataset["mc_altitude"] = events_info["mc_altitude"]
    dataset["mc_azimuth"] = events_info["mc_azimuth"]
    dataset["mc_core_x"] = events_info["mc_core_x"]
    dataset["mc_core_y"] = events_info["mc_core_y"]
    dataset["mc_h_first_int"] = events_info["mc_h_first_int"]
    dataset["mc_azimuth_raw"] = events_info["mc_azimuth_raw"]
    dataset["mc_altitude_raw"] = events_info["mc_altitude_raw"]
    dataset["mc_azimuth_cor"] = events_info["mc_azimuth_cor"]
    dataset["mc_altitude_cor"] = events_info["mc_altitude_cor"]
    dataset["mc_time_slice"] = events_info["mc_time_slice"]
    dataset["mc_refstep"] = events_info["mc_refstep"]
    dataset["tel_id"] = events_info["tel_id"]
    dataset["mc_gamma_proton"] = events_info["mc_gamma_proton"]

    #hdf5.create_dataset('image1', data=image_infos["image"], chunks=(10000,), maxshape=(None,))
    #hdf5.create_dataset('image3', data=events_info["image_3"], chunks=(10000,), maxshape=(None,))

    #dat_im_1["image"] = events_info["image"]

    hdf5.close()
    return new_data_name


def main(Filename, Nummer):
    # Nummer, Filename = get_num_filename(argv)
    #right_tel = set_right_tel(Filename)
    #num_events, image_1, image_2, image_3 = get_num_events(Filename, right_tel)
    right_tel = [22, 56, 90, 116, 121, 28, 62, 33, 67, 36, 37, 70, 39, 71, 73, 105, 107, 41, 42, 75, 76, 110, 24, 9, 11, 13, 14, 15, 16, 29, 30, 38, 43, 46, 47, 48, 49, 50, 58, 63, 64, 72, 77, 82, 83, 96, 97, 106, 111, 66, 104, 65, 12, 45, 79, 18, 52, 23, 57, 26, 91, 60, 94, 34, 68, 102, 17, 51, 84, 85, 92, 98, 25, 59, 21, 32, 55, 81, 89, 100, 101, 112, 117, 122, 10, 19, 20, 27, 35, 44, 53, 54, 61, 69, 78, 80, 87, 88, 95, 103, 113, 114, 115, 118, 119, 120, 123, 124, 125, 31, 40, 74, 86, 99, 93, 108, 109]
    num_events = 2188
    image_1 = 85332
    image_2 = 85332
    image_3 = 85332
    print(image_1)
    print(image_2)
    print(image_3)
    new_data_name = transfer_Data_to_hdf5(Filename, right_tel, num_events, image_1, image_2, image_3)


if __name__ == "__main__":
    Files = os.popen('find ../Master_Daten/PROD3/* -name "*.simtel.gz"').read().split('\n')
    main(Files[0], "2")
