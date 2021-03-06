import multiprocessing
import os
import time


def open(name):
    nummer = name.split("run")[1].split("___")[0]
    os.system('nice -n 10 python3 preprocess_hdf5_new.py ' + str(nummer) + ' "' + name + '"')
    return True


Files = os.popen('find hdf5_event -name "*no_0_dl1.hdf5"').read().split('\n')
pool = multiprocessing.Pool(processes=1)
async_result = []
for i in range(len(Files)):
    if i == len(Files) - 1:
        continue
    async_result.append(pool.apply_async(open, (Files[i],)))
    # time.sleep(120)
pool.close()

for i in range(len(async_result)):
    print(str(i + 1) + "\t" + str(len(async_result)))
    geschaftt = async_result[i].get()
