import multiprocessing
import os
import time


def open_python(name):
    os.system('nice -n 10 python3 process.py ' + str(name) + ' "PT7_BT2"')
    return True


conf_temp = open("conf/E_req_temp.yaml", "r").read()

pool = multiprocessing.Pool(processes=1)
async_result = []
for i in range(1, 5):
    name = "E_req_" + str(10**i)
    conf = open("conf/" + name + ".yaml", "w")
    conf.write(conf_temp.replace("num_data", str(10**i)))
    async_result.append(pool.apply_async(open_python, (name,)))
pool.close()

for i in range(len(async_result)):
    print(str(i + 1) + "\t" + str(len(async_result)))
    geschaftt = async_result[i].get()
