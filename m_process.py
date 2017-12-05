import multiprocessing
import os
import time


def open_python(name):
    os.system('nice -n 10 python3 process.py "' + str(name) + '" "PT7_BT2"')
    return True


conf_temp = open("conf/E_req_temp.yaml", "r").read()


List_num_data = ["100", "1000", "10000"]
List_num_trees = ["25", "50", "75", "100", "125"]
List_max_depth = ["5", "10", "20", "30", "40", "50"]


pool = multiprocessing.Pool(processes=1)
async_result = []
for i in List_num_trees:
    for j in List_max_depth:
        for k in List_num_data:
            name = "E_req_NT" + str(i) + "_MD" + str(j) + "_ND" + str(k)
            conf = open("conf/" + name + ".yaml", "w")
            conf.write(conf_temp.replace("num_data", k).replace("num_tree", i).replace("m_depth", j))
            conf.close()
            #async_result.append(pool.apply_async(open_python, (name,)))
pool.close()

for i in range(len(async_result)):
    geschaftt = async_result[i].get()
    print(str(i + 1) + "\t" + str(len(async_result)))
