#!/usr/bin/python3
import gpustat
import json
import subprocess
import os, sys
import time

def processCount(processname):
        tmp = os.popen("ps -Af").read()
        tmp = "\n".join((line for line in tmp.split("\n")
                         if "SCREEN" not in line))
        proccount = tmp.count(processname)
        return(proccount)
def duplicationKill(Process):
    if processCount(Process) > 3:
        sys.exit(1)

# Prevent multiple copies of the script from running
duplicationKill('gpu.py')

# Initial json setup
PLUGIN_VERSION = "4"
HEARTBEAT_REQUIRED = "true"
output = gpustat.new_query()
result_json = {}
result_json["plugin_version"] = PLUGIN_VERSION
result_json["heartbeat_required"] = HEARTBEAT_REQUIRED
result_json["execution_time"] = 9999

#capture start time
start_time = time.time()

# Create Defaults
for x in range(0, 8):
    result_json[f"gpu_temp_{x}"] = '0'
    result_json[f"gpu_type_{x}"] = ''
    result_json[f"gpu_process_{x}"] = 'none'

# Get the gpu stats
cmd = '/usr/local/bin/gpustat -cp'
timeout_s = 60

try:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate(timeout=timeout_s)
    p_status = p.wait()
    output = output.decode()
    # parse the cpu stats
    core_output = output.split('\n')
    for each in core_output:
        if each.startswith('['):
            core_out_line = each.split('|')

            gpu_num_and_name = core_out_line[0].split(']')
            gpu_temp_and_load = core_out_line[1].split(',')
            gpu_memory = core_out_line[2].replace(' ','')
            gpu_proc = "".join(core_out_line[3:]).strip().replace(' ', ',')

            gpu_num = gpu_num_and_name[0].replace('[','').strip()
            gpu_name = gpu_num_and_name[1].strip()
            gpu_temp = gpu_temp_and_load[0].replace("'C",'').strip()
            gpu_load = gpu_temp_and_load[1].replace("%",'').strip()
            gpu_type = gpu_name.split(' ')[-1].strip()

            result_json[f"gpu_type_{gpu_num}"] = gpu_type
            result_json[f"gpu_temp_{gpu_num}"] = gpu_temp
            if (len(gpu_proc) > 2):
                result_json[f"gpu_process_{gpu_num}"] = gpu_proc
            else:
                result_json[f"gpu_process_{gpu_num}"] = 'none'
    result_json[f"execution_time"] = round((time.time() - start_time),2) 
except subprocess.TimeoutExpired:
    #print(f'Timeout for {cmd} ({timeout_s}s) expired')
    print('')

print(json.dumps(result_json))
