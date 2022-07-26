# python
Python Codez

gpu.py 
goes in /opt/site24x7/monagent/plugins/gpu on a server.
provides gpu health monitoring info to site24x7
if the script timesout it sets execution_time to 9999 to let site24x7 know that that gpustat (nvidia-smi) did not complete in a timely manner
which may indicate a system problem.

