#!/bin/python3
'''
This run script runs the workload of choice from the workloads repository.
'''

import os
import signal
import argparse
import subprocess

WORKLOADS_PATH = '/mnt/nvme/workloads'
PEBS_PATH      = '/mnt/nvme/PEBS_page_tracking'

# workloads-list
workloads = [
    'gups',
    'xsbench',
    'gapbs-bc',
    'gapbs-cc',
    'gapbs-pr',
    'graph500',
    'liblinear'
]

pipe = '/tmp/pebs_pipe'

def start_pebs(output_file):
    sampling_period = 50
    epoch_size      = (500 * 1000) # 500ms

    # Build PEBS if not built
    #os.system('cd {} && make'.format(PEBS_PATH))

    # Create a pipe for sending 'q' to PEBS
    # Check if pipe exists, delete if it does
    if os.path.exists(pipe):
        os.system('rm {}'.format(pipe))
    os.system('mkfifo {}'.format(pipe))

    # Run PEBS in the background, capture stdout to a file
    # Use subprocess to run in the background
    cmd = 'sudo {}/bin/pebs_periodic_reads.x {} {} {} < {} &'.format(PEBS_PATH, sampling_period, epoch_size, output_file, pipe)
    pebs_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if pebs_proc.poll() is not None:
        print('PEBS failed to start.')
        exit(1)

    os.system('sleep infinity > {} &'.format(pipe))
    
    return pebs_proc

    # # Send SIGINT to stop PEBS
    # pebs_pid = os.popen('pgrep pebs').read().strip()
    # os.system('sudo kill -s 2 {}'.format(pebs_pid))

def stop_pebs(pebs_proc):
    print('Stopping PEBS...')
    # End PEBS process
    # PEBS process can be ended by writing 'q' to the stdin
    os.system('echo "q" > {}'.format(pipe))
    os.system('rm {}'.format(pipe))

    # Write PEBS to a file
    stdout, stderr = pebs_proc.communicate()
    with open('pebs.out', 'w') as f:
        f.write(stdout.decode('utf-8'))

def main():
    parser = argparse.ArgumentParser(description='Run a workload.')
    parser.add_argument('workload', type=str, help='The workload to run.')
    args = parser.parse_args()

    # If no workload, print help message
    if args.workload is None:
        parser.print_help()
        exit(1)

    # Check if workload exists
    if args.workload not in workloads:
        print('Workload not found.')
        print('Available workloads: {}'.format(workloads))
        exit(1)

    if args.workload == 'gups':
        num_threads      = 8
        mem_size_log     = 35 # (1 << 35) = 32 GB
        num_iter         = 1 * 1000 * 1000 * 1000
        hot_mem_size_log = mem_size_log / 4
        item_size        = 8

        # Need to enable hugepages for gups
        nr_hugepages = (1 << mem_size_log) / (1 << 21)
        os.system('sudo bash -c "echo %d > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"' % nr_hugepages)

        # Build GUPS if not built
        os.system('cd {}/gups_hemem && make'.format(WORKLOADS_PATH))

        # Run GUPS
        pebs_proc = start_pebs('gups_mem_samples.dat')
        os.system('taskset 0xFF {}/gups_hemem/gups-skewed {} {} {} {} {}'.format(WORKLOADS_PATH, num_threads, num_iter, mem_size_log, item_size, hot_mem_size_log))
        stop_pebs(pebs_proc)

        # Disable hugepages
        os.system('sudo bash -c "echo 0 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')

    elif args.workload == 'xsbench':
        num_threads = 8
        particles   = 20000000 # Should take about 64GB
        gridpoints  = 130000

        # Build XSBench if not built
        os.system('cd {}/XSBench/openmp-threading && make'.format(WORKLOADS_PATH))

        # Run XSBench
        os.system('taskset 0xFF {}/XSBench/openmp-threading/XSBench -t {} -p {} -g {}'.format(WORKLOADS_PATH, num_threads, particles, gridpoints))

    elif args.workload == 'gapbs-bc':
        num_threads = 8
        num_iter    = 16
       # graph_path  = '/mnt/nvme/dataset/gapbs/twitter.sg' # other options: kron_s28.sg
        graph_path  = '/mnt/nvme/dataset/gapbs/kron_s28.sg'

        # Build GAPBS if not built
        os.system('cd {}/gapbs && make'.format(WORKLOADS_PATH))

        # Run GAPBS
        os.system('OMP_NUM_THREADS={} taskset 0xFF {}/gapbs/bc -n {} -f {}'.format(num_threads, WORKLOADS_PATH, num_iter, graph_path))

    elif args.workload == 'gapbs-cc':
        num_threads = 8
        num_iter    = 16
        #graph_path  = '/mnt/nvme/dataset/gapbs/twitter.sg' # other options: kron_s28.sg
        graph_path  = '/mnt/nvme/dataset/gapbs/kron_s28.sg'

        # Build GAPBS if not built
        os.system('cd {}/gapbs && make'.format(WORKLOADS_PATH))

        # Run GAPBS
        pebs_proc = start_pebs('gapbs-cc.dat')
        os.system('OMP_NUM_THREADS={} taskset 0xFF {}/gapbs/cc -n {} -f {}'.format(num_threads, WORKLOADS_PATH, num_iter, graph_path))
        stop_pebs(pebs_proc)

    elif args.workload == 'gapbs-pr':
        num_threads = 8
        num_iter    = 16
        #graph_path  = '/mnt/nvme/dataset/gapbs/twitter.sg' # other options: kron_s28.sg
        graph_path  = '/mnt/nvme/dataset/gapbs/kron_s28.sg'

        # Build GAPBS if not built
        os.system('cd {}/gapbs && make'.format(WORKLOADS_PATH))

        # Run GAPBS
        os.system('OMP_NUM_THREADS={} taskset 0xFF {}/gapbs/pr -n {} -f {}'.format(num_threads, WORKLOADS_PATH, num_iter, graph_path))

    elif args.workload == 'graph500':
        num_threads     = 8
        size            = 27 # 2^27 vertices ~ 68GB
        skip_validation = 1

        # Build Graph500 if not built
        os.system('cd {}/graph500 && make'.format(WORKLOADS_PATH))

        # Run Graph500
        os.system('SKIP_VALIDATION={} OMP_NUM_THREADS={} taskset 0xFF {}/graph500/omp-csr/omp-csr -s {} -V'.format(skip_validation, num_threads, WORKLOADS_PATH, size))

    elif args.workload == 'liblinear':
        num_threads = 8
        dataset    = '/mnt/nvme/dataset/liblinear/kdd12' # other options: kdda, kddb

        # Build Liblinear if not built
        os.system('cd {}/liblinear-2.47 && make'.format(WORKLOADS_PATH))

        # Run training
        os.system('taskset 0xFF {}/liblinear-2.47/train -s 6 -m {} {}'.format(WORKLOADS_PATH, num_threads, dataset))

main()
