echo 1000000 > /proc/sys/vm/max_map_count;

FLEXKV_SIZE=$((32*1024*1024*1024))
WARMUP=20
RUNTIME=$((${WARMUP}+100))

mkdir -p results/

# Run with DRAM (NUMA NODE = 1)
nice -20 numactl -N1 -m1 ./kvsbench -t 4 -T ${RUNTIME} -w ${WARMUP} -h 0.25 127.0.0.1:11211 -S ${FLEXKV_SIZE} > results/dram_client.txt;

sleep 20

# Run with NVM (NUMA NODE = 2)
nice -20 numactl -N1 -m2 ./kvsbench -t 4 -T ${RUNTIME} -w ${WARMUP} -h 0.25 127.0.0.1:11211 -S ${FLEXKV_SIZE} > results/nvm_client.txt;