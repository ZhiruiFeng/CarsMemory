for worker in $(seq 1 4); do
    peg sshcmd-node processing-cluster $worker "python3 /home/ubuntu/workspace/CarsMemory/starter/consumer_starter.py"
    echo "[processing-cluster $woker] Started consuming..."
    sleep 1s
done
