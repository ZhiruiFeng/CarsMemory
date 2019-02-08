for worker in $(seq 1 4); do
    peg sshcmd-node processing-cluster $worker "pkill python3"
    echo "Killed all python3 processes on processing-cluster $woker"
done
