# nifi.docker-compose
A Docker compose stack of NiFi and NiFi Registry using using volumes for persistent storage.

## Pre requisites
You need to have `ghq` installed on your laptop/desktop for the following commands to work. Here is one way of getting ghq in your laptop/desktop.

```
# Create a new conda environment for ghq
conda create -n ghq
conda activate ghq
conda install -c conda-forge go-ghq
```
### New to ghq?
https://dev.to/ekeih/managing-your-git-repositories-with-ghq-3ffa

## Usage

```
# Bring up the Stateful HA ZooKeeper Stack
ghq get code.scraawl.me/pcarlos/zookeeper.docker-compose
cd ~/ghq/code.scraawl.me/pcarlos/zookeeper.docker-compose
docker-compose up -d

# Bring up the NiFi stack
ghq get code.scraawl.me/pcarlos/nifi.docker-compose
cd ~/ghq/code.scraawl.me/pcarlos/nifi.docker-compose
docker-compose up -d

# Browse to your NiFi Cluster
xdg-open http://localhost:9090/nifi

# Open your NiFi Registry
xdg-open http://localhost:9091/nifi-registry

# Connect NiFi Registry to Cluster
xdg-open https://nifi.apache.org/docs/nifi-registry-docs/html/getting-started.html#i-started-nifi-registry-now-what
```

You can bring the stack down with `docker compose down`

If you want to erase the volumes issue `docker compose down -v`. 
WARNING: There is no coming back from this.
