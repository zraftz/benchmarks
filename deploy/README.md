# Manual three-host deployment

The automatic supervisor is local-only. This inventory helper emits node
configurations; it does not open cloud accounts, provision hosts, deploy code,
or execute SSH commands. The standalone binaries can listen on addresses on
three different machines; the same Go client can run on a fourth machine.

1. Build for the server architecture with `./raft-bench build`; use the same
   reviewed source/lockfile and compiler across hosts. Retain `dist/build.json`.
2. Copy `three-hosts.example.json`, edit its cluster ID, bind addresses, and
   absolute data paths for your isolated network, then generate configurations.

```sh
./raft-bench node-configs \
  --inventory deploy/three-hosts.example.json \
  --output deploy/generated
```

3. Copy the matching node configuration and **one selected implementation's**
   binary to each host. Create writable data directories with restrictive
   permissions. All binaries require `--config` followed by the configuration path.
On host 1, for example:

```sh
./raft-bench-rafter --config node-1.json
# Or: ./raft-bench-raft-rs --config node-1.json
# Or: ./raft-bench-openraft --config node-1.json
```

For a fresh OpenRaft cluster only, after all three processes are running:

```sh
./raft-bench initialize --address 10.0.0.11:7000
```

Do not reinitialize an existing cluster, mix implementations in a cluster, or
reuse another implementation's data folders. Keep ports off the public Internet:
there is no TLS, peer authentication, authorization, or hardened public API.

4. From the fourth, load-generator host, after checking cluster health:

```sh
./raft-bench-load \
  --nodes 1=10.0.0.11:7000,2=10.0.0.12:7000,3=10.0.0.13:7000 \
  --session remote-trial-unique-session \
  --namespace trial-1 \
  --duration 60s --timeout 2s --concurrency 64 \
  --payload 512 --keyspace 10000 --rate 1000 \
  --output remote-measurement.json
```

Use new session IDs and output paths for each run. Do not feed this raw output
into the local qualified-suite report as though its prerequisites ran. The
manual path currently lacks automatic remote history qualification, recovery
canaries, host metric collection, and failure control. It is an exploratory
multi-host measurement until you execute and retain equivalent checks and
inventories. Remote performance has not been measured in this delivery.
