#!/usr/bin/env bash
# One-shot migration of Docker's data-root to the new 1TB SSD (TEAM T2531TB).
#
#   sudo bash scripts/migrate_docker_to_ssd.sh
#
# What it does (expect ~20-30 min, Docker and ALL containers are DOWN during the copy):
#   1. Partitions + formats the blank SSD (ext4, label DOCKER_SSD) — refuses if the
#      disk already has partitions, a filesystem, or is mounted.
#   2. Mounts it at /mnt/docker-ssd and adds a LABEL-based fstab entry
#      (device names shuffled sda<->sdb on the last boot; labels don't care).
#   3. Stops docker + containerd, rsyncs /var/lib/docker -> SSD (everything moves:
#      images, overlay layers, build cache, and the Postgres volumes with all data).
#   4. Merges "data-root" into /etc/docker/daemon.json (preserves the nvidia runtime).
#   5. Restarts Docker; containers come back via their restart policies.
#
# The old /var/lib/docker is left untouched as a rollback. After a few healthy days:
#   sudo rm -rf /var/lib/docker.old-hdd   (it gets renamed at the end of this script)
set -euo pipefail

[ "$(id -u)" -eq 0 ] || { echo "run with sudo"; exit 1; }

MODEL_MATCH="T2531TB"
MOUNT=/mnt/docker-ssd
NEW_ROOT=$MOUNT/docker

DISK=""
while read -r name rota model; do
    if [ "$rota" = "0" ] && echo "$model" | grep -q "$MODEL_MATCH"; then
        DISK="/dev/$name"
    fi
done < <(lsblk -d -n -o NAME,ROTA,MODEL | grep -v loop)
[ -n "$DISK" ] || { echo "ERROR: SSD with model $MODEL_MATCH not found"; exit 1; }

if [ -n "$(lsblk -n -o FSTYPE "$DISK" | tr -d '[:space:]')" ] || [ "$(lsblk -n "$DISK" | wc -l)" -gt 1 ]; then
    echo "ERROR: $DISK already has a filesystem or partitions — refusing to format."
    lsblk "$DISK"
    exit 1
fi
if lsblk -n -o MOUNTPOINT "$DISK" | grep -q .; then
    echo "ERROR: $DISK is mounted somewhere — refusing."
    exit 1
fi

echo "==> Target SSD: $DISK"
lsblk -d -o NAME,SIZE,MODEL "$DISK"
read -r -p "Format $DISK and migrate /var/lib/docker to it? [y/N] " ans
[ "$ans" = "y" ] || [ "$ans" = "Y" ] || { echo "aborted"; exit 0; }

echo "==> Partitioning + formatting"
parted -s "$DISK" mklabel gpt mkpart primary ext4 1MiB 100%
sleep 2
PART="${DISK}1"
[ -b "$PART" ] || PART="${DISK}p1"
mkfs.ext4 -q -L DOCKER_SSD "$PART"

echo "==> Mounting at $MOUNT"
mkdir -p "$MOUNT"
grep -q "LABEL=DOCKER_SSD" /etc/fstab || \
    echo "LABEL=DOCKER_SSD $MOUNT ext4 defaults,noatime 0 2" >> /etc/fstab
mount "$MOUNT"

echo "==> Stopping Docker (all containers go down now)"
systemctl stop docker.socket docker containerd

echo "==> Copying /var/lib/docker -> $NEW_ROOT (this is the long part)"
mkdir -p "$NEW_ROOT"
rsync -aHAX --info=progress2 /var/lib/docker/ "$NEW_ROOT/"

echo "==> Pointing daemon.json at the SSD (merging, nvidia runtime preserved)"
cp /etc/docker/daemon.json /etc/docker/daemon.json.bak.$(date +%s) 2>/dev/null || true
python3 - << 'PYEOF'
import json
path = "/etc/docker/daemon.json"
try:
    with open(path) as f:
        cfg = json.load(f)
except FileNotFoundError:
    cfg = {}
cfg["data-root"] = "/mnt/docker-ssd/docker"
with open(path, "w") as f:
    json.dump(cfg, f, indent=4)
print(json.dumps(cfg, indent=4))
PYEOF

echo "==> Starting Docker from the SSD"
systemctl start containerd
systemctl start docker

sleep 5
docker info --format 'data-root now: {{.DockerRootDir}}'
docker ps --format '{{.Names}}\t{{.Status}}' | head -15

echo "==> Renaming old HDD copy to /var/lib/docker.old-hdd (rollback; delete after a few good days)"
mv /var/lib/docker /var/lib/docker.old-hdd

echo "==> DONE. Verify containers go healthy, then trading resumes on its own."
