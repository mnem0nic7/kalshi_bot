# Migrate Docker data (incl. Postgres) from SMR HDD → SSD

**Date drafted:** 2026-05-19
**Status:** PLAN — execute only after the current Postgres recovery completes and the stack is verified healthy.
**Why:** Production Postgres data lives on `/var/lib/docker` on `sda` — a Seagate `ST8000DM004` 8 TB **SMR** HDD. SMR's random-write penalty drove ~1000 ms write latency, making crash recovery after an unclean reboot take 30–45+ min. A 931 GB SATA SSD (`sdb`, `WDC WDS100T2G0A`) sits **idle** (orphaned `zfs_member`/swap partitions, not in `/etc/fstab`, ZFS not installed). Moving Docker's data root onto the SSD fixes this at the source.

## Facts captured at drafting time
- `sda` (7.3 TB SMR HDD) → `sda2` ext4 = `/` → `/var/lib/docker` (Docker default root; no `data-root` set in `/etc/docker/daemon.json`).
- `sdb` (931.5 GB SSD, `rotational=0`): `sdb1` vfat EFI stub (unused — active EFI is `sda1`), `sdb2`+`sdb4` `zfs_member` (orphaned), `sdb3` 8 GB swap (inactive — swap is `/swap.img`). **Not referenced in `/etc/fstab`; nothing mounts it.**
- Docker data to move: `infra_postgres_production_data` 53.75 GB, `infra_postgres_demo_data` 14.26 GB, images ~5.3 GB, build cache ~2.6 GB → **~76 GB total** (fits easily).
- `/etc/docker/daemon.json` currently contains only the `nvidia` runtime block — **must be preserved**.
- Docker is a systemd service (`active`, `enabled`).
- Approach chosen: **mount the SSD at `/var/lib/docker`** (keeps `daemon.json` untouched; standard pattern; easy rollback). The named volumes are copied as-is, so **no DB re-init or re-migration** is needed.

> ⚠️ All `sudo` steps must be run by the operator in an interactive terminal (this environment has no passwordless sudo). Run them from the host shell, not inside a container.

---

## Phase 0 — Preconditions (do NOT start early)
1. Current recovery finished and stack healthy:
   ```bash
   cd ~/workspace/kalshi_bot
   docker compose -f infra/docker-compose.yml --env-file .env ps
   docker exec infra-postgres_production-1 pg_isready -U postgres -d kalshi_bot
   ```
   All app/daemon/web containers `Up`, both Postgres `healthy`. Confirm crypto collection is writing (see Phase 5 query).
2. The platform is in shadow mode (`APP_SHADOW_MODE=true`) and weather is paused, so a maintenance window here is low-risk — but **collection pauses for the duration** (rsync of ~76 GB *read off the SMR disk* may take 15–40 min).

## Phase 1 — Verify `sdb` is safe to wipe (data-destructive to sdb only)
```bash
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,MODEL
grep -v '^#' /etc/fstab                 # confirm no sdb reference
sudo blkid /dev/sdb1 /dev/sdb2 /dev/sdb3 /dev/sdb4
sudo lsof /dev/sdb* 2>/dev/null         # expect empty
which zpool zfs || echo "zfs not installed — zfs_member partitions are orphaned"
```
Proceed only if nothing mounts/uses `sdb`. **This destroys the orphaned ZFS/swap/EFI partitions on `sdb`.**

## Phase 2 — Partition + format the SSD
```bash
sudo wipefs -a /dev/sdb
sudo sgdisk --zap-all /dev/sdb
sudo sgdisk -n 1:0:0 -t 1:8300 -c 1:docker /dev/sdb      # single Linux-fs partition
sudo mkfs.ext4 -L docker-ssd /dev/sdb1
sudo blkid /dev/sdb1                                       # note the UUID for fstab
```
(If `sgdisk` is missing: `sudo apt install -y gdisk`.)

## Phase 3 — Stop the stack and the Docker daemon
```bash
cd ~/workspace/kalshi_bot
docker compose -f infra/docker-compose.yml --env-file .env stop
sudo systemctl stop docker docker.socket
ps aux | grep -i [d]ockerd                                # expect none
```

## Phase 4 — Copy data to the SSD, then mount it at /var/lib/docker
```bash
sudo mkdir -p /mnt/docker-ssd
sudo mount /dev/sdb1 /mnt/docker-ssd
sudo rsync -aHAX --info=progress2 /var/lib/docker/ /mnt/docker-ssd/
sudo du -sh /var/lib/docker /mnt/docker-ssd               # sizes should match closely

# swap the SSD into place
sudo umount /mnt/docker-ssd
sudo mv /var/lib/docker /var/lib/docker.old
sudo mkdir /var/lib/docker
UUID=$(sudo blkid -s UUID -o value /dev/sdb1)
echo "UUID=$UUID /var/lib/docker ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
sudo mount /var/lib/docker
findmnt /var/lib/docker                                    # confirm it's the SSD
```
`nofail` ensures a missing/failed SSD never blocks boot.

## Phase 5 — Start Docker and verify
```bash
sudo systemctl start docker
docker info | grep -i 'Docker Root Dir'                    # /var/lib/docker (now on SSD)
docker volume ls | grep postgres                           # both volumes present
cd ~/workspace/kalshi_bot
docker compose -f infra/docker-compose.yml --env-file .env up -d
# Postgres should now go healthy in seconds, not minutes:
docker exec infra-postgres_production-1 pg_isready -U postgres -d kalshi_bot
# Confirm crypto collection resumed at 365d:
docker exec infra-postgres_production-1 psql -U postgres -d kalshi_bot -c \
  "select max(observed_at) latest, count(*) n from crypto_market_snapshots where observed_at > now() - interval '10 min';"
```

## Phase 6 — Reclaim old space (after ~1 day of confidence)
```bash
sudo du -sh /var/lib/docker.old
sudo rm -rf /var/lib/docker.old                            # frees ~76 GB on the SMR disk
```

## Rollback (if anything looks wrong before Phase 6)
```bash
sudo systemctl stop docker docker.socket
sudo umount /var/lib/docker
sudo sed -i '\#/var/lib/docker ext4#d' /etc/fstab          # remove the SSD fstab line
sudo rmdir /var/lib/docker && sudo mv /var/lib/docker.old /var/lib/docker
sudo systemctl start docker
```
This returns Docker to the original SMR copy, untouched.

## Notes / risks
- The single biggest win: crash recovery and live write latency both collapse to SSD timescales.
- Keep `/var/lib/docker.old` until verified — it's the rollback safety net.
- Alternative to mounting at `/var/lib/docker`: set `"data-root": "/mnt/docker-ssd"` in `daemon.json` (preserving the `nvidia` runtime block) and mount `sdb1` at `/mnt/docker-ssd` in fstab. Equivalent result; the mount-at-`/var/lib/docker` approach is chosen here to avoid editing `daemon.json`.
- Consider longer term: tune `checkpoint_timeout`/`max_wal_size` so future recoveries replay less WAL regardless of disk.
