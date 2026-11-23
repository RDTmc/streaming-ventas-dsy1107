#!/usr/bin/env bash
# Crea swap persistente para hosts pequeños del laboratorio (2 GB de RAM).
# Uso: ./bootstrap-swap.sh [tamano]   ej: ./bootstrap-swap.sh 2G
set -euo pipefail

SIZE="${1:-1G}"
SWAPFILE="/swapfile"

if [ ! -f "$SWAPFILE" ]; then
  echo "Creando $SWAPFILE de $SIZE..."
  if ! sudo fallocate -l "$SIZE" "$SWAPFILE" 2>/dev/null; then
    case "$SIZE" in
      *G) MB=$(( ${SIZE%G} * 1024 ));;
      *M) MB=${SIZE%M};;
      *) MB=1024;;
    esac
    sudo dd if=/dev/zero of="$SWAPFILE" bs=1M count="$MB" status=none
  fi
  sudo chmod 600 "$SWAPFILE"
  sudo mkswap "$SWAPFILE" >/dev/null
fi

if ! sudo swapon --show | grep -q "$SWAPFILE"; then
  sudo swapon "$SWAPFILE"
fi

if ! grep -q "^$SWAPFILE" /etc/fstab; then
  echo "$SWAPFILE none swap sw 0 0" | sudo tee -a /etc/fstab >/dev/null
fi

echo "vm.swappiness=10" | sudo tee /etc/sysctl.d/99-lab-swap.conf >/dev/null
sudo sysctl -w vm.swappiness=10 >/dev/null

swapon --show
free -m
