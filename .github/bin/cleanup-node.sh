#!/usr/bin/env bash

set -euxo pipefail

echo "Space before cleanup"
df -h

echo "Removing redundant directories"
sudo rm -Rf /opt/hostedtoolcache/go/
sudo rm -rf /usr/local/lib/android
sudo rm -rf /usr/share/dotnet

echo "Space after cleanup"
df -h

