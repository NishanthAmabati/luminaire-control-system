#!/usr/bin/bash

cd "$HOME/.luminaire-control/"
/usr/bin/python3 pi-server &

while ! /usr/bin/pgrep -x "Xorg" > /dev/null; do
  /usr/bin/sleep 10
done
/usr/bin/firefox localhost:3000