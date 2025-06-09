#!/usr/bin/bash

cd "$HOME/.luminaire-control/"
./pi-server &

while ! /usr/bin/pgrep -x "lxsession" > /dev/null; do
  /usr/bin/sleep 10
done
/usr/bin/firefox localhost:3000
