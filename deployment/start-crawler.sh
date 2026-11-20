#!/bin/sh
set -eu

# Xvfb reports the display number once the virtual display is ready.
display_file=$(mktemp)
trap 'rm -f "$display_file"' EXIT
Xvfb -displayfd 3 -screen 0 1280x800x24 -nolisten tcp 3>"$display_file" &
xvfb_pid=$!
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if [ -s "$display_file" ]; then
        read -r display_number < "$display_file"
        rm -f "$display_file"
        trap - EXIT
        export DISPLAY=":$display_number"
        exec "$@"
    fi
    kill -0 "$xvfb_pid"
    sleep 1
done

echo 'Xvfb did not become ready' >&2
kill "$xvfb_pid"
exit 1
