#!/bin/bash
# Chunked ranged download of the RQTT archive.
#
# A single streaming GET stalls at well under 1 MB through this machine's TLS
# interception; ranged requests come back at ~840 kB/s each and several run
# concurrently, so the archive is pulled in 16 MB pieces and concatenated.
# Each piece is size-checked and retried, so a dropped range is re-fetched
# rather than silently leaving a hole in the zip.
URL="https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Vectoriel/Theme/Local/RQTT/OGC(GPKG)/RQTT_GPKG.zip"
DIR="$(cd "$(dirname "$0")/.." && pwd)/data/cache/rqtt"
SIZE=408695811
CHUNK=16777216
N=$(( (SIZE + CHUNK - 1) / CHUNK ))

fetch() {
  i=$1
  start=$(( i * CHUNK ))
  end=$(( start + CHUNK - 1 ))
  [ $end -ge $SIZE ] && end=$(( SIZE - 1 ))
  want=$(( end - start + 1 ))
  out="$DIR/parts/p$(printf '%03d' $i)"
  for attempt in 1 2 3 4 5; do
    if [ -f "$out" ] && [ "$(stat -c %s "$out" 2>/dev/null)" = "$want" ]; then
      return 0
    fi
    curl -s --max-time 300 -r ${start}-${end} "$URL" -o "$out" 2>/dev/null
    if [ -f "$out" ] && [ "$(stat -c %s "$out" 2>/dev/null)" = "$want" ]; then
      echo "chunk $i/$N ok"
      return 0
    fi
    sleep 2
  done
  echo "chunk $i FAILED"
  return 1
}
export -f fetch
export URL DIR SIZE CHUNK N

seq 0 $((N-1)) | xargs -P 6 -I{} bash -c 'fetch {}'

got=$(cat "$DIR"/parts/p* | wc -c)
if [ "$got" != "$SIZE" ]; then
  echo "INCOMPLETE: $got of $SIZE bytes"
  exit 1
fi
cat "$DIR"/parts/p* > "$DIR/RQTT_GPKG_20260703.zip"
echo "DONE $(stat -c %s "$DIR/RQTT_GPKG_20260703.zip") bytes"
