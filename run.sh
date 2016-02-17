#!/bin/bash

set -e
set -x

JAVA_ARGS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=150 -XX:+ParallelRefProcEnabled -Xmx14617M"
FT="csv"
OFILE="result.${FT}"

rm -f hs_* || true

[[ -f "$OFILE" ]] &&  rm ${OFILE}
mvn clean package
clear
mvn -version
java ${JAVA_ARGS} -jar target/benchmarks.jar -foe true -rf ${FT} -rff ${OFILE} "$@"

