#!/usr/bin/env -S bash 

clear
rm logs/*log*

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Config
JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
PROGUARD_DIR="$HOME/tools/proguard"
MAVEN_BIN="/usr/bin/mvn"
JAVA_LIBS="$JAVA_HOME/lib/rt.jar"
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

# ─────────────────────────────────────────────────────────────
echo "... starting"

echo "Running Maven      ... please wait ----------------------------------------------------------------"
echo "Started at  $(date +%T)"

"$MAVEN_BIN" clean    > "$LOG_DIR/000-mvn-clean.log"    2>&1
clean_exit=$?
"$MAVEN_BIN" package  > "$LOG_DIR/001-mvn-package.log"  2>&1
package_exit=$?

echo "Finished at  $(date +%T)"
echo "Maven completed with exit code $?"

# ─────────────────────────────────────────────────────────────
if [[ $clean_exit -ne 0 || $package_exit -ne 0 ]]; then
  echo "Maven failed. Showing logs:"
  [[ $clean_exit -ne 0 ]] && echo "--- Clean log ---" && cat "$LOG_DIR/000-mvn-clean.log"
  [[ $package_exit -ne 0 ]] && echo "--- Package log ---" && cat "$LOG_DIR/001-mvn-package.log"
  echo "Exiting. /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\\"
  exit 1
fi

echo "Running ProGuard   ... please wait ----------------------------------------------------------------"
echo "Started at  $(date +%T)"

java -jar "$PROGUARD_DIR/lib/proguard.jar" -include 3.1.0-Java11.pro > "$LOG_DIR/002-proguard.log" 2>&1

echo "Finished at  $(date +%T)"
echo "---------------------------------------------------------------------------------------------------"
echo "Done... Ready to send to S3 ======================================================================="
echo
