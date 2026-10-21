#!/usr/bin/env bash
# Explicitly configured localhost tc-netem experiment. This script is not run
# by the normal benchmark and requires sudo only for tc qdisc operations.
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: bash benchmarks/run_netem_file_transfer.sh [options]
  --loss PERCENT       netem random loss percentage (default: 1)
  --delay MS           one-way netem delay in milliseconds (default: 20)
  --reorder PERCENT    netem reordering percentage, 0 disables (default: 0)
  --sizes BYTES,...    file sizes (default: 4096,65536,1048576)
  --repetitions N      repetitions per size (default: 5)
  --output-root DIR    artifact parent directory (default: netem_results)

The script only accepts the default loopback root qdisc (noqueue), verifies
the installed netem qdisc, and removes it on exit if its exact configuration
is still active. It refuses to replace a pre-existing custom root qdisc.
USAGE
}

LOSS=1
DELAY=20
REORDER=0
SIZES=4096,65536,1048576
REPETITIONS=5
OUTPUT_ROOT=netem_results
while (($#)); do
  case "$1" in
    --loss) LOSS=${2:?missing value for --loss}; shift 2 ;;
    --delay) DELAY=${2:?missing value for --delay}; shift 2 ;;
    --reorder) REORDER=${2:?missing value for --reorder}; shift 2 ;;
    --sizes) SIZES=${2:?missing value for --sizes}; shift 2 ;;
    --repetitions) REPETITIONS=${2:?missing value for --repetitions}; shift 2 ;;
    --output-root) OUTPUT_ROOT=${2:?missing value for --output-root}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

numeric() { [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; }
for value in "$LOSS" "$DELAY" "$REORDER"; do
  numeric "$value" || { echo "Loss, delay, and reorder must be numeric." >&2; exit 2; }
done
numeric "$REPETITIONS" || { echo "Repetitions must be a positive integer." >&2; exit 2; }
[[ "$REPETITIONS" =~ ^[0-9]+$ ]] && ((REPETITIONS > 0)) || {
  echo "Repetitions must be a positive integer." >&2; exit 2;
}
awk -v v="$LOSS" 'BEGIN {exit !(v >= 0 && v <= 100)}' || { echo "Loss must be between 0 and 100." >&2; exit 2; }
awk -v v="$REORDER" 'BEGIN {exit !(v >= 0 && v <= 100)}' || { echo "Reorder must be between 0 and 100." >&2; exit 2; }
awk -v v="$DELAY" 'BEGIN {exit !(v > 0)}' || { echo "Delay must be greater than zero (required for reproducible reordering)." >&2; exit 2; }
command -v tc >/dev/null || { echo "iproute2 tc is required." >&2; exit 1; }
command -v sudo >/dev/null || { echo "sudo is required to configure qdisc state." >&2; exit 1; }

sudo -v
before=$(sudo tc qdisc show dev lo)
if ! grep -Eq '^qdisc noqueue 0: root([[:space:]]|$)' <<<"$before"; then
  echo "Refusing to replace existing loopback root qdisc:" >&2
  printf '%s\n' "$before" >&2
  exit 1
fi

mkdir -p "$OUTPUT_ROOT"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
out_dir="$OUTPUT_ROOT/netem_${stamp}_$$"
mkdir "$out_dir"
installed=0
installed_config=""

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if ((installed)); then
    current=$(sudo tc qdisc show dev lo 2>&1) || current=""
    ours=0
    if [[ -n "$installed_config" && "$current" == "$installed_config" ]]; then
      ours=1
    elif grep -Eq '^qdisc netem 7e57: root([[:space:]]|$)' <<<"$current" \
      && grep -Fq "loss ${LOSS}%" <<<"$current" \
      && grep -Fq "delay ${DELAY}ms" <<<"$current"; then
      # Cover a signal between successful qdisc installation and capturing
      # installed_config, while still matching our unique handle/settings.
      if awk -v v="$REORDER" 'BEGIN {exit !(v == 0)}' \
        || grep -Fq "reorder ${REORDER}%" <<<"$current"; then
        ours=1
      fi
    fi
    if ((ours)); then
      if sudo tc qdisc del dev lo root; then
        sudo tc qdisc show dev lo >"$out_dir/qdisc-after-cleanup.txt"
      else
        echo "Failed to remove the netem qdisc; inspect $out_dir/qdisc-configured.txt" >&2
        status=1
      fi
    else
      echo "Loopback qdisc changed during the run; left current qdisc untouched. Inspect $out_dir." >&2
      status=1
    fi
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

sudo tc qdisc add dev lo root handle 7e57: netem \
  loss "${LOSS}%" delay "${DELAY}ms" reorder "${REORDER}%"
installed=1
installed_config=$(sudo tc qdisc show dev lo)
printf '%s\n' "$before" >"$out_dir/qdisc-before.txt"
printf '%s\n' "$installed_config" >"$out_dir/qdisc-configured.txt"
printf 'date_utc=%s\n' "$stamp" >"$out_dir/environment.txt"
printf 'uname=%s\n' "$(uname -a)" >>"$out_dir/environment.txt"
printf 'tc_version=%s\n' "$(tc -V 2>&1)" >>"$out_dir/environment.txt"
printf 'loss_percent=%s\ndelay_ms=%s\nreorder_percent=%s\n' \
  "$LOSS" "$DELAY" "$REORDER" >>"$out_dir/environment.txt"

grep -Eq '^qdisc netem 7e57: root([[:space:]]|$)' <<<"$installed_config" || {
  echo "Could not verify the expected netem qdisc; refusing to benchmark." >&2
  exit 1
}
grep -Fq "loss ${LOSS}%" <<<"$installed_config" || {
  echo "Could not verify configured loss in tc output." >&2; exit 1;
}
grep -Fq "delay ${DELAY}ms" <<<"$installed_config" || {
  echo "Could not verify configured delay in tc output." >&2; exit 1;
}
if awk -v v="$REORDER" 'BEGIN {exit !(v > 0)}'; then
  grep -Fq "reorder ${REORDER}%" <<<"$installed_config" || {
    echo "Could not verify configured reordering in tc output." >&2; exit 1;
  }
fi

context="loopback_tc_netem_loss_${LOSS}pct_delay_${DELAY}ms_reorder_${REORDER}pct"
PYTHONPATH=. python3 benchmarks/compare_kernel_tcp.py \
  --sizes "$SIZES" --repetitions "$REPETITIONS" \
  --measurement-context "$context" \
  --csv "$out_dir/benchmark_results.csv" \
  --summary-csv "$out_dir/benchmark_summary.csv" \
  --plot "$out_dir/throughput_comparison.svg"
echo "Netem experiment artifacts: $out_dir"
