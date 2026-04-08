#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_NAME="${0##*/}"
readonly CHECKPOINT_PROPOSED_EVENT="CheckpointProposed(uint256,bytes32,bytes32[],bytes32,bytes32)"
readonly CHECKPOINT_INVALIDATED_EVENT="CheckpointInvalidated(uint256)"
readonly ROLLUP_PROPOSE_SIG='propose((bytes32,(int256),(bytes32,bytes32,bytes32,bytes32,bytes32,uint256,uint256,address,bytes32,(uint128,uint128),uint256)),(bytes,bytes),address[],(uint8,bytes32,bytes32),bytes)'
readonly PROPOSE_SELECTOR="0x85b98fd8"

declare -a MONITORED_ADDRESSES=()
declare -A MONITORED_SET=()
declare -A SEQUENCER_LABELS=()
declare -A LOG_ALERT_LEVELS=()

load_dotenv_defaults() {
  local dotenv_file="$1"
  local entry key value

  [[ -f "$dotenv_file" ]] || return 0

  while IFS= read -r -d '' entry; do
    key="${entry%%=*}"
    value="${entry#*=}"
    if [[ -z "${!key+x}" ]]; then
      printf -v "$key" '%s' "$value"
      export "$key"
    fi
  done < <(
    (
      set -a
      # shellcheck disable=SC1090
      source "$dotenv_file"
      env -0
    ) | grep -z '^AZMON_'
  )
}

load_dotenv_defaults "$SCRIPT_DIR/.env"

AZMON_RPC_URL="${AZMON_RPC_URL:-http://localhost:8545}"
AZMON_ROLLUP_ADDRESS="${AZMON_ROLLUP_ADDRESS:-0xAe2001f7e21d5EcABf6234E9FDd1E76F50F74962}"
AZMON_DOCKER_CONTAINER="${AZMON_DOCKER_CONTAINER:-aztec-sequencer}"
AZMON_STATE_DIR="${AZMON_STATE_DIR:-$HOME/azmon/state}"
AZMON_POLL_INTERVAL_SEC="${AZMON_POLL_INTERVAL_SEC:-10}"
AZMON_LOG_LOOKBACK_SEC="${AZMON_LOG_LOOKBACK_SEC:-20}"
AZMON_LOG_ALERT_LEVELS="${AZMON_LOG_ALERT_LEVELS:-warn,error,fatal}"
AZMON_DUTY_INFO="${AZMON_DUTY_INFO:-false}"
AZMON_PROPOSAL_GRACE_SEC="${AZMON_PROPOSAL_GRACE_SEC:-120}"
AZMON_BLOCK_CONFIRMATIONS="${AZMON_BLOCK_CONFIRMATIONS:-2}"
AZMON_ALERT_COOLDOWN_SEC="${AZMON_ALERT_COOLDOWN_SEC:-900}"
AZMON_DRY_RUN="${AZMON_DRY_RUN:-false}"
AZMON_DEBUG="${AZMON_DEBUG:-false}"
AZMON_TELEGRAM_THREAD_ID="${AZMON_TELEGRAM_THREAD_ID:-}"
AZMON_SEQUENCER_LABELS="${AZMON_SEQUENCER_LABELS:-}"
AZMON_LOG_INCLUDE_REGEX="${AZMON_LOG_INCLUDE_REGEX:-}"
AZMON_LOG_EXCLUDE_REGEX="${AZMON_LOG_EXCLUDE_REGEX:-}"

log_ts() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

log_line() {
  local level="$1"
  shift
  printf '%s [%s] %s\n' "$(log_ts)" "$level" "$*" >&2
}

log_debug() {
  if is_true "$AZMON_DEBUG"; then
    log_line DEBUG "$@"
  fi
}

log_info() {
  log_line INFO "$@"
}

log_warn() {
  log_line WARN "$@"
}

log_error() {
  log_line ERROR "$@"
}

die() {
  log_error "$@"
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || die "missing required command: $cmd"
}

require_uint() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be an unsigned integer, got: $value"
}

is_true() {
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

normalize_address() {
  local value="${1,,}"
  [[ "$value" =~ ^0x[0-9a-f]{40}$ ]] || return 1
  printf '%s\n' "$value"
}

split_csv() {
  local csv="$1"
  tr ',' '\n' <<<"$csv" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e '/^$/d'
}

now_epoch() {
  date +%s
}

format_utc() {
  local ts="$1"
  date -u -d "@$ts" '+%Y-%m-%d %H:%M:%SZ'
}

join_lines() {
  local output="" line
  for line in "$@"; do
    if [[ -n "$output" ]]; then
      output+=$'\n'
    fi
    output+="$line"
  done
  printf '%s' "$output"
}

html_escape() {
  jq -Rr '@html' <<<"${1-}"
}

html_link() {
  local url="$1"
  local text="$2"
  printf '<a href="%s">%s</a>' "$(html_escape "$url")" "$(html_escape "$text")"
}

html_field_line() {
  local key="$1"
  local value="$2"
  printf '%s: %s' "$(html_escape "$key")" "$(html_escape "$value")"
}

html_field_line_html() {
  local key="$1"
  local value_html="$2"
  printf '%s: %s' "$(html_escape "$key")" "$value_html"
}

short_hex() {
  local value="$1"
  if ((${#value} <= 18)); then
    printf '%s\n' "$value"
    return
  fi
  printf '%s..%s\n' "${value:0:10}" "${value: -8}"
}

hash_text() {
  local value="$1"
  printf '%s' "$value" | sha256sum | awk '{print $1}'
}

state_file() {
  printf '%s/%s\n' "$AZMON_STATE_DIR" "$1"
}

state_get() {
  local file
  file="$(state_file "$1")"
  [[ -f "$file" ]] || return 0
  sed -n '1p' "$file"
}

state_put() {
  local file tmp
  file="$(state_file "$1")"
  tmp="$(mktemp "$AZMON_STATE_DIR/.state.XXXXXX")"
  printf '%s\n' "$2" >"$tmp"
  mv "$tmp" "$file"
}

json_init() {
  local file default_value
  file="$(state_file "$1")"
  default_value="$2"
  if [[ ! -f "$file" ]]; then
    printf '%s\n' "$default_value" >"$file"
  fi
}

json_get() {
  local file filter
  file="$(state_file "$1")"
  filter="$2"
  shift 2
  jq -cr "$@" "$filter" "$file"
}

json_put() {
  local file filter tmp
  file="$(state_file "$1")"
  filter="$2"
  shift 2
  tmp="$(mktemp "$AZMON_STATE_DIR/.json.XXXXXX")"
  jq -S "$@" "$filter" "$file" >"$tmp"
  mv "$tmp" "$file"
}

json_has() {
  local file key
  file="$(state_file "$1")"
  key="$2"
  jq -e --arg key "$key" 'has($key)' "$file" >/dev/null
}

runtime_mark() {
  local field="$1"
  local ts
  ts="$(now_epoch)"
  json_put "runtime.json" '.[$field] = $ts' --arg field "$field" --argjson ts "$ts"
}

prune_alert_dedupe() {
  local cutoff
  cutoff=$(( $(now_epoch) - (AZMON_ALERT_COOLDOWN_SEC * 4) ))
  json_put "alert_dedupe.json" 'with_entries(select(.value >= $cutoff))' --argjson cutoff "$cutoff"
}

tg_send() {
  local message="$1"
  if is_true "$AZMON_DRY_RUN"; then
    log_info "[dry-run][telegram] ${message//$'\n'/ | }"
    return 0
  fi

  local url="https://api.telegram.org/bot${AZMON_TELEGRAM_BOT_TOKEN}/sendMessage"
  local -a args=(
    --silent
    --show-error
    --fail
    -X POST
    "$url"
    --data-urlencode "chat_id=$AZMON_TELEGRAM_CHAT_ID"
    --data-urlencode "text=$message"
    --data-urlencode "parse_mode=HTML"
    --data-urlencode "disable_web_page_preview=true"
  )

  if [[ -n "$AZMON_TELEGRAM_THREAD_ID" ]]; then
    args+=(--data-urlencode "message_thread_id=$AZMON_TELEGRAM_THREAD_ID")
  fi

  curl "${args[@]}" >/dev/null
}

tg_send_once() {
  local category="$1"
  local unique_key="$2"
  local message="$3"
  local dedupe_key last_sent now
  dedupe_key="${category}:$(hash_text "$unique_key")"
  now="$(now_epoch)"
  last_sent="$(json_get "alert_dedupe.json" '.[$key] // 0' --arg key "$dedupe_key")"
  if (( now - last_sent < AZMON_ALERT_COOLDOWN_SEC )); then
    return 0
  fi

  if tg_send "$message"; then
    json_put "alert_dedupe.json" '.[$key] = $now' --arg key "$dedupe_key" --argjson now "$now"
    return 0
  fi

  log_error "telegram send failed for category=$category"
  return 1
}

notify_runtime_error() {
  local key="$1"
  local message="$2"
  local tx_hash="${3:-}"
  local log_message
  local -a lines=(
    "Runtime error"
    "$(html_field_line "component" "$key")"
    "$(html_field_line "error" "$message")"
  )

  if [[ -n "$tx_hash" ]]; then
    log_message="$message for tx $tx_hash"
    lines+=("$(html_field_line_html "tx" "$(tx_hash_html "$tx_hash")")")
  else
    log_message="$message"
  fi

  log_error "$log_message"
  tg_send_once "runtime-error" "$key" "$(join_lines "${lines[@]}")" || true
}

notify_container_unhealthy() {
  local detail="$1"
  log_error "$detail"
  tg_send_once "container-unhealthy" "$AZMON_DOCKER_CONTAINER" "$(join_lines \
    "Container unhealthy" \
    "$(html_field_line "container" "$AZMON_DOCKER_CONTAINER")" \
    "$(html_field_line "detail" "$detail")"
  )" || true
}

rpc_call() {
  local method="$1"
  shift
  cast rpc --rpc-url "$AZMON_RPC_URL" "$method" "$@"
}

rpc_call_raw() {
  local method="$1"
  local params="$2"
  cast rpc --rpc-url "$AZMON_RPC_URL" "$method" --raw "$params"
}

cast_call() {
  local signature="$1"
  shift
  cast call --json --rpc-url "$AZMON_RPC_URL" "$AZMON_ROLLUP_ADDRESS" "$signature" "$@" | jq -r '.[0]'
}

cast_logs() {
  local from_block="$1"
  local to_block="$2"
  local signature="$3"
  cast logs --json --rpc-url "$AZMON_RPC_URL" --address "$AZMON_ROLLUP_ADDRESS" --from-block "$from_block" --to-block "$to_block" "$signature"
}

trace_tx() {
  local tx_hash="$1"
  rpc_call_raw "debug_traceTransaction" "[\"$tx_hash\", {\"tracer\":\"callTracer\"}]"
}

address_label() {
  local address label
  address="$(normalize_address "$1")" || {
    printf '%s\n' "$1"
    return 0
  }
  label="${SEQUENCER_LABELS[$address]:-}"
  if [[ -n "$label" ]]; then
    printf '%s (%s)\n' "$label" "$(short_hex "$address")"
  else
    printf '%s\n' "$(short_hex "$address")"
  fi
}

address_label_html() {
  local address label short_link
  address="$(normalize_address "$1")" || {
    html_escape "$1"
    return 0
  }
  label="${SEQUENCER_LABELS[$address]:-}"
  short_link="$(html_link "https://dashtec.xyz/sequencers/$address" "$(short_hex "$address")")"
  if [[ -n "$label" ]]; then
    printf '%s (%s)\n' "$(html_escape "$label")" "$short_link"
  else
    printf '%s\n' "$short_link"
  fi
}

tx_hash_html() {
  local tx_hash="${1,,}"
  if [[ "$tx_hash" =~ ^0x[0-9a-f]{64}$ ]]; then
    html_link "https://etherscan.io/tx/$tx_hash" "$(short_hex "$tx_hash")"
  else
    html_escape "$1"
  fi
}

hex_word_at() {
  local data index raw
  raw="${1#0x}"
  index="$2"
  printf '%s\n' "${raw:$((index * 64)):64}"
}

hex_to_dec() {
  cast to-dec "0x$1"
}

rollup_eth_call() {
  local signature calldata params output
  signature="$1"
  shift
  calldata="$(cast calldata "$signature" "$@")"
  params="$(printf '[{"to":"%s","data":"%s"},"latest"]' "$AZMON_ROLLUP_ADDRESS" "$calldata")"
  output="$(rpc_call_raw "eth_call" "$params")"
  jq -r '.' <<<"$output"
}

decode_uint256_output() {
  hex_to_dec "$(hex_word_at "$1" 0)"
}

decode_address_output() {
  local word
  word="$(hex_word_at "$1" 0)"
  normalize_address "0x${word:24:40}"
}

decode_address_array_output() {
  local output offset_words length index word
  output="$1"
  offset_words=$(( $(hex_to_dec "$(hex_word_at "$output" 0)") / 32 ))
  length="$(hex_to_dec "$(hex_word_at "$output" "$offset_words")")"
  for ((index = 0; index < length; index++)); do
    word="$(hex_word_at "$output" $((offset_words + 1 + index)))"
    normalize_address "0x${word:24:40}"
  done
}

decode_checkpoint_slot_output() {
  hex_to_dec "$(hex_word_at "$1" 6)"
}

get_current_epoch() {
  decode_uint256_output "$(rollup_eth_call 'getCurrentEpoch()')"
}

get_genesis_time() {
  decode_uint256_output "$(rollup_eth_call 'getGenesisTime()')"
}

get_slot_duration() {
  decode_uint256_output "$(rollup_eth_call 'getSlotDuration()')"
}

get_epoch_duration() {
  decode_uint256_output "$(rollup_eth_call 'getEpochDuration()')"
}

get_lag_validator_set() {
  decode_uint256_output "$(rollup_eth_call 'getLagInEpochsForValidatorSet()')"
}

get_lag_randao() {
  decode_uint256_output "$(rollup_eth_call 'getLagInEpochsForRandao()')"
}

get_timestamp_for_slot() {
  decode_uint256_output "$(rollup_eth_call 'getTimestampForSlot(uint256)' "$1")"
}

get_epoch_at_slot() {
  decode_uint256_output "$(rollup_eth_call 'getEpochAtSlot(uint256)' "$1")"
}

get_epoch_committee() {
  decode_address_array_output "$(rollup_eth_call 'getEpochCommittee(uint256)' "$1")"
}

get_proposer_at() {
  decode_address_output "$(rollup_eth_call 'getProposerAt(uint256)' "$1")"
}

get_checkpoint_slot() {
  decode_checkpoint_slot_output "$(rollup_eth_call 'getCheckpoint(uint256)' "$1")"
}

get_latest_block() {
  cast block-number --rpc-url "$AZMON_RPC_URL"
}

decode_rollup_propose_signers() {
  local calldata="$1"
  cast decode-calldata --json "$ROLLUP_PROPOSE_SIG" "$calldata" | jq -r '.[2][] | ascii_downcase'
}

find_nested_rollup_propose_input() {
  local trace_json="$1"
  jq -r --arg rollup "${AZMON_ROLLUP_ADDRESS,,}" --arg selector "$PROPOSE_SELECTOR" \
    '.. | objects | select((.to? // "" | ascii_downcase) == $rollup and (.input? // "" | startswith($selector))) | .input' \
    <<<"$trace_json" | head -n1
}

monitored_for_epoch() {
  local epoch="$1"
  local member
  while IFS= read -r member; do
    if [[ -n "${MONITORED_SET[$member]:-}" ]]; then
      printf '%s\n' "$member"
    fi
  done < <(get_epoch_committee "$epoch")
}

json_array_from_lines() {
  if [[ $# -eq 0 ]]; then
    jq -nc '[]'
  else
    printf '%s\n' "$@" | jq -Rcs 'split("\n") | map(select(length > 0))'
  fi
}

log_level_enabled() {
  local level="$1"
  [[ -n "${LOG_ALERT_LEVELS[$level]:-}" ]]
}

parse_monitored_addresses() {
  local raw normalized
  while IFS= read -r raw; do
    normalized="$(normalize_address "$raw")" || die "invalid monitored sequencer address: $raw"
    if [[ -z "${MONITORED_SET[$normalized]:-}" ]]; then
      MONITORED_SET["$normalized"]=1
      MONITORED_ADDRESSES+=("$normalized")
    fi
  done < <(split_csv "$AZMON_MONITORED_SEQUENCERS")

  ((${#MONITORED_ADDRESSES[@]} > 0)) || die "AZMON_MONITORED_SEQUENCERS must contain at least one address"
}

parse_sequencer_labels() {
  local entry label raw_address normalized
  [[ -n "$AZMON_SEQUENCER_LABELS" ]] || return 0
  while IFS= read -r entry; do
    label="${entry%%=*}"
    raw_address="${entry#*=}"
    [[ "$label" != "$entry" ]] || die "invalid AZMON_SEQUENCER_LABELS entry: $entry"
    normalized="$(normalize_address "$raw_address")" || die "invalid label address: $raw_address"
    SEQUENCER_LABELS["$normalized"]="$label"
  done < <(split_csv "$AZMON_SEQUENCER_LABELS")
}

parse_log_levels() {
  local level
  while IFS= read -r level; do
    LOG_ALERT_LEVELS["${level,,}"]=1
  done < <(split_csv "$AZMON_LOG_ALERT_LEVELS")
}

ensure_state_layout() {
  local file
  mkdir -p "$AZMON_STATE_DIR"
  json_init "duty_cache.json" '{}'
  json_init "completion_cache.json" '{}'
  json_init "alert_dedupe.json" '{}'
  json_init "runtime.json" '{}'
  file="$(state_file "docker_cursor.txt")"
  [[ -f "$file" ]] || : >"$file"
  file="$(state_file "l1_block_cursor.txt")"
  [[ -f "$file" ]] || : >"$file"
}

validate_env() {
  [[ -n "${AZMON_TELEGRAM_BOT_TOKEN:-}" ]] || die "AZMON_TELEGRAM_BOT_TOKEN is required"
  [[ -n "${AZMON_TELEGRAM_CHAT_ID:-}" ]] || die "AZMON_TELEGRAM_CHAT_ID is required"
  [[ -n "${AZMON_MONITORED_SEQUENCERS:-}" ]] || die "AZMON_MONITORED_SEQUENCERS is required"

  require_uint "AZMON_POLL_INTERVAL_SEC" "$AZMON_POLL_INTERVAL_SEC"
  require_uint "AZMON_LOG_LOOKBACK_SEC" "$AZMON_LOG_LOOKBACK_SEC"
  require_uint "AZMON_PROPOSAL_GRACE_SEC" "$AZMON_PROPOSAL_GRACE_SEC"
  require_uint "AZMON_BLOCK_CONFIRMATIONS" "$AZMON_BLOCK_CONFIRMATIONS"
  require_uint "AZMON_ALERT_COOLDOWN_SEC" "$AZMON_ALERT_COOLDOWN_SEC"
}

check_dependencies() {
  require_cmd docker
  require_cmd curl
  require_cmd jq
  require_cmd cast
  require_cmd sha256sum
}

load_runtime_config() {
  validate_env
  check_dependencies
  AZMON_ROLLUP_ADDRESS="$(normalize_address "$AZMON_ROLLUP_ADDRESS")" || die "invalid AZMON_ROLLUP_ADDRESS"
  parse_monitored_addresses
  parse_sequencer_labels
  parse_log_levels
  ensure_state_layout
}

ensure_attester_duty() {
  local epoch="$1"
  local address="$2"
  local start_slot="$3"
  local end_slot="$4"
  local start_time="$5"
  local end_time="$6"
  local key discovered_at label already_exists=0
  key="attester:${epoch}:${address}"
  if json_has "duty_cache.json" "$key"; then
    already_exists=1
  fi

  discovered_at="$(now_epoch)"
  label="$(address_label "$address")"
  json_put "duty_cache.json" \
    'if has($key) then
       .[$key].type = "attester" |
       .[$key].epoch = $epoch |
       .[$key].start_slot = $start_slot |
       .[$key].end_slot = $end_slot |
       .[$key].start_time = $start_time |
       .[$key].end_time = $end_time |
       .[$key].address = $address |
       .[$key].label = $label
     else
       .[$key] = {
         type:"attester",
         epoch:$epoch,
         start_slot:$start_slot,
         end_slot:$end_slot,
         start_time:$start_time,
         end_time:$end_time,
         address:$address,
         label:$label,
         discovered_at:$discovered_at
       }
     end' \
    --arg key "$key" \
    --argjson epoch "$epoch" \
    --argjson start_slot "$start_slot" \
    --argjson end_slot "$end_slot" \
    --argjson start_time "$start_time" \
    --argjson end_time "$end_time" \
    --arg address "$address" \
    --arg label "$label" \
    --argjson discovered_at "$discovered_at"

  if (( ! already_exists )) && is_true "$AZMON_DUTY_INFO"; then
    tg_send_once "duty-info" "$key" "$(join_lines \
      "Duty info" \
      "$(html_field_line "kind" "attester")" \
      "$(html_field_line_html "sequencer" "$(address_label_html "$address")")" \
      "$(html_field_line "epoch" "$epoch")" \
      "$(html_field_line "epoch_start" "$(format_utc "$start_time")")" \
      "$(html_field_line "epoch_end" "$(format_utc "$end_time")")"
    )" || true
  fi
}

ensure_proposer_duty() {
  local epoch="$1"
  local slot="$2"
  local slot_time="$3"
  local address="$4"
  local key discovered_at label
  key="proposer:${slot}:${address}"
  if json_has "duty_cache.json" "$key"; then
    return 0
  fi

  discovered_at="$(now_epoch)"
  label="$(address_label "$address")"
  json_put "duty_cache.json" \
    '.[$key] = {type:"proposer", epoch:$epoch, slot:$slot, slot_time:$slot_time, address:$address, label:$label, discovered_at:$discovered_at, status:"scheduled"}' \
    --arg key "$key" \
    --argjson epoch "$epoch" \
    --argjson slot "$slot" \
    --argjson slot_time "$slot_time" \
    --arg address "$address" \
    --arg label "$label" \
    --argjson discovered_at "$discovered_at"

  if is_true "$AZMON_DUTY_INFO"; then
    tg_send_once "duty-info" "$key" "$(join_lines \
      "Duty info" \
      "$(html_field_line "kind" "proposer")" \
      "$(html_field_line_html "sequencer" "$(address_label_html "$address")")" \
      "$(html_field_line "epoch" "$epoch")" \
      "$(html_field_line "slot" "$slot")" \
      "$(html_field_line "slot_time" "$(format_utc "$slot_time")")"
    )" || true
  fi
}

mark_proposer_status() {
  local slot="$1"
  local address="$2"
  local status="$3"
  local checkpoint="$4"
  local tx_hash="$5"
  local key
  key="proposer:${slot}:${address}"
  if ! json_has "duty_cache.json" "$key"; then
    return 0
  fi

  json_put "duty_cache.json" \
    'if has($key) then .[$key].status = $status | .[$key].checkpoint = $checkpoint | .[$key].tx_hash = $tx_hash | .[$key].updated_at = $now else . end' \
    --arg key "$key" \
    --arg status "$status" \
    --arg checkpoint "$checkpoint" \
    --arg tx_hash "$tx_hash" \
    --argjson now "$(now_epoch)"
}

poll_container_health() {
  local running
  if ! running="$(docker inspect --format '{{.State.Running}}' "$AZMON_DOCKER_CONTAINER" 2>&1)"; then
    notify_container_unhealthy "docker inspect failed: $running"
    return 1
  fi

  if [[ "$running" != "true" ]]; then
    notify_container_unhealthy "container is not running"
    return 1
  fi

  runtime_mark "last_health_check"
  return 0
}

poll_docker_logs() {
  local since cursor logs_output logs_error tmp_out tmp_err
  local line ts app_line severity last_ts next_cursor line_key

  cursor="$(state_get "docker_cursor.txt" || true)"
  if [[ -n "$cursor" ]]; then
    since="$cursor"
  else
    since="${AZMON_LOG_LOOKBACK_SEC}s"
  fi

  tmp_out="$(mktemp "$AZMON_STATE_DIR/.docker-logs.XXXXXX")"
  tmp_err="$(mktemp "$AZMON_STATE_DIR/.docker-logs.XXXXXX")"
  if ! docker logs --timestamps --since "$since" "$AZMON_DOCKER_CONTAINER" >"$tmp_out" 2>"$tmp_err"; then
    logs_error="$(<"$tmp_err")"
    rm -f "$tmp_out" "$tmp_err"
    notify_container_unhealthy "docker logs failed: ${logs_error:-unknown error}"
    return 1
  fi

  logs_output="$(<"$tmp_out")"
  rm -f "$tmp_out" "$tmp_err"
  last_ts=""

  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    ts="${line%% *}"
    app_line="${line#* }"
    last_ts="$ts"

    if [[ -n "$AZMON_LOG_INCLUDE_REGEX" ]] && ! [[ "$app_line" =~ $AZMON_LOG_INCLUDE_REGEX ]]; then
      continue
    fi
    if [[ -n "$AZMON_LOG_EXCLUDE_REGEX" ]] && [[ "$app_line" =~ $AZMON_LOG_EXCLUDE_REGEX ]]; then
      continue
    fi

    severity="unknown"
    if [[ "$app_line" =~ (^|[[:space:]])(TRACE|DEBUG|INFO|WARN|ERROR|FATAL): ]]; then
      severity="${BASH_REMATCH[2],,}"
    fi

    if ! log_level_enabled "$severity"; then
      continue
    fi

    line_key="$(hash_text "${severity}|${app_line}")"
    tg_send_once "log" "$line_key" "$(join_lines \
      "Log alert" \
      "$(html_field_line "severity" "$severity")" \
      "$(html_field_line "container" "$AZMON_DOCKER_CONTAINER")" \
      "$(html_field_line "line" "$app_line")"
    )" || true
  done <<<"$logs_output"

  if [[ -n "$last_ts" ]]; then
    state_put "docker_cursor.txt" "$last_ts"
  else
    next_cursor="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    state_put "docker_cursor.txt" "$next_cursor"
  fi

  runtime_mark "last_docker_log_poll"
}

poll_upcoming_duties() {
  local current_epoch validator_lag randao_lag epoch_duration slot_duration stable_epoch_limit min_lag
  local epoch slot_start slot_end epoch_start_time epoch_end_time slot slot_time proposer
  local address

  if ! current_epoch="$(get_current_epoch 2>/dev/null)"; then
    notify_runtime_error "getCurrentEpoch" "failed to fetch current epoch"
    return 1
  fi
  if ! validator_lag="$(get_lag_validator_set 2>/dev/null)"; then
    notify_runtime_error "getLagInEpochsForValidatorSet" "failed to fetch validator-set lag"
    return 1
  fi
  if ! randao_lag="$(get_lag_randao 2>/dev/null)"; then
    notify_runtime_error "getLagInEpochsForRandao" "failed to fetch randao lag"
    return 1
  fi
  if ! epoch_duration="$(get_epoch_duration 2>/dev/null)"; then
    notify_runtime_error "getEpochDuration" "failed to fetch epoch duration"
    return 1
  fi
  if ! slot_duration="$(get_slot_duration 2>/dev/null)"; then
    notify_runtime_error "getSlotDuration" "failed to fetch slot duration"
    return 1
  fi

  if (( validator_lag < randao_lag )); then
    min_lag="$validator_lag"
  else
    min_lag="$randao_lag"
  fi
  stable_epoch_limit=$(( current_epoch + min_lag ))

  for ((epoch = current_epoch; epoch <= stable_epoch_limit; epoch++)); do
    slot_start=$(( epoch * epoch_duration ))
    slot_end=$(( slot_start + epoch_duration - 1 ))
    if ! epoch_start_time="$(get_timestamp_for_slot "$slot_start" 2>/dev/null)"; then
      notify_runtime_error "getTimestampForSlot" "failed to fetch timestamp for epoch $epoch start slot $slot_start"
      return 1
    fi
    epoch_end_time=$(( epoch_start_time + (epoch_duration * slot_duration) - 1 ))

    while IFS= read -r address; do
      [[ -n "$address" ]] || continue
      ensure_attester_duty "$epoch" "$address" "$slot_start" "$slot_end" "$epoch_start_time" "$epoch_end_time"
    done < <(monitored_for_epoch "$epoch" 2>/dev/null || true)

    for ((slot = slot_start; slot <= slot_end; slot++)); do
      if ! slot_time="$(get_timestamp_for_slot "$slot" 2>/dev/null)"; then
        notify_runtime_error "getTimestampForSlot" "failed to fetch timestamp for slot $slot"
        return 1
      fi
      if (( slot_time < $(now_epoch) )); then
        continue
      fi
      if ! proposer="$(get_proposer_at "$slot_time" 2>/dev/null)"; then
        notify_runtime_error "getProposerAt" "failed to fetch proposer for slot $slot"
        return 1
      fi
      if [[ -n "${MONITORED_SET[$proposer]:-}" ]]; then
        ensure_proposer_duty "$epoch" "$slot" "$slot_time" "$proposer"
      fi
    done
  done

  runtime_mark "last_duty_poll"
}

record_checkpoint() {
  local checkpoint="$1"
  local slot="$2"
  local epoch="$3"
  local slot_time="$4"
  local tx_hash="$5"
  local proposer="$6"
  local signers_status="$7"
  local signers_json="$8"
  local now
  now="$(now_epoch)"

  json_put "completion_cache.json" \
    '.[$key] = {type:"checkpoint", checkpoint:$checkpoint, slot:$slot, epoch:$epoch, slot_time:$slot_time, tx_hash:$tx_hash, proposer:$proposer, signers_status:$signers_status, signers:$signers, invalidated:false, updated_at:$now}' \
    --arg key "checkpoint:$checkpoint" \
    --arg checkpoint "$checkpoint" \
    --argjson slot "$slot" \
    --argjson epoch "$epoch" \
    --argjson slot_time "$slot_time" \
    --arg tx_hash "$tx_hash" \
    --arg proposer "$proposer" \
    --arg signers_status "$signers_status" \
    --argjson signers "$signers_json" \
    --argjson now "$now"

  json_put "completion_cache.json" \
    '.[$key] = {type:"proposal-slot", slot:$slot, checkpoint:$checkpoint, proposer:$proposer, tx_hash:$tx_hash, invalidated:false, updated_at:$now}' \
    --arg key "proposal-slot:$slot" \
    --argjson slot "$slot" \
    --arg checkpoint "$checkpoint" \
    --arg proposer "$proposer" \
    --arg tx_hash "$tx_hash" \
    --argjson now "$now"
}

record_attestation_result() {
  local checkpoint="$1"
  local epoch="$2"
  local address="$3"
  local status="$4"
  local tx_hash="$5"
  local now key included=false
  if [[ "$status" == "completed" ]]; then
    included=true
  fi
  now="$(now_epoch)"
  key="attestation:${checkpoint}:${address}"
  json_put "completion_cache.json" \
    'if has($key) then
       .[$key].type = "attestation" |
       .[$key].checkpoint = $checkpoint |
       .[$key].epoch = $epoch |
       .[$key].address = $address |
       .[$key].status = $status |
       .[$key].included = $included |
       .[$key].tx_hash = $tx_hash |
       .[$key].invalidated = (.[$key].invalidated // false) |
       .[$key].updated_at = $now
     else
       .[$key] = {
         type:"attestation",
         checkpoint:$checkpoint,
         epoch:$epoch,
         address:$address,
         status:$status,
         included:$included,
         tx_hash:$tx_hash,
         invalidated:false,
         updated_at:$now
       }
     end' \
    --arg key "$key" \
    --arg checkpoint "$checkpoint" \
    --argjson epoch "$epoch" \
    --arg address "$address" \
    --arg status "$status" \
    --argjson included "$included" \
    --arg tx_hash "$tx_hash" \
    --argjson now "$now"
}

process_attestations_for_checkpoint() {
  local checkpoint="$1"
  local epoch="$2"
  local tx_hash="$3"
  local signers_status="$4"
  local signers_json="$5"
  local -A signer_set=()
  local address status

  if [[ "$signers_status" == "decoded" ]]; then
    while IFS= read -r address; do
      [[ -n "$address" ]] || continue
      signer_set["$address"]=1
    done < <(jq -r '.[]' <<<"$signers_json")
  fi

  while IFS= read -r address; do
    [[ -n "$address" ]] || continue
    if [[ "$signers_status" == "decoded" ]]; then
      if [[ -n "${signer_set[$address]:-}" ]]; then
        status="completed"
        if is_true "$AZMON_DUTY_INFO"; then
          tg_send_once "attestation-completed" "${checkpoint}:${address}" "$(join_lines \
            "Attestation completed" \
            "$(html_field_line_html "sequencer" "$(address_label_html "$address")")" \
            "$(html_field_line "epoch" "$epoch")" \
            "$(html_field_line "checkpoint" "$checkpoint")" \
            "$(html_field_line_html "tx" "$(tx_hash_html "$tx_hash")")"
          )" || true
        fi
      else
        # Proposal calldata only carries the attestations needed for quorum.
        status="not-included"
      fi
    else
      status="unknown"
    fi
    record_attestation_result "$checkpoint" "$epoch" "$address" "$status" "$tx_hash"
  done < <(monitored_for_epoch "$epoch" 2>/dev/null || true)
}

mark_attester_epoch_result() {
  local epoch="$1"
  local address="$2"
  local observed="$3"
  local included="$4"
  local not_included="$5"
  local unknown="$6"
  local key finalized_at
  key="attester:${epoch}:${address}"
  finalized_at="$(now_epoch)"

  json_put "duty_cache.json" \
    'if has($key) then
       .[$key].observed_attestations = $observed |
       .[$key].included_attestations = $included |
       .[$key].not_included_attestations = $not_included |
       .[$key].unknown_attestations = $unknown |
       .[$key].epoch_result_finalized_at = $finalized_at
     else
       .
     end' \
    --arg key "$key" \
    --argjson observed "$observed" \
    --argjson included "$included" \
    --argjson not_included "$not_included" \
    --argjson unknown "$unknown" \
    --argjson finalized_at "$finalized_at"
}

check_attester_epoch_results() {
  local entry epoch address end_time finalized_at
  local stats_json observed included not_included unknown

  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    epoch="$(jq -r '.value.epoch' <<<"$entry")"
    address="$(jq -r '.value.address' <<<"$entry")"
    end_time="$(jq -r '.value.end_time // empty' <<<"$entry")"
    finalized_at="$(jq -r '.value.epoch_result_finalized_at // empty' <<<"$entry")"

    if [[ -n "$finalized_at" ]]; then
      continue
    fi
    if [[ -z "$end_time" || "$end_time" == "null" ]]; then
      continue
    fi
    if (( $(now_epoch) < end_time + AZMON_PROPOSAL_GRACE_SEC )); then
      continue
    fi

    stats_json="$(json_get "completion_cache.json" '
      {
        observed: ([to_entries[]
          | select(.value.type == "attestation" and .value.invalidated != true and .value.epoch == $epoch and .value.address == $address)
        ] | length),
        included: ([to_entries[]
          | select(.value.type == "attestation" and .value.invalidated != true and .value.epoch == $epoch and .value.address == $address)
          | select((.value.included // false) or .value.status == "completed")
        ] | length),
        not_included: ([to_entries[]
          | select(.value.type == "attestation" and .value.invalidated != true and .value.epoch == $epoch and .value.address == $address)
          | select(((.value.included // false) != true) and (.value.status == "not-included" or .value.status == "missed"))
        ] | length),
        unknown: ([to_entries[]
          | select(.value.type == "attestation" and .value.invalidated != true and .value.epoch == $epoch and .value.address == $address and .value.status == "unknown")
        ] | length)
      }' \
      --argjson epoch "$epoch" \
      --arg address "$address")"

    observed="$(jq -r '.observed' <<<"$stats_json")"
    included="$(jq -r '.included' <<<"$stats_json")"
    not_included="$(jq -r '.not_included' <<<"$stats_json")"
    unknown="$(jq -r '.unknown' <<<"$stats_json")"

    if (( observed == 0 || unknown > 0 )); then
      continue
    fi

    mark_attester_epoch_result "$epoch" "$address" "$observed" "$included" "$not_included" "$unknown"

    if (( included == 0 )); then
      tg_send_once "attestation-missed-epoch" "${epoch}:${address}" "$(join_lines \
        "Attestation missed" \
        "$(html_field_line_html "sequencer" "$(address_label_html "$address")")" \
        "$(html_field_line "epoch" "$epoch")" \
        "$(html_field_line "attested" "$included/$observed")"
      )" || true
    fi

    if is_true "$AZMON_DUTY_INFO"; then
      tg_send_once "attestation-stats" "${epoch}:${address}" "$(join_lines \
        "Attestation stats" \
        "$(html_field_line_html "sequencer" "$(address_label_html "$address")")" \
        "$(html_field_line "epoch" "$epoch")" \
        "$(html_field_line "attested" "$included/$observed")" \
        "$(html_field_line "not_included" "$not_included")"
      )" || true
    fi
  done < <(json_get "duty_cache.json" 'to_entries[] | select(.value.type == "attester")')
}

try_decode_checkpoint_signers() {
  local checkpoint="$1"
  local epoch="$2"
  local tx_hash="$3"
  local slot="$4"
  local slot_time="$5"
  local proposer="$6"
  local trace_json propose_input signers_json
  local -a signers=()

  if ! trace_json="$(trace_tx "$tx_hash" 2>/dev/null)"; then
    notify_runtime_error "debug_traceTransaction" "trace failed" "$tx_hash"
    record_checkpoint "$checkpoint" "$slot" "$epoch" "$slot_time" "$tx_hash" "$proposer" "unknown" '[]'
    process_attestations_for_checkpoint "$checkpoint" "$epoch" "$tx_hash" "unknown" '[]'
    return 1
  fi

  propose_input="$(find_nested_rollup_propose_input "$trace_json")"
  if [[ -z "$propose_input" ]]; then
    notify_runtime_error "decode_rollup_propose_signers" "nested propose call not found in trace" "$tx_hash"
    record_checkpoint "$checkpoint" "$slot" "$epoch" "$slot_time" "$tx_hash" "$proposer" "unknown" '[]'
    process_attestations_for_checkpoint "$checkpoint" "$epoch" "$tx_hash" "unknown" '[]'
    return 1
  fi

  while IFS= read -r address; do
    [[ -n "$address" ]] || continue
    signers+=("$address")
  done < <(decode_rollup_propose_signers "$propose_input" 2>/dev/null || true)

  signers_json="$(json_array_from_lines "${signers[@]}")"
  record_checkpoint "$checkpoint" "$slot" "$epoch" "$slot_time" "$tx_hash" "$proposer" "decoded" "$signers_json"
  process_attestations_for_checkpoint "$checkpoint" "$epoch" "$tx_hash" "decoded" "$signers_json"
}

process_checkpoint_proposed_log() {
  local log_json="$1"
  local checkpoint_hex checkpoint slot slot_time epoch proposer tx_hash

  checkpoint_hex="$(jq -r '.topics[1]' <<<"$log_json")"
  checkpoint="$((16#${checkpoint_hex#0x}))"
  tx_hash="$(jq -r '.transactionHash' <<<"$log_json")"

  if ! slot="$(get_checkpoint_slot "$checkpoint" 2>/dev/null)"; then
    notify_runtime_error "getCheckpoint" "failed to fetch checkpoint $checkpoint"
    return 1
  fi
  if ! slot_time="$(get_timestamp_for_slot "$slot" 2>/dev/null)"; then
    notify_runtime_error "getTimestampForSlot" "failed to fetch timestamp for slot $slot"
    return 1
  fi
  if ! epoch="$(get_epoch_at_slot "$slot" 2>/dev/null)"; then
    notify_runtime_error "getEpochAtSlot" "failed to fetch epoch for slot $slot"
    return 1
  fi
  if ! proposer="$(get_proposer_at "$slot_time" 2>/dev/null)"; then
    notify_runtime_error "getProposerAt" "failed to fetch proposer for slot $slot"
    return 1
  fi

  if [[ -n "${MONITORED_SET[$proposer]:-}" ]]; then
    ensure_proposer_duty "$epoch" "$slot" "$slot_time" "$proposer"
    mark_proposer_status "$slot" "$proposer" "completed" "$checkpoint" "$tx_hash"
    if is_true "$AZMON_DUTY_INFO"; then
      tg_send_once "proposal-completed" "${slot}:${proposer}" "$(join_lines \
        "Proposal completed" \
        "$(html_field_line_html "sequencer" "$(address_label_html "$proposer")")" \
        "$(html_field_line "epoch" "$epoch")" \
        "$(html_field_line "slot" "$slot")" \
        "$(html_field_line "checkpoint" "$checkpoint")" \
        "$(html_field_line_html "tx" "$(tx_hash_html "$tx_hash")")"
      )" || true
    fi
  fi

  try_decode_checkpoint_signers "$checkpoint" "$epoch" "$tx_hash" "$slot" "$slot_time" "$proposer" || true
}

process_checkpoint_invalidated_log() {
  local log_json="$1"
  local checkpoint_hex checkpoint slot proposer
  checkpoint_hex="$(jq -r '.topics[1]' <<<"$log_json")"
  checkpoint="$((16#${checkpoint_hex#0x}))"

  slot="$(json_get "completion_cache.json" '.[$key].slot // empty' --arg key "checkpoint:$checkpoint")"
  proposer="$(json_get "completion_cache.json" '.[$key].proposer // empty' --arg key "checkpoint:$checkpoint")"

  json_put "completion_cache.json" 'if has($key) then .[$key].invalidated = true | .[$key].updated_at = $now else . end' \
    --arg key "checkpoint:$checkpoint" \
    --argjson now "$(now_epoch)"

  if [[ -n "$slot" ]]; then
    json_put "completion_cache.json" 'if has($key) then .[$key].invalidated = true | .[$key].updated_at = $now else . end' \
      --arg key "proposal-slot:$slot" \
      --argjson now "$(now_epoch)"
  fi

  json_put "completion_cache.json" \
    'with_entries(
      if (.key | startswith($prefix)) then
        .value.included = (.value.included // (.value.status == "completed")) |
        .value.invalidated = true |
        .value.updated_at = $now
      else
        .
      end
    )' \
    --arg prefix "attestation:${checkpoint}:" \
    --argjson now "$(now_epoch)"

  if [[ -n "$slot" && -n "$proposer" ]]; then
    mark_proposer_status "$slot" "$proposer" "scheduled" "" ""
    tg_send_once "runtime-error" "checkpoint-invalidated:${checkpoint}" "$(join_lines \
      "Checkpoint invalidated" \
      "$(html_field_line "checkpoint" "$checkpoint")" \
      "$(html_field_line "slot" "$slot")" \
      "$(html_field_line_html "sequencer" "$(address_label_html "$proposer")")"
    )" || true
  else
    tg_send_once "runtime-error" "checkpoint-invalidated:${checkpoint}" "$(join_lines \
      "Checkpoint invalidated" \
      "$(html_field_line "checkpoint" "$checkpoint")"
    )" || true
  fi
}

retry_unknown_checkpoints() {
  local entry checkpoint slot epoch slot_time tx_hash proposer
  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    checkpoint="$(jq -r '.key | sub("^checkpoint:"; "")' <<<"$entry")"
    slot="$(jq -r '.value.slot' <<<"$entry")"
    epoch="$(jq -r '.value.epoch' <<<"$entry")"
    slot_time="$(jq -r '.value.slot_time' <<<"$entry")"
    tx_hash="$(jq -r '.value.tx_hash' <<<"$entry")"
    proposer="$(jq -r '.value.proposer' <<<"$entry")"
    try_decode_checkpoint_signers "$checkpoint" "$epoch" "$tx_hash" "$slot" "$slot_time" "$proposer" || true
  done < <(json_get "completion_cache.json" 'to_entries[] | select(.key | startswith("checkpoint:")) | select(.value.invalidated != true and .value.signers_status == "unknown")')
}

check_for_missed_proposals() {
  local entry slot slot_time address status checkpoint
  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    slot="$(jq -r '.value.slot' <<<"$entry")"
    slot_time="$(jq -r '.value.slot_time' <<<"$entry")"
    address="$(jq -r '.value.address' <<<"$entry")"
    status="$(jq -r '.value.status' <<<"$entry")"

    if [[ "$status" == "completed" ]]; then
      continue
    fi
    checkpoint="$(json_get "completion_cache.json" '.[$key] | select(.invalidated != true) | .checkpoint // empty' --arg key "proposal-slot:$slot")"
    if [[ -n "$checkpoint" ]]; then
      continue
    fi
    if (( $(now_epoch) < slot_time + AZMON_PROPOSAL_GRACE_SEC )); then
      continue
    fi

    mark_proposer_status "$slot" "$address" "missed" "" ""
    tg_send_once "proposal-missed" "${slot}:${address}" "$(join_lines \
      "Proposal missed" \
      "$(html_field_line_html "sequencer" "$(address_label_html "$address")")" \
      "$(html_field_line "slot" "$slot")" \
      "$(html_field_line "slot_time" "$(format_utc "$slot_time")")"
    )" || true
  done < <(json_get "duty_cache.json" 'to_entries[] | select(.value.type == "proposer")')
}

poll_l1_completions() {
  local latest_block confirmed_block cursor from_block proposed_logs invalidated_logs
  local entry

  if ! latest_block="$(get_latest_block 2>/dev/null)"; then
    notify_runtime_error "eth_blockNumber" "failed to fetch latest block number"
    return 1
  fi
  if (( latest_block <= AZMON_BLOCK_CONFIRMATIONS )); then
    return 0
  fi
  confirmed_block=$(( latest_block - AZMON_BLOCK_CONFIRMATIONS ))

  cursor="$(state_get "l1_block_cursor.txt" || true)"
  if [[ -z "$cursor" ]]; then
    state_put "l1_block_cursor.txt" "$confirmed_block"
    runtime_mark "last_l1_poll"
    return 0
  fi

  from_block=$(( cursor + 1 ))
  if (( from_block > confirmed_block )); then
    retry_unknown_checkpoints
    check_for_missed_proposals
    runtime_mark "last_l1_poll"
    return 0
  fi

  if ! proposed_logs="$(cast_logs "$from_block" "$confirmed_block" "$CHECKPOINT_PROPOSED_EVENT" 2>/dev/null)"; then
    notify_runtime_error "cast_logs:CheckpointProposed" "failed to fetch CheckpointProposed logs from $from_block to $confirmed_block"
    return 1
  fi
  if ! invalidated_logs="$(cast_logs "$from_block" "$confirmed_block" "$CHECKPOINT_INVALIDATED_EVENT" 2>/dev/null)"; then
    notify_runtime_error "cast_logs:CheckpointInvalidated" "failed to fetch CheckpointInvalidated logs from $from_block to $confirmed_block"
    return 1
  fi

  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    process_checkpoint_proposed_log "$entry" || true
  done < <(jq -c '.[]' <<<"$proposed_logs")

  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    process_checkpoint_invalidated_log "$entry" || true
  done < <(jq -c '.[]' <<<"$invalidated_logs")

  retry_unknown_checkpoints
  check_for_missed_proposals
  check_attester_epoch_results
  state_put "l1_block_cursor.txt" "$confirmed_block"
  runtime_mark "last_l1_poll"
}

main_loop() {
  while true; do
    poll_container_health || true
    poll_docker_logs || true
    poll_upcoming_duties || true
    poll_l1_completions || true
    prune_alert_dedupe || true
    runtime_mark "last_loop"
    sleep "$AZMON_POLL_INTERVAL_SEC"
  done
}

main() {
  load_runtime_config
  log_info "$SCRIPT_NAME starting with state dir $AZMON_STATE_DIR"
  log_info "monitoring ${#MONITORED_ADDRESSES[@]} sequencer address(es)"
  main_loop
}

main "$@"
