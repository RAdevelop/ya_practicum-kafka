#!/bin/bash
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

wait_for_kafka() {
  local broker=$1
  local SERVER=$2
  local start_time=$SECONDS

  echo "=== Waiting for Kafka cluster to be ready (${broker}: ${SERVER}) ==="

  for i in $(seq 1 60); do
    # Захватываем и stdout, и stderr
    output=$(docker exec -e KAFKA_OPTS="" ${broker} kafka-topics \
      --describe \
      --bootstrap-server ${SERVER} \
      --command-config ${COMMAND_CONFIG} \
      --topic __cluster_health_check_never_exists 2>&1) || true

    # "does not exist" — кластер ответил, он готов
    # "Timed out" — кластер ещё не готов
    if echo "$output" | grep -q "does not exist"; then
      local elapsed=$(( SECONDS - start_time ))
      echo "=== Cluster is ready! Took ${elapsed}s (${i} attempts) ==="
      return 0
    fi

    local elapsed=$(( SECONDS - start_time ))
    echo "  [${elapsed}s] Cluster not ready yet (attempt ${i}/60)..."
    sleep 2
  done

  echo "ERROR: Cluster did not become ready in $(( SECONDS - start_time ))s."
  exit 1
}

wait_for_kafka kafka-b-1 ${BOOTSTRAP_SERVER}
wait_for_kafka kafka2-b-1 ${BOOTSTRAP_SERVER2}