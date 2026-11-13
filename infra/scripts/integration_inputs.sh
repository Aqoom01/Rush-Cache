# Sourced by both script and after_script. Runtime exports take precedence over
# the prefilled web-pipeline variables; YAML job variables alone do not.
if [ "${EXPERIMENT_SUITE:-}" = integrated-core ]; then
  case "${CI_JOB_NAME%%:*}" in
    core-pilot) CORE_PHASE=pilot ;;
    core-screen) CORE_PHASE=screen ;;
    core-working-set) CORE_PHASE=working-set ;;
    core-balance) CORE_PHASE=balance ;;
    core-copy) CORE_PHASE=copy ;;
    core-failure) CORE_PHASE=failure ;;
    core-final) CORE_PHASE=final ;;
    *) echo "Unknown core cloud job: ${CI_JOB_NAME:-unset}" >&2; exit 1 ;;
  esac
  INTEGRATION_MATRIX="core-plan/${CORE_PHASE}.csv"
  INTEGRATION_PLAN_SPEC="core-plan/${CORE_PHASE}.json"
  INTEGRATION_PLAN_ARGS='[]'
  export CORE_PHASE INTEGRATION_MATRIX INTEGRATION_PLAN_SPEC INTEGRATION_PLAN_ARGS
fi
