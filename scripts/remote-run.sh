#!/bin/bash
#
# remote-run.sh - Drive a remote EC2 host through a benchmark / TCL test run.
#
# Lifecycle:
#   1. aws ec2 start-instances     -> boot the (normally stopped) instance
#   2. aws ec2 wait ...            -> block until it is running / status-ok
#   3. ssh <host> "... make <target>" -> run the requested workload remotely
#   4. scp results DB back         -> pull ~/.vibesql/test_results/*.vbsql local
#   5. aws ec2 stop-instances      -> always stop the instance (via EXIT trap)
#
# The stop step runs from an EXIT trap so a failed run can NEVER leave the
# instance running unattended (a forgotten instance = wasted spend).
#
# IMPORTANT: This script is the repo-side "Part 2" automation. It is designed to
# be correct-by-construction and validated with --dry-run + shellcheck; it does
# NOT require (and this repo does not perform) any live AWS provisioning. The
# actual EC2 instance / IAM / credentials are operator-provisioned "Part 1".
#
# Usage:
#   scripts/remote-run.sh --target=<instance-id-or-name> --task=<task> [options]
#
# Required:
#   --target=<id>       EC2 instance id (i-...) or a Name tag to resolve.
#   --task=<task>       Workload to run. One of the mapped tasks below.
#
# Options:
#   --machine-tag=<tag> Tag recorded in the results DB (VIBESQL_MACHINE_TAG on
#                       the remote make invocation). Default: ec2-c7i-4xlarge.
#   --ssh-host=<host>   SSH destination (user@host). If omitted, resolved from
#                       the instance's public DNS name after it is running.
#   --ssh-user=<user>   SSH user when --ssh-host is not given. Default: ubuntu.
#   --remote-dir=<dir>  Remote repo directory. Default: vibesql.
#   --region=<region>   AWS region. Default: $AWS_DEFAULT_REGION or us-west-2.
#   --results-dir=<dir> Local dir to fetch results into. Default:
#                       ~/.vibesql/test_results
#   --no-pull           Skip `git pull` on the remote before running the target.
#   --no-stop           Do NOT stop the instance when finished (leave running).
#   --dry-run           Print every aws/ssh/scp command WITHOUT executing it.
#   -h, --help          Show this help.
#
# Task -> make target mapping:
#   tcl-all         -> make test-tcl-all
#   tcl             -> make test-tcl
#   benchmark-quick -> make benchmark-quick
#   benchmark       -> make benchmark
#   benchmark-all   -> make benchmark-all
#   benchmark-tpch  -> make benchmark-tpch
#   benchmark-tpcds -> make benchmark-tpcds
#   benchmark-tpcc  -> make benchmark-tpcc
#   benchmark-sysbench -> make benchmark-sysbench
#
# Examples:
#   scripts/remote-run.sh --target=i-0abc123 --task=tcl-all --dry-run
#   scripts/remote-run.sh --target=vibesql-bench --task=benchmark-quick \
#       --machine-tag=ec2-c7i-4xlarge

set -euo pipefail

# ---------------------------------------------------------------------------
# Colors / logging helpers (match scripts/tcltest style)
# ---------------------------------------------------------------------------
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''
fi

print_error()   { echo -e "${RED}❌ $1${NC}" >&2; }
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}" >&2; }
print_info()    { echo -e "${BLUE}ℹ $1${NC}"; }

# In --dry-run we print the command we WOULD run; otherwise we run it.
DRY_RUN=0
run_cmd() {
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "DRYRUN: $*"
    else
        print_info "RUN: $*"
        "$@"
    fi
}

usage() {
    # Strip the leading "# " comment block at the top of this file.
    sed -n '3,60p' "$0" | sed 's/^# \{0,1\}//'
}

# ---------------------------------------------------------------------------
# Defaults / argument parsing
# ---------------------------------------------------------------------------
TARGET=""
TASK=""
MACHINE_TAG="ec2-c7i-4xlarge"
SSH_HOST=""
SSH_USER="ubuntu"
REMOTE_DIR="vibesql"
REGION="${AWS_DEFAULT_REGION:-us-west-2}"
RESULTS_DIR="$HOME/.vibesql/test_results"
DO_PULL=1
DO_STOP=1

for arg in "$@"; do
    case "$arg" in
        --target=*)      TARGET="${arg#*=}" ;;
        --task=*)        TASK="${arg#*=}" ;;
        --machine-tag=*) MACHINE_TAG="${arg#*=}" ;;
        --ssh-host=*)    SSH_HOST="${arg#*=}" ;;
        --ssh-user=*)    SSH_USER="${arg#*=}" ;;
        --remote-dir=*)  REMOTE_DIR="${arg#*=}" ;;
        --region=*)      REGION="${arg#*=}" ;;
        --results-dir=*) RESULTS_DIR="${arg#*=}" ;;
        --no-pull)       DO_PULL=0 ;;
        --no-stop)       DO_STOP=0 ;;
        --dry-run)       DRY_RUN=1 ;;
        -h|--help)       usage; exit 0 ;;
        *)
            print_error "Unknown argument: $arg"
            echo "" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ -z "$TARGET" ]; then
    print_error "Missing required --target=<instance-id-or-name>"
    exit 2
fi
if [ -z "$TASK" ]; then
    print_error "Missing required --task=<task>"
    exit 2
fi

# ---------------------------------------------------------------------------
# Map --task to a make target
# ---------------------------------------------------------------------------
case "$TASK" in
    tcl-all)            MAKE_TARGET="test-tcl-all" ;;
    tcl)                MAKE_TARGET="test-tcl" ;;
    benchmark-quick)    MAKE_TARGET="benchmark-quick" ;;
    benchmark)          MAKE_TARGET="benchmark" ;;
    benchmark-all)      MAKE_TARGET="benchmark-all" ;;
    benchmark-tpch)     MAKE_TARGET="benchmark-tpch" ;;
    benchmark-tpcds)    MAKE_TARGET="benchmark-tpcds" ;;
    benchmark-tpcc)     MAKE_TARGET="benchmark-tpcc" ;;
    benchmark-sysbench) MAKE_TARGET="benchmark-sysbench" ;;
    *)
        print_error "Unknown --task=$TASK"
        echo "Valid tasks: tcl-all, tcl, benchmark-quick, benchmark, benchmark-all," >&2
        echo "             benchmark-tpch, benchmark-tpcds, benchmark-tpcc, benchmark-sysbench" >&2
        exit 2
        ;;
esac

# ---------------------------------------------------------------------------
# Preflight: aws CLI presence + credentials (don't swallow errors)
# ---------------------------------------------------------------------------
# In dry-run we still want to catch a missing aws CLI early, but we do NOT
# require valid credentials (the whole point of dry-run is to work offline).
if ! command -v aws >/dev/null 2>&1; then
    print_error "The 'aws' CLI is not installed or not on PATH."
    print_error "Install it (https://docs.aws.amazon.com/cli/) before running remote tasks."
    exit 3
fi

if [ "$DRY_RUN" -eq 0 ]; then
    if ! aws sts get-caller-identity --region "$REGION" >/dev/null 2>&1; then
        print_error "AWS credentials are missing or invalid (aws sts get-caller-identity failed)."
        print_error "Configure credentials (e.g. source the repo-local .env, or run 'aws configure')"
        print_error "and set AWS_DEFAULT_REGION (currently: $REGION)."
        exit 3
    fi
fi

# ---------------------------------------------------------------------------
# Resolve the instance id from --target (may be an id or a Name tag)
# ---------------------------------------------------------------------------
INSTANCE_ID=""
resolve_instance_id() {
    if [[ "$TARGET" == i-* ]]; then
        INSTANCE_ID="$TARGET"
        return
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        # Show the resolution command we would run, but keep a readable
        # placeholder so downstream commands are still assertable.
        echo "DRYRUN: aws ec2 describe-instances --region $REGION --filters Name=tag:Name,Values=$TARGET --query Reservations[].Instances[].InstanceId --output text"
        INSTANCE_ID="<resolved-from-name:$TARGET>"
        return
    fi
    print_info "Resolving instance id for Name tag: $TARGET"
    INSTANCE_ID="$(aws ec2 describe-instances \
        --region "$REGION" \
        --filters "Name=tag:Name,Values=$TARGET" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[].Instances[].InstanceId' \
        --output text)"
    if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "None" ]; then
        print_error "Could not resolve an instance id for Name=$TARGET in region $REGION."
        exit 4
    fi
}
resolve_instance_id

# ---------------------------------------------------------------------------
# Cleanup trap: ALWAYS attempt to stop the instance, even on failure.
# ---------------------------------------------------------------------------
STOP_DONE=0
stop_instance() {
    # Guard against double-invocation (EXIT after an explicit call).
    if [ "$STOP_DONE" -eq 1 ]; then
        return
    fi
    STOP_DONE=1

    if [ "$DO_STOP" -eq 0 ]; then
        print_warning "--no-stop set: leaving instance $INSTANCE_ID running."
        return
    fi

    print_info "Stopping instance $INSTANCE_ID (cleanup)..."
    run_cmd aws ec2 stop-instances --region "$REGION" --instance-ids "$INSTANCE_ID"
}
# The trap fires on normal exit AND on any error (set -e) or signal, so the
# stop step is guaranteed to run even if ssh/scp/make fails mid-run.
trap stop_instance EXIT INT TERM

# ---------------------------------------------------------------------------
# 1. Start the instance and wait until it is reachable
# ---------------------------------------------------------------------------
print_info "Starting instance $INSTANCE_ID in $REGION..."
run_cmd aws ec2 start-instances --region "$REGION" --instance-ids "$INSTANCE_ID"

print_info "Waiting for instance $INSTANCE_ID to reach status-ok..."
run_cmd aws ec2 wait instance-status-ok --region "$REGION" --instance-ids "$INSTANCE_ID"

# ---------------------------------------------------------------------------
# 2. Resolve the SSH host (public DNS) if not provided explicitly
# ---------------------------------------------------------------------------
REMOTE=""
if [ -n "$SSH_HOST" ]; then
    REMOTE="$SSH_HOST"
elif [ "$DRY_RUN" -eq 1 ]; then
    echo "DRYRUN: aws ec2 describe-instances --region $REGION --instance-ids $INSTANCE_ID --query Reservations[].Instances[].PublicDnsName --output text"
    REMOTE="$SSH_USER@<public-dns:$INSTANCE_ID>"
else
    print_info "Resolving public DNS for $INSTANCE_ID..."
    PUBLIC_DNS="$(aws ec2 describe-instances \
        --region "$REGION" \
        --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[].Instances[].PublicDnsName' \
        --output text)"
    if [ -z "$PUBLIC_DNS" ] || [ "$PUBLIC_DNS" = "None" ]; then
        print_error "Instance $INSTANCE_ID has no public DNS name; pass --ssh-host explicitly."
        exit 4
    fi
    REMOTE="$SSH_USER@$PUBLIC_DNS"
fi

# ---------------------------------------------------------------------------
# 3. Run the workload remotely
# ---------------------------------------------------------------------------
# Build the remote command: cd into the repo, optionally pull, then run the
# make target with VIBESQL_MACHINE_TAG so results are tagged automatically.
REMOTE_STEPS="cd $REMOTE_DIR"
if [ "$DO_PULL" -eq 1 ]; then
    REMOTE_STEPS="$REMOTE_STEPS && git pull --ff-only"
fi
REMOTE_STEPS="$REMOTE_STEPS && VIBESQL_MACHINE_TAG=$MACHINE_TAG make $MAKE_TARGET"

print_info "Running remote task '$TASK' (make $MAKE_TARGET) on $REMOTE..."
run_cmd ssh "$REMOTE" "$REMOTE_STEPS"

# ---------------------------------------------------------------------------
# 4. Fetch the results database(s) back to the local machine
# ---------------------------------------------------------------------------
print_info "Fetching results DB(s) into $RESULTS_DIR ..."
run_cmd mkdir -p "$RESULTS_DIR"
run_cmd scp "$REMOTE:~/.vibesql/test_results/*.vbsql" "$RESULTS_DIR/"

# ---------------------------------------------------------------------------
# 5. Stop the instance (explicit; trap is a backstop for the failure path)
# ---------------------------------------------------------------------------
stop_instance

print_success "Remote task '$TASK' complete (machine_tag=$MACHINE_TAG)."
