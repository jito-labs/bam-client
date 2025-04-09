#!/usr/bin/env bash

# Default remote directory to the current directory name
CURRENT_DIR_NAME=$(basename "$(pwd)")
REMOTE_DIR="/home/core/$CURRENT_DIR_NAME"
BUILD_SERVER="" # Initialize as empty, will be set via args or .env

# Function to display the script's usage
usage() {
  echo "The script syncs the script directory with a remote directory"
  echo "Usage: [--remote_dir </path/to/remote/dir>] [--host <hostname>] [--user <username>]"
  echo "If no remote directory is specified, we sync with the default remote directory: /home/core/<current_directory_name>"
  echo ""
  echo "Options:"
  echo "  --remote_dir : Remote directory to sync the current directory to. Example --remote_dir /path/to/remote/dir"
  echo "  --host       : Hostname to sync with. Example --host dallas-testnet-validator-1"
  echo "  --user       : Username to use for SSH connection. Example --user core"
  echo ""
  echo "Examples:"
  echo "  # Sync current directory to remote server using the same directory name"
  echo "  ./s --user core --host dallas-testnet-validator-1"
  echo ""
  echo "  # Sync current directory to a specific remote directory"
  echo "  ./s --user core --host dallas-testnet-validator-1 --remote_dir /home/core/"
}

parse_args() {
  # Initialize user and host as empty
  USER=""
  HOST=""

  # Loop through the remaining arguments
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --remote_dir)
        if [[ $# -gt 1 ]]; then
          REMOTE_DIR="$2" # Assign the value
          shift        # Consume the value
        else
          echo "Error: Missing value for $1. Example usage: --remote_dir /path/to/remote/dir"
          exit 1
        fi
        ;;
      --host)
        if [[ $# -gt 1 ]]; then
          HOST="$2"
          shift
        else
          echo "Error: Missing value for $1. Example usage: --host dallas-testnet-validator-1"
          exit 1
        fi
        ;;
      --user)
        if [[ $# -gt 1 ]]; then
          USER="$2"
          shift
        else
          echo "Error: Missing value for $1. Example usage: --user core"
          exit 1
        fi
        ;;
      --help)
        # Display usage information
        usage
        exit 0
        ;;
      *)
        # Handle unknown arguments here if needed
        echo "Unknown argument: $1"
        usage
        exit 1
        ;;
    esac
    shift
  done
}

# Parse command-line arguments
parse_args "$@"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Set up the BUILD_SERVER using provided user and host if available
if [[ -n "$USER" && -n "$HOST" ]]; then
  BUILD_SERVER="$USER@$HOST"
elif [[ -z "$BUILD_SERVER" ]]; then
  echo "Error: No host/user specified"
  echo "Please use --host and --user parameters"
  exit 1
fi

echo "Syncing $SCRIPT_DIR to $BUILD_SERVER:$REMOTE_DIR"

# Sync all files
rsync -avh --delete --exclude target "$SCRIPT_DIR" "$BUILD_SERVER":"$REMOTE_DIR"
