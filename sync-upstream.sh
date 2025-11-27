#!/bin/bash

# LiveKit Fork Sync Script - Option 3: Custom Branch Strategy
# This script syncs your fork with upstream LiveKit while preserving custom changes

set -e  # Exit on error

# Configuration
UPSTREAM_REMOTE="upstream"
UPSTREAM_URL="https://github.com/livekit/livekit.git"
ORIGIN_REMOTE="origin"
MAIN_BRANCH="master"
CUSTOM_BRANCH="promobi-customizations"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    log_error "Not in a git repository!"
    exit 1
fi

log_info "Starting LiveKit upstream sync process..."

# Step 1: Add upstream remote if it doesn't exist
if ! git remote | grep -q "^${UPSTREAM_REMOTE}$"; then
    log_info "Adding upstream remote: ${UPSTREAM_URL}"
    git remote add "${UPSTREAM_REMOTE}" "${UPSTREAM_URL}"
    log_success "Upstream remote added"
else
    log_info "Upstream remote already exists"
fi

# Step 2: Fetch latest changes from upstream
log_info "Fetching latest changes from upstream..."
git fetch "${UPSTREAM_REMOTE}"
git fetch "${ORIGIN_REMOTE}"
log_success "Fetch completed"

# Step 3: Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    log_error "You have uncommitted changes. Please commit or stash them first."
    git status --short
    exit 1
fi

# Step 4: Create custom branch if it doesn't exist
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

if ! git show-ref --verify --quiet "refs/heads/${CUSTOM_BRANCH}"; then
    log_warning "Custom branch '${CUSTOM_BRANCH}' doesn't exist yet."
    echo -e "${YELLOW}This is the FIRST TIME SETUP. I will:${NC}"
    echo "  1. Create '${CUSTOM_BRANCH}' branch from current master"
    echo "  2. Reset master to match upstream exactly"
    echo "  3. Your custom commits will be preserved in '${CUSTOM_BRANCH}'"
    echo ""
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Aborted by user"
        exit 0
    fi

    log_info "Creating custom branch from current master..."
    git checkout -b "${CUSTOM_BRANCH}" "${MAIN_BRANCH}"
    log_success "Custom branch created: ${CUSTOM_BRANCH}"

    # Switch back to master
    git checkout "${MAIN_BRANCH}"
fi

# Step 5: Update master to match upstream
log_info "Updating ${MAIN_BRANCH} to match upstream/${MAIN_BRANCH}..."
git checkout "${MAIN_BRANCH}"

# Show what will change
BEHIND_COUNT=$(git rev-list --count HEAD..${UPSTREAM_REMOTE}/${MAIN_BRANCH})
AHEAD_COUNT=$(git rev-list --count ${UPSTREAM_REMOTE}/${MAIN_BRANCH}..HEAD)

log_info "Master is ${AHEAD_COUNT} commits ahead and ${BEHIND_COUNT} commits behind upstream"

if [ "$AHEAD_COUNT" -gt 0 ]; then
    log_warning "Your master has custom commits. These will be moved to ${CUSTOM_BRANCH}."
fi

# Reset master to upstream
git reset --hard "${UPSTREAM_REMOTE}/${MAIN_BRANCH}"
log_success "Master branch updated to match upstream"

# Step 6: Update custom branch with latest master
log_info "Rebasing ${CUSTOM_BRANCH} on top of updated master..."
git checkout "${CUSTOM_BRANCH}"

# Check if rebase is needed
CUSTOM_BEHIND=$(git rev-list --count HEAD..${MAIN_BRANCH})
if [ "$CUSTOM_BEHIND" -eq 0 ]; then
    log_success "Custom branch is already up to date!"
    echo ""
    log_info "Summary:"
    echo "  - master: synced with upstream"
    echo "  - ${CUSTOM_BRANCH}: already up to date"
    exit 0
fi

log_info "Custom branch is ${CUSTOM_BEHIND} commits behind master"

# Attempt rebase
log_warning "Starting rebase. If conflicts occur, the script will pause for manual resolution."
echo ""

if git rebase "${MAIN_BRANCH}"; then
    log_success "Rebase completed successfully without conflicts!"
else
    log_error "Rebase encountered conflicts!"
    echo ""
    echo -e "${YELLOW}=== CONFLICT RESOLUTION GUIDE ===${NC}"
    echo ""
    echo "To resolve conflicts:"
    echo "  1. Fix conflicts in the files listed above"
    echo "  2. Stage resolved files: ${GREEN}git add <file>${NC}"
    echo "  3. Continue rebase: ${GREEN}git rebase --continue${NC}"
    echo "  4. Repeat until rebase completes"
    echo ""
    echo "If you want to abort:"
    echo "  - Abort rebase: ${RED}git rebase --abort${NC}"
    echo ""
    echo "After resolving conflicts, run this script again to push changes."
    exit 1
fi

# Step 7: Push changes
echo ""
log_info "Rebase successful! Ready to push changes."
echo ""
echo "Branches to push:"
echo "  - ${MAIN_BRANCH}: will be force-pushed (safe, it's a mirror)"
echo "  - ${CUSTOM_BRANCH}: will be force-pushed (history rewritten due to rebase)"
echo ""
read -p "Push changes to origin? (y/n) " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "Pushing ${MAIN_BRANCH}..."
    git checkout "${MAIN_BRANCH}"
    git push "${ORIGIN_REMOTE}" "${MAIN_BRANCH}" --force-with-lease

    log_info "Pushing ${CUSTOM_BRANCH}..."
    git checkout "${CUSTOM_BRANCH}"
    git push "${ORIGIN_REMOTE}" "${CUSTOM_BRANCH}" --force-with-lease

    log_success "All changes pushed successfully!"
else
    log_info "Skipped pushing. You can manually push later with:"
    echo "  git push origin ${MAIN_BRANCH} --force-with-lease"
    echo "  git push origin ${CUSTOM_BRANCH} --force-with-lease"
fi

# Final summary
echo ""
echo -e "${GREEN}=== SYNC COMPLETE ===${NC}"
echo ""
echo "Branch status:"
echo "  - ${MAIN_BRANCH}: Clean mirror of upstream LiveKit"
echo "  - ${CUSTOM_BRANCH}: Your custom changes rebased on latest master"
echo ""
echo "For deployment: Use the '${CUSTOM_BRANCH}' branch"
echo ""

git checkout "${CUSTOM_BRANCH}"
log_info "Switched to ${CUSTOM_BRANCH} (ready for building/deployment)"
