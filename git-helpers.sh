#!/bin/bash

# Helper scripts for managing LiveKit fork with custom branch strategy

CUSTOM_BRANCH="promobi-customizations"
MAIN_BRANCH="master"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_help() {
    echo "LiveKit Fork Management Helper"
    echo ""
    echo "Usage: ./git-helpers.sh <command>"
    echo ""
    echo "Commands:"
    echo "  status        - Show current branch status and divergence"
    echo "  switch        - Switch between master and custom branch"
    echo "  build         - Switch to custom branch and build"
    echo "  diff          - Show differences between your custom changes and upstream"
    echo "  list-custom   - List all your custom commits"
    echo "  backup        - Create a backup tag of current custom branch"
    echo "  deploy-prep   - Prepare custom branch for deployment (build & test)"
    echo "  help          - Show this help message"
}

show_status() {
    echo -e "${BLUE}=== Fork Status ===${NC}"
    echo ""

    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    echo -e "Current branch: ${GREEN}${CURRENT_BRANCH}${NC}"
    echo ""

    # Master status
    if git show-ref --verify --quiet refs/heads/${MAIN_BRANCH}; then
        if git remote | grep -q "^upstream$"; then
            git fetch upstream --quiet 2>/dev/null
            MASTER_BEHIND=$(git rev-list --count ${MAIN_BRANCH}..upstream/${MAIN_BRANCH} 2>/dev/null || echo "?")
            MASTER_AHEAD=$(git rev-list --count upstream/${MAIN_BRANCH}..${MAIN_BRANCH} 2>/dev/null || echo "?")
            echo "📍 ${MAIN_BRANCH}:"
            echo "   Behind upstream: ${MASTER_BEHIND} commits"
            echo "   Ahead of upstream: ${MASTER_AHEAD} commits"
            if [ "$MASTER_AHEAD" != "0" ] && [ "$MASTER_AHEAD" != "?" ]; then
                echo -e "   ${YELLOW}⚠ Warning: master should be a clean mirror of upstream${NC}"
            fi
        else
            echo "📍 ${MAIN_BRANCH}: (upstream not configured)"
        fi
        echo ""
    fi

    # Custom branch status
    if git show-ref --verify --quiet refs/heads/${CUSTOM_BRANCH}; then
        CUSTOM_BEHIND=$(git rev-list --count ${CUSTOM_BRANCH}..${MAIN_BRANCH} 2>/dev/null || echo "?")
        CUSTOM_AHEAD=$(git rev-list --count ${MAIN_BRANCH}..${CUSTOM_BRANCH} 2>/dev/null || echo "?")
        echo "📍 ${CUSTOM_BRANCH}:"
        echo "   Behind master: ${CUSTOM_BEHIND} commits"
        echo "   Custom commits: ${CUSTOM_AHEAD} commits"
        if [ "$CUSTOM_BEHIND" != "0" ] && [ "$CUSTOM_BEHIND" != "?" ]; then
            echo -e "   ${YELLOW}⚠ Need to sync: Run ./sync-upstream.sh${NC}"
        fi
        echo ""
    else
        echo -e "${YELLOW}📍 ${CUSTOM_BRANCH}: Not created yet${NC}"
        echo "   Run ./sync-upstream.sh to set up"
        echo ""
    fi

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        echo -e "${RED}⚠ You have uncommitted changes:${NC}"
        git status --short
        echo ""
    fi
}

switch_branch() {
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

    if [ "$CURRENT_BRANCH" = "$MAIN_BRANCH" ]; then
        TARGET="$CUSTOM_BRANCH"
    else
        TARGET="$MAIN_BRANCH"
    fi

    if ! git show-ref --verify --quiet refs/heads/${TARGET}; then
        echo -e "${RED}Branch '${TARGET}' doesn't exist${NC}"
        echo "Run ./sync-upstream.sh to set up the branch structure"
        exit 1
    fi

    echo -e "Switching from ${YELLOW}${CURRENT_BRANCH}${NC} to ${GREEN}${TARGET}${NC}"
    git checkout "$TARGET"
}

build_project() {
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

    if [ "$CURRENT_BRANCH" != "$CUSTOM_BRANCH" ]; then
        echo -e "${YELLOW}Not on ${CUSTOM_BRANCH}, switching...${NC}"
        git checkout "$CUSTOM_BRANCH"
    fi

    echo -e "${BLUE}Building project from ${CUSTOM_BRANCH}...${NC}"
    echo ""

    # Check if it's a Go project
    if [ -f "go.mod" ]; then
        echo "Running: go build ./..."
        go build ./...
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Build successful${NC}"
        else
            echo -e "${RED}✗ Build failed${NC}"
            exit 1
        fi
    else
        echo -e "${YELLOW}No go.mod found. Please build manually.${NC}"
    fi
}

show_diff() {
    if ! git show-ref --verify --quiet refs/heads/${CUSTOM_BRANCH}; then
        echo -e "${RED}Custom branch doesn't exist yet${NC}"
        exit 1
    fi

    echo -e "${BLUE}=== Custom Changes (${CUSTOM_BRANCH} vs ${MAIN_BRANCH}) ===${NC}"
    echo ""
    git diff --stat "${MAIN_BRANCH}..${CUSTOM_BRANCH}"
    echo ""
    echo "For detailed diff, run:"
    echo "  git diff ${MAIN_BRANCH}..${CUSTOM_BRANCH}"
}

list_custom_commits() {
    if ! git show-ref --verify --quiet refs/heads/${CUSTOM_BRANCH}; then
        echo -e "${RED}Custom branch doesn't exist yet${NC}"
        exit 1
    fi

    echo -e "${BLUE}=== Your Custom Commits ===${NC}"
    echo ""
    git log --oneline --graph "${MAIN_BRANCH}..${CUSTOM_BRANCH}"
    echo ""
    echo "Total custom commits: $(git rev-list --count ${MAIN_BRANCH}..${CUSTOM_BRANCH})"
}

backup_custom_branch() {
    if ! git show-ref --verify --quiet refs/heads/${CUSTOM_BRANCH}; then
        echo -e "${RED}Custom branch doesn't exist yet${NC}"
        exit 1
    fi

    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    TAG_NAME="backup/${CUSTOM_BRANCH}-${TIMESTAMP}"

    echo -e "${BLUE}Creating backup tag: ${TAG_NAME}${NC}"
    git tag "${TAG_NAME}" "${CUSTOM_BRANCH}"

    echo -e "${GREEN}✓ Backup created${NC}"
    echo ""
    echo "To restore this backup later:"
    echo "  git checkout -b ${CUSTOM_BRANCH}-restored ${TAG_NAME}"
    echo ""
    echo "To push backup to remote:"
    echo "  git push origin ${TAG_NAME}"
    echo ""
    echo "To list all backups:"
    echo "  git tag -l 'backup/*'"
}

deploy_prep() {
    echo -e "${BLUE}=== Deployment Preparation ===${NC}"
    echo ""

    # Switch to custom branch
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [ "$CURRENT_BRANCH" != "$CUSTOM_BRANCH" ]; then
        echo "Switching to ${CUSTOM_BRANCH}..."
        git checkout "$CUSTOM_BRANCH"
    fi

    # Check if up to date
    if git remote | grep -q "^upstream$"; then
        git fetch upstream --quiet
        BEHIND=$(git rev-list --count ${CUSTOM_BRANCH}..${MAIN_BRANCH} 2>/dev/null || echo "0")
        if [ "$BEHIND" != "0" ]; then
            echo -e "${YELLOW}⚠ Custom branch is ${BEHIND} commits behind master${NC}"
            echo "  Consider running ./sync-upstream.sh first"
            echo ""
        fi
    fi

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        echo -e "${RED}⚠ You have uncommitted changes. Commit or stash them first.${NC}"
        git status --short
        exit 1
    fi

    # Build
    echo "Step 1: Building..."
    if [ -f "go.mod" ]; then
        go build ./...
        if [ $? -ne 0 ]; then
            echo -e "${RED}✗ Build failed${NC}"
            exit 1
        fi
        echo -e "${GREEN}✓ Build successful${NC}"
        echo ""
    fi

    # Test
    echo "Step 2: Running tests..."
    if [ -f "go.mod" ]; then
        go test ./... -short
        if [ $? -ne 0 ]; then
            echo -e "${RED}✗ Tests failed${NC}"
            exit 1
        fi
        echo -e "${GREEN}✓ Tests passed${NC}"
        echo ""
    fi

    # Summary
    echo -e "${GREEN}=== Ready for Deployment ===${NC}"
    echo ""
    echo "Current commit:"
    git log -1 --oneline
    echo ""
    echo "Custom changes included:"
    git log --oneline "${MAIN_BRANCH}..${CUSTOM_BRANCH}"
    echo ""
}

# Main script
case "${1:-}" in
    status)
        show_status
        ;;
    switch)
        switch_branch
        ;;
    build)
        build_project
        ;;
    diff)
        show_diff
        ;;
    list-custom)
        list_custom_commits
        ;;
    backup)
        backup_custom_branch
        ;;
    deploy-prep)
        deploy_prep
        ;;
    help|--help|-h|"")
        show_help
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac
