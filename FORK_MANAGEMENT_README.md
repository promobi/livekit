# LiveKit Fork Management - Custom Branch Strategy

This repository uses a **custom branch strategy** to maintain Promobi-specific changes while staying in sync with upstream LiveKit.

## Branch Structure

```
upstream/master (livekit/livekit)
    ↓
origin/master (promobi/livekit) ← Clean mirror, never commit here
    ↓
origin/promobi-customizations ← Your custom changes, use for deployment
```

### Branches

- **`master`**: Clean mirror of upstream LiveKit (never commit directly to this branch)
- **`promobi-customizations`**: Your custom features rebased on top of master (use this for builds/deployment)

## Custom Changes

Your current custom commits (4 total):
1. **Redis Error Handling** - Added error handling when Redis connection is unavailable
2. **Telemetry: Room Bytes** - Collect room incoming/outgoing bytes in Redis for reporting
3. **Bandwidth Metrics** - Added room bandwidth usage metrics collection and reporting to Prometheus
4. **CI Workflows Disabled** - Prevent Docker builds, releases, and tests from running on fork

## CI/CD Workflows

**All GitHub Actions workflows are disabled on this fork** to prevent:
- Unwanted Docker image builds when pushing to master
- Failed release workflows (missing credentials)
- Unnecessary CI minutes usage

Workflows only run on the original `livekit/livekit` repository, not on `promobi/livekit`.

**See [CI_WORKFLOWS_GUIDE.md](./CI_WORKFLOWS_GUIDE.md)** for details on:
- How workflows were disabled
- How to re-enable specific workflows if needed
- Managing CI on your fork

## Quick Start

### Initial Setup (First Time Only)

```bash
# Run the sync script - it will set up everything
./sync-upstream.sh
```

This will:
1. Add upstream remote (https://github.com/livekit/livekit.git)
2. Create `promobi-customizations` branch with your changes
3. Reset `master` to match upstream exactly
4. Rebase your custom commits on top of updated master

### Regular Workflow

#### 1. Check Status
```bash
./git-helpers.sh status
```

Shows:
- Current branch
- How far behind upstream you are
- Number of custom commits
- Any uncommitted changes

#### 2. Sync with Upstream (Weekly/Bi-weekly)
```bash
./sync-upstream.sh
```

This script will:
- Fetch latest changes from upstream LiveKit
- Update `master` to match upstream
- Rebase `promobi-customizations` on top of the new master
- Handle conflicts if any (with guidance)
- Push changes to your fork

#### 3. Build for Deployment
```bash
./git-helpers.sh build
```

Or prepare for deployment with tests:
```bash
./git-helpers.sh deploy-prep
```

#### 4. View Your Custom Changes
```bash
# List custom commits
./git-helpers.sh list-custom

# Show diff of custom changes
./git-helpers.sh diff

# Detailed diff
git diff master..promobi-customizations
```

#### 5. Switch Between Branches
```bash
./git-helpers.sh switch
```

Toggles between `master` and `promobi-customizations`.

#### 6. Backup Before Major Sync
```bash
./git-helpers.sh backup
```

Creates a timestamped tag for safety.

## Handling Conflicts

When running `./sync-upstream.sh`, if conflicts occur:

### During the Sync Script
The script will pause and show:
```
[ERROR] Rebase encountered conflicts!

=== CONFLICT RESOLUTION GUIDE ===

To resolve conflicts:
  1. Fix conflicts in the files listed above
  2. Stage resolved files: git add <file>
  3. Continue rebase: git rebase --continue
  4. Repeat until rebase completes
```

### Conflict Resolution Steps

1. **Identify conflicted files:**
   ```bash
   git status
   ```

2. **Open conflicted file and look for markers:**
   ```go
   <<<<<<< HEAD (Your custom code)
   // Your implementation
   =======
   // Upstream's new code
   >>>>>>> upstream
   ```

3. **Resolve conflict** by editing the file:
   - Keep both changes and merge them logically
   - Or choose one version
   - Remove conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`)

4. **Mark as resolved:**
   ```bash
   git add path/to/file.go
   git rebase --continue
   ```

5. **If more conflicts, repeat steps 2-4**

6. **After all conflicts resolved:**
   ```bash
   # Run sync script again to push changes
   ./sync-upstream.sh
   ```

### Detailed Conflict Resolution Guide

See [CONFLICT_RESOLUTION_GUIDE.md](./CONFLICT_RESOLUTION_GUIDE.md) for:
- Common conflict scenarios
- Resolution strategies
- Visual merge tools
- Prevention strategies
- Emergency procedures

## Adding New Custom Changes

### When You Make New Commits

Always work on the `promobi-customizations` branch:

```bash
# Switch to custom branch
git checkout promobi-customizations

# Make your changes
# ... edit files ...

# Commit
git commit -am "feat: your new feature"

# Push to your fork
git push origin promobi-customizations
```

### Never Commit to Master

The `master` branch should always be a clean mirror of upstream:
- ❌ Don't commit to `master`
- ❌ Don't merge into `master`
- ✅ Only update `master` via `./sync-upstream.sh`

## Deployment

### Which Branch to Deploy?

**Always deploy from `promobi-customizations`** ✅

```bash
# Prepare for deployment
./git-helpers.sh deploy-prep

# This will:
# - Switch to promobi-customizations
# - Build the project
# - Run tests
# - Show summary of changes

# Then deploy as usual
# (Your deployment commands here)
```

## Troubleshooting

### Problem: Merge commits in master

If you accidentally committed to `master`:

```bash
# Move those commits to custom branch
git checkout promobi-customizations
git merge master

# Reset master to upstream
git checkout master
git reset --hard upstream/master
git push origin master --force-with-lease
```

### Problem: Rebase conflict is too complex

```bash
# Abort the rebase
git rebase --abort

# Try cherry-pick approach instead
git checkout master
git checkout -b promobi-customizations-new
git cherry-pick <commit-hash-1>
git cherry-pick <commit-hash-2>
# ... resolve conflicts one by one ...

# Replace old branch
git branch -D promobi-customizations
git branch -m promobi-customizations-new promobi-customizations
```

### Problem: Lost track of custom changes

```bash
# Your custom commits are between master and promobi-customizations
git log --oneline master..promobi-customizations

# See detailed changes
git diff master..promobi-customizations

# If you created backups
git tag -l 'backup/*'
git checkout -b restore-branch backup/promobi-customizations-20240315-143000
```

### Problem: Accidentally force-pushed master

```bash
# Check reflog
git reflog show master

# Find the previous good state
# Reset to it
git reset --hard master@{1}  # or appropriate reflog entry
git push origin master --force-with-lease
```

## Commands Reference

### Sync Script (`./sync-upstream.sh`)
```bash
./sync-upstream.sh          # Sync with upstream, rebase custom changes
```

### Helper Script (`./git-helpers.sh`)
```bash
./git-helpers.sh status      # Show branch status
./git-helpers.sh switch      # Switch between master/custom branch
./git-helpers.sh build       # Build from custom branch
./git-helpers.sh diff        # Show custom changes
./git-helpers.sh list-custom # List custom commits
./git-helpers.sh backup      # Create backup tag
./git-helpers.sh deploy-prep # Prepare for deployment
```

### Manual Git Commands
```bash
# View custom commits
git log master..promobi-customizations

# View changes in a file across branches
git diff master..promobi-customizations -- path/to/file.go

# Check if branch is up to date
git fetch upstream
git log master..upstream/master  # Should be empty

# Interactive rebase (advanced)
git rebase -i master
```

## Best Practices

1. **Sync regularly** (weekly/bi-weekly) to minimize conflicts
2. **Always build from `promobi-customizations`**
3. **Backup before major syncs** (`./git-helpers.sh backup`)
4. **Keep custom code isolated** (separate functions when possible)
5. **Document your custom changes** in comments and commit messages
6. **Test after every sync** before deploying

## Architecture of Custom Changes

To minimize future conflicts, structure your custom code like this:

```go
// ✅ Good: Separate custom logic
func (r *RoomManager) UpdateStats() {
    r.updateStatsUpstream()    // Original upstream logic
    r.updateStatsCustom()       // Your custom logic (isolated)
}

func (r *RoomManager) updateStatsCustom() {
    // All Promobi-specific telemetry code here
    // Less likely to conflict with upstream changes
}
```

```go
// ❌ Avoid: Mixing custom code throughout existing functions
func (r *RoomManager) UpdateStats() {
    // upstream code
    // your code
    // upstream code
    // your code
    // (conflicts likely on every upstream change)
}
```

## Emergency Contacts

If you run into issues:
1. Check [CONFLICT_RESOLUTION_GUIDE.md](./CONFLICT_RESOLUTION_GUIDE.md)
2. Review git reflog: `git reflog`
3. Check backups: `git tag -l 'backup/*'`
4. All changes are recoverable - don't panic!

## Maintenance Schedule

**Recommended:**
- **Weekly**: Run `./git-helpers.sh status` to check divergence
- **Bi-weekly**: Run `./sync-upstream.sh` to sync with upstream
- **Before major releases**: Create backup with `./git-helpers.sh backup`
- **After sync**: Run `./git-helpers.sh deploy-prep` to verify build

## Files in This Setup

- `sync-upstream.sh` - Main sync script
- `git-helpers.sh` - Helper commands for daily operations
- `CONFLICT_RESOLUTION_GUIDE.md` - Detailed conflict resolution strategies
- `FORK_MANAGEMENT_README.md` - This file

---

**Questions?** Review the guides above or check Git reflog/backups if something goes wrong.
