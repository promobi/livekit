# Conflict Resolution Guide for LiveKit Fork Sync

## Understanding Conflicts

When rebasing your custom changes on top of upstream updates, conflicts occur when:
- Upstream modified the same code lines you changed
- Upstream deleted files you modified
- Structural changes in upstream affect your customizations

## Conflict Resolution Strategies

### Strategy 1: Accept Upstream Changes (Theirs)
**When to use:** Upstream refactored code significantly, and you want to reapply your logic to the new structure.

```bash
# For a specific file
git checkout --theirs path/to/conflicted/file.go

# Then manually re-add your custom logic
# Edit the file to add back your changes
git add path/to/conflicted/file.go
git rebase --continue
```

### Strategy 2: Keep Your Changes (Ours)
**When to use:** You're confident your changes should override upstream modifications.

```bash
# For a specific file
git checkout --ours path/to/conflicted/file.go
git add path/to/conflicted/file.go
git rebase --continue
```

### Strategy 3: Manual Merge (Recommended)
**When to use:** Most cases - you want to combine both changes intelligently.

```bash
# Open the conflicted file in your editor
# Look for conflict markers:
# <<<<<<< HEAD (your changes)
# your code
# =======
# upstream code
# >>>>>>> upstream

# Manually edit to combine both
# Remove conflict markers
# Save the file

git add path/to/conflicted/file.go
git rebase --continue
```

## Step-by-Step Conflict Resolution Process

### 1. When Rebase Stops Due to Conflict

```bash
$ git rebase master
# ... conflict occurs ...
```

You'll see output like:
```
Auto-merging pkg/service/roommanager.go
CONFLICT (content): Merge conflict in pkg/service/roommanager.go
error: could not apply 48945699... feat(telemetry): collect room bytes
```

### 2. Identify Conflicted Files

```bash
# See which files have conflicts
git status

# Shows:
# Unmerged paths:
#   (use "git add <file>..." to mark resolution)
#       both modified:   pkg/service/roommanager.go
```

### 3. Examine the Conflict

```bash
# View the conflicted file
cat pkg/service/roommanager.go

# Or open in editor
code pkg/service/roommanager.go  # VS Code
vim pkg/service/roommanager.go   # Vim
```

You'll see conflict markers:
```go
func (r *RoomManager) UpdateStats() {
<<<<<<< HEAD (Current Change - Your Custom Code)
    // Your custom telemetry code
    r.collectBandwidthMetrics()
    r.reportToRedis()
=======
    // Upstream changed this function signature
    r.collectMetrics(ctx, opts)
>>>>>>> 48945699 (Incoming Change - Your Commit Being Applied)
}
```

### 4. Resolve the Conflict

**Option A: Merge both logically**
```go
func (r *RoomManager) UpdateStats(ctx context.Context, opts MetricOptions) {
    // Keep upstream's new signature
    r.collectMetrics(ctx, opts)

    // Add back your custom logic
    r.collectBandwidthMetrics()
    r.reportToRedis()
}
```

**Option B: Use diff3 style (more context)**
```bash
git config merge.conflictstyle diff3

# Now conflicts show 3 sections:
# <<<<<<< HEAD
# your version
# ||||||| base (common ancestor)
# original version
# =======
# upstream version
# >>>>>>>
```

### 5. Mark as Resolved and Continue

```bash
# After fixing the file
git add pkg/service/roommanager.go

# Continue the rebase
git rebase --continue

# If more conflicts, repeat steps 3-5
# If no more conflicts, rebase completes
```

### 6. If You Get Stuck

```bash
# Abort and return to pre-rebase state
git rebase --abort

# Then investigate the conflicts more carefully
# Or ask for help
```

## Common Conflict Scenarios for Your Fork

### Scenario 1: Telemetry Code Conflicts
**Your changes:** Adding Redis/Prometheus metrics
**Upstream changes:** Refactored metrics collection

**Resolution approach:**
1. Accept upstream's new architecture
2. Integrate your Redis/Prometheus reporting into the new structure
3. Test that metrics still work

### Scenario 2: Function Signature Changes
**Your changes:** Called a function with old parameters
**Upstream changes:** Added/removed function parameters

**Resolution approach:**
```go
// Before (your code)
room.Close()

// Upstream changed to
room.Close(reason CloseReason)

// Fix by adding the new parameter
room.Close(ReasonNormal)
```

### Scenario 3: Import Path Changes
**Your changes:** Using package imports
**Upstream changes:** Reorganized packages

**Resolution approach:**
```go
// Update import paths
import (
    // Old
    // "github.com/livekit/livekit-server/pkg/telemetry"

    // New (upstream moved it)
    "github.com/livekit/livekit-server/pkg/observability/telemetry"
)
```

## Advanced Techniques

### Interactive Rebase for Complex Conflicts

```bash
# Rebase interactively to handle commits one by one
git rebase -i master

# You can:
# - reorder commits (easier resolution order)
# - squash related commits
# - drop commits that are no longer needed
# - edit commits to fix them manually
```

### Using Merge Tools

```bash
# Configure a visual merge tool
git config merge.tool vimdiff  # or meld, kdiff3, etc.

# When conflicts occur
git mergetool

# Opens visual 3-way merge interface
```

### Checking What Changed in Upstream

```bash
# Before rebasing, see what changed in the conflicting file
git log --oneline master -- pkg/service/roommanager.go

# See the actual changes
git diff master -- pkg/service/roommanager.go

# This helps you understand upstream's changes before resolving
```

## Testing After Conflict Resolution

After resolving all conflicts and completing the rebase:

```bash
# 1. Build the project
go build ./...

# 2. Run tests
go test ./...

# 3. Run specific tests for your custom features
go test ./pkg/service -run TestBandwidthMetrics

# 4. Manual testing
# - Start the server
# - Verify Redis metrics are collected
# - Verify Prometheus metrics work
```

## Prevention Strategies

### 1. Keep Custom Code Isolated
Structure your changes to minimize conflicts:

```go
// Instead of modifying existing functions extensively
func (r *RoomManager) UpdateStats() {
    r.updateStatsOriginal()  // Call original logic
    r.updateStatsCustom()    // Your custom logic in separate function
}

func (r *RoomManager) updateStatsCustom() {
    // All your custom code here
    // Less likely to conflict with upstream changes
}
```

### 2. Regular Syncing
Sync with upstream frequently (weekly/bi-weekly) to avoid large divergences:

```bash
# Small, frequent syncs = easier conflict resolution
./sync-upstream.sh
```

### 3. Document Your Changes
Keep track of your customizations:

```bash
# Create a file listing your custom features
echo "- Redis error handling (pkg/service/redismanager.go:123)" >> CUSTOM_CHANGES.md
echo "- Prometheus bandwidth metrics (pkg/service/roommanager.go:456)" >> CUSTOM_CHANGES.md
```

## Emergency: Complete Rebase Failure

If rebase becomes too complex:

### Option A: Cherry-pick approach
```bash
# Abort the rebase
git rebase --abort

# Create new branch from updated master
git checkout master
git checkout -b promobi-customizations-new

# Cherry-pick your custom commits one by one
git cherry-pick 10828991  # First custom commit
# Resolve conflicts if any
git cherry-pick 48945699  # Second custom commit
# Resolve conflicts if any
git cherry-pick 33d71a9d  # Third custom commit
# Resolve conflicts if any

# Replace old custom branch
git branch -D promobi-customizations
git branch -m promobi-customizations-new promobi-customizations
```

### Option B: Manual port
```bash
# Start fresh from master
git checkout master
git checkout -b promobi-customizations-new

# Manually re-implement your custom features
# Use old branch as reference:
git diff master origin/promobi-customizations

# Copy and adapt your code manually
# Then test thoroughly
```

## Getting Help

If stuck on a specific conflict:

```bash
# Show the conflict in detail
git diff

# Show which commit introduced the conflict
git log --oneline --graph master..HEAD

# Show the file history
git log -p -- path/to/file.go
```

## Quick Reference Commands

```bash
# During rebase conflict resolution
git status              # See conflicted files
git diff                # See conflict details
git add <file>          # Mark resolved
git rebase --continue   # Continue after fixing
git rebase --skip       # Skip current commit
git rebase --abort      # Abort entire rebase

# Tools
git mergetool          # Visual merge tool
git checkout --theirs  # Accept upstream
git checkout --ours    # Keep yours
git diff --ours        # See your version
git diff --theirs      # See upstream version

# After rebase
git log --oneline      # Verify commits
go test ./...          # Run tests
go build ./...         # Build project
```
