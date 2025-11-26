# Quick Reference - LiveKit Fork Management

## Daily Commands

```bash
# Check status
./git-helpers.sh status

# List your custom commits
./git-helpers.sh list-custom

# View changes
./git-helpers.sh diff

# Switch between branches
./git-helpers.sh switch

# Build
mage build

# Build + test (for deployment)
./git-helpers.sh deploy-prep
```

## Adding New Customizations

```bash
# 1. Make sure you're synced
./git-helpers.sh status
./sync-upstream.sh  # if needed

# 2. Switch to custom branch
git checkout promobi-customizations

# 3. Make your changes
vim pkg/service/myfeature.go

# 4. Test
mage build
mage test

# 5. Commit
git add pkg/service/myfeature.go
git commit -m "feat(service): add my feature"

# 6. Push
git push origin promobi-customizations

# 7. Deploy
./git-helpers.sh deploy-prep
```

## Syncing with Upstream

```bash
# Weekly/bi-weekly
./sync-upstream.sh

# If conflicts occur:
# 1. Fix conflicts in files
# 2. git add <file>
# 3. git rebase --continue
# 4. Repeat if needed
# 5. Run ./sync-upstream.sh again to push
```

## Branch Rules

- **master**: Clean mirror of upstream (sync-only, never commit here)
- **promobi-customizations**: Your work (commit here, deploy from here)

## Best Practices for Customizations

### ✅ DO:
- Create new files: `pkg/service/promobi_*.go`
- Keep changes minimal in existing files
- Use descriptive commit messages
- Test before committing
- Sync regularly (weekly/bi-weekly)

### ❌ DON'T:
- Commit to master
- Make massive changes to core files
- Skip testing
- Mix multiple features in one commit

## File Locations

- **Sync script**: `./sync-upstream.sh`
- **Helper script**: `./git-helpers.sh`
- **Built binary**: `./bin/livekit-server`
- **Config example**: `config.yaml`

## Documentation

- **Adding customizations**: `ADDING_CUSTOMIZATIONS_GUIDE.md`
- **Conflict resolution**: `CONFLICT_RESOLUTION_GUIDE.md`
- **Fork management**: `FORK_MANAGEMENT_README.md`
- **CI/CD workflows**: `CI_WORKFLOWS_GUIDE.md`
- **This file**: `QUICK_REFERENCE.md`

## Emergency Commands

```bash
# Abort rebase if stuck
git rebase --abort

# Create backup before major changes
./git-helpers.sh backup

# View reflog (find lost commits)
git reflog

# Restore from backup
git tag -l 'backup/*'
git checkout -b restore backup/promobi-customizations-YYYYMMDD-HHMMSS

# Cancel running GitHub Actions (if needed)
open https://github.com/promobi/livekit/actions
```

## Troubleshooting

### "Uncommitted changes"
```bash
git status
git stash  # or commit them
```

### "Branch diverged"
```bash
./git-helpers.sh status  # check status
./sync-upstream.sh       # sync to fix
```

### "Build failed"
```bash
# Check syntax
go build ./...

# Clean and rebuild
mage clean
mage build
```

### "Conflicts during sync"
See `CONFLICT_RESOLUTION_GUIDE.md`

## Deployment Workflow

```bash
# 1. Ensure up-to-date
./git-helpers.sh status

# 2. Build and test
./git-helpers.sh deploy-prep

# 3. Binary is ready
./bin/livekit-server --version

# 4. Deploy (your deployment process)
# ... copy binary to server ...
# ... restart service ...
```

## Custom Commits Tracker

Current customizations:
1. Room bandwidth metrics (Prometheus)
2. Room bytes collection (Redis)
3. Redis error handling
4. CI workflows disabled
5. Documentation

Update this list when adding new customizations!

## Contact

For questions about:
- **Fork strategy**: See `FORK_MANAGEMENT_README.md`
- **Adding features**: See `ADDING_CUSTOMIZATIONS_GUIDE.md`
- **Conflicts**: See `CONFLICT_RESOLUTION_GUIDE.md`
- **CI/CD**: See `CI_WORKFLOWS_GUIDE.md`
