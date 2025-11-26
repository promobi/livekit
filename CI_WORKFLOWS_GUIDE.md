# CI/CD Workflows Management for LiveKit Fork

## What Happened

When you pushed to `master`, GitHub Actions workflows were triggered, specifically the Docker build workflow. This happened because the original LiveKit repository has workflows that run on every push to master.

## Solution Applied

I've modified all GitHub Actions workflows to only run on the **original LiveKit repository** (`livekit/livekit`), not on forks like `promobi/livekit`.

### Modified Workflows

All workflows now include this condition:
```yaml
if: github.repository == 'livekit/livekit'
```

This was added to:
- ✅ `.github/workflows/docker.yaml` - Docker image builds
- ✅ `.github/workflows/release.yaml` - Release builds
- ✅ `.github/workflows/buildtest.yaml` - Tests
- ✅ `.github/workflows/slack-notifier.yaml` - Slack notifications

## Verifying Workflows Are Disabled

Check your GitHub repository:
1. Go to: `https://github.com/promobi/livekit/actions`
2. Recent workflow runs should skip with message: "Job skipped"
3. Or they won't appear at all

## If You Want to Enable Workflows on Your Fork

### Option 1: Enable Specific Workflows

Remove the `if: github.repository == 'livekit/livekit'` line from workflows you want to run.

For example, to enable tests on your fork:

**`.github/workflows/buildtest.yaml`:**
```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    # Remove or comment out this line:
    # if: github.repository == 'livekit/livekit'
    steps:
      ...
```

### Option 2: Enable with Different Conditions

Modify the condition to include your fork:

```yaml
if: github.repository == 'livekit/livekit' || github.repository == 'promobi/livekit'
```

### Option 3: Create Custom Workflows

Create your own workflows in `.github/workflows/` specifically for your fork:

**`.github/workflows/promobi-docker.yaml`:**
```yaml
name: Promobi Docker Build

on:
  workflow_dispatch:  # Manual trigger only
  push:
    branches:
      - promobi-customizations  # Only your custom branch

jobs:
  docker:
    runs-on: ubuntu-latest
    steps:
      # Your custom Docker build steps
      # Push to your own Docker registry
```

## Workflow Triggers Explained

### docker.yaml
**Original triggers:**
- Push to `master` branch → Builds Docker image with 'master' tag
- Push version tags (v*.*.*) → Builds and tags as 'latest'

**Why it's a problem for forks:**
- Tries to push to DockerHub (livekit/livekit-server)
- Requires DockerHub credentials you don't have
- Wastes CI minutes

### release.yaml
**Original triggers:**
- Push version tags (v*.*.*) → Creates GitHub releases

**Why it's a problem for forks:**
- Creates releases on your fork unnecessarily
- Runs GoReleaser which is expensive

### buildtest.yaml
**Original triggers:**
- Push to `master`
- Pull requests to `master`

**Why it might be a problem:**
- Uses GitHub Actions minutes
- Runs expensive tests with race detector
- You're already building locally with `mage build`

**When you might want it:**
- To verify your changes don't break tests
- Before creating a pull request to upstream

### slack-notifier.yaml
**Original triggers:**
- Pull request events (review requested, reopened, closed)

**Why it's a problem for forks:**
- Requires Slack credentials you don't have
- Tries to notify LiveKit's Slack channel

## Best Practices

### 1. Keep Workflows Disabled by Default
- Prevents accidental CI runs during sync
- Saves GitHub Actions minutes
- Avoids failed workflows due to missing secrets

### 2. Use Workflow Dispatch for Manual Triggers
If you want to run tests occasionally:

```yaml
on:
  workflow_dispatch:  # Enable manual triggering from GitHub UI
```

Then trigger manually from: `https://github.com/promobi/livekit/actions`

### 3. Use Different Branch Names
Trigger workflows only on your custom branch:

```yaml
on:
  push:
    branches:
      - promobi-customizations  # Not master
```

### 4. Use [skip ci] in Commit Messages
If workflows are enabled, skip them per commit:

```bash
git commit -m "sync: update from upstream [skip ci]"
```

GitHub Actions will skip workflows when `[skip ci]` or `[ci skip]` is in the commit message.

## Checking CI Status

### View Workflow Runs
```bash
# Open GitHub Actions page
open https://github.com/promobi/livekit/actions
```

### Check Latest Run
```bash
# Using GitHub CLI (if installed)
gh run list --repo promobi/livekit

# View specific run
gh run view <run-id> --repo promobi/livekit
```

### Cancel Running Workflows
If workflows are running and you want to stop them:

```bash
# Via GitHub UI
# Go to Actions tab → Click on running workflow → Cancel workflow

# Via GitHub CLI
gh run cancel <run-id> --repo promobi/livekit
```

## Cost Considerations

GitHub provides:
- **Free tier**: 2,000 minutes/month for private repos
- **Public repos**: Unlimited (but LiveKit's workflows are expensive)

Each workflow run costs:
- **docker.yaml**: ~10-15 minutes (multi-platform build)
- **buildtest.yaml**: ~5-10 minutes (tests with race detector)
- **release.yaml**: ~5-10 minutes

**With workflows disabled**, you save these minutes for when you actually need them.

## When Syncing with Upstream

The `sync-upstream.sh` script will:
1. Fetch from upstream (no CI trigger)
2. Update master (pushes, but workflows skip due to `if` condition)
3. Rebase custom branch (pushes, but workflows skip)

**Result**: No unwanted CI runs! ✅

## Troubleshooting

### "Workflow required but not running"
Some repos have required status checks. Since workflows are disabled on forks, you might see warnings. This is fine - these checks don't apply to your fork.

### "Workflow failed" notifications
If you're still getting workflow failure emails:
1. Check `.github/workflows/` files have the `if` condition
2. Verify the condition uses correct repository name
3. Disable email notifications: GitHub Settings → Notifications → Actions

### Re-enabling after upstream sync
After syncing with upstream, if they add new workflows:
1. New workflows will run on your fork
2. Add the same `if` condition to new workflows
3. Or disable them entirely

## Summary

✅ **All workflows now disabled on your fork**
- No Docker builds on push
- No releases on tags
- No test runs on push
- No Slack notifications

✅ **Safe to sync with upstream**
- `sync-upstream.sh` won't trigger CI
- Pushes to master and custom branch are safe

✅ **Can re-enable selectively if needed**
- Edit workflow files to remove `if` condition
- Or use `workflow_dispatch` for manual runs

🎯 **Recommendation**: Keep workflows disabled unless you specifically need them.
