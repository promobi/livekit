# Guide: Adding New Customizations to Your LiveKit Fork

## Strategy Overview

**Golden Rule**: Always work on `promobi-customizations` branch, never on `master`.

```
master (upstream mirror) ← Don't touch, sync-only
    ↓
promobi-customizations ← Work here, add customizations here
```

## Step-by-Step Process

### 1. Ensure You're Synced with Upstream

Before adding new customizations, make sure you're up-to-date:

```bash
# Check current status
./git-helpers.sh status

# If behind upstream, sync first
./sync-upstream.sh
```

**Why?** Adding customizations on an outdated base makes future syncs harder.

### 2. Switch to Custom Branch

```bash
# Make sure you're on the right branch
git checkout promobi-customizations

# Verify
git branch --show-current
# Should show: promobi-customizations
```

### 3. Create Your Customization

Work on your feature as normal:

```bash
# Edit files
vim pkg/service/myfeature.go

# Test locally
mage build
mage test

# Or run specific tests
go test ./pkg/service/...
```

### 4. Commit Your Changes

**Use clear, descriptive commit messages:**

```bash
# Good commit message format
git add pkg/service/myfeature.go
git commit -m "feat(service): add custom feature X

- Implements feature X for Promobi needs
- Adds configuration option Y
- Updates telemetry to track Z

Relates to internal ticket: PROJ-123"
```

**Commit message best practices:**
- Use conventional commits: `feat:`, `fix:`, `chore:`, `refactor:`
- Include scope: `feat(telemetry):`, `fix(rtc):`
- Explain *why*, not just *what*
- Reference internal tickets/issues
- Keep commits atomic (one logical change per commit)

### 5. Push to Your Fork

```bash
# Push to your fork
git push origin promobi-customizations
```

**Note:** No CI workflows will trigger (they're disabled on your fork).

### 6. Deploy and Test

```bash
# Build for deployment
./git-helpers.sh deploy-prep

# Or just build
mage build

# Binary is in ./bin/livekit-server
```

## Best Practices for Minimizing Future Conflicts

### Strategy 1: Isolate Custom Code (Recommended)

Keep your customizations separate from core logic:

#### ❌ Bad: Modifying existing functions extensively

```go
// pkg/service/room.go (existing file)
func (r *Room) Close() error {
    // upstream code...
    r.cleanupParticipants()

    // your custom code mixed in
    r.reportToPromobi()
    r.sendCustomMetrics()
    r.updatePromobiDatabase()

    // more upstream code...
    r.closeConnections()
}
```

**Problem:** Every upstream change to `Close()` will conflict with your modifications.

#### ✅ Good: Separate custom logic

```go
// pkg/service/room.go (existing file)
func (r *Room) Close() error {
    // upstream code...
    r.cleanupParticipants()

    // call your custom logic
    r.closePromobi()

    // more upstream code...
    r.closeConnections()
}

// pkg/service/room_promobi.go (your new file)
func (r *Room) closePromobi() {
    r.reportToPromobi()
    r.sendCustomMetrics()
    r.updatePromobiDatabase()
}
```

**Benefits:**
- Minimal changes to existing files
- Your custom logic is isolated
- Upstream changes to `Close()` rarely conflict
- Easy to see what's custom vs. upstream

### Strategy 2: Create New Files for Custom Features

Instead of modifying existing files, create new files:

```bash
# Your custom files (examples)
pkg/service/promobi_metrics.go
pkg/service/promobi_telemetry.go
pkg/telemetry/promobi_reporter.go
pkg/config/promobi_config.go
```

**Naming convention:**
- Use `promobi_` prefix for custom files
- Or create a subdirectory: `pkg/service/promobi/`

**Benefits:**
- Zero conflicts with upstream (they won't modify your files)
- Easy to identify custom code
- Can be extracted later if needed

### Strategy 3: Use Build Tags for Optional Features

For features that might not always be needed:

```go
//go:build promobi
// +build promobi

package service

// This file only compiles when: go build -tags promobi
func (r *Room) PromobitTelemetry() {
    // custom logic
}
```

**Build with tags:**
```bash
go build -tags promobi ./...
```

### Strategy 4: Configuration-Driven Customizations

Use configuration to enable/disable features:

```go
// pkg/config/config.go
type Config struct {
    // ... upstream fields ...

    // Promobi custom config
    Promobi *PromobitConfig `yaml:"promobi,omitempty"`
}

type PromobitConfig struct {
    EnableCustomMetrics bool   `yaml:"enable_custom_metrics"`
    RedisEndpoint       string `yaml:"redis_endpoint"`
    // ...
}
```

```go
// pkg/service/room.go
func (r *Room) Close() error {
    // upstream code...

    // custom code only runs if configured
    if r.config.Promobi != nil && r.config.Promobi.EnableCustomMetrics {
        r.sendPromobitMetrics()
    }

    // more upstream code...
}
```

**Benefits:**
- Can disable custom features via config
- Easier to test both modes
- Can upstream your fork if you make it generic enough

### Strategy 5: Use Hooks/Callbacks Pattern

If upstream code supports callbacks, use them:

```go
// If upstream has:
type Room struct {
    onClose []func(*Room)
}

func (r *Room) RegisterCloseCallback(fn func(*Room)) {
    r.onClose = append(r.onClose, fn)
}

// Your custom code:
room.RegisterCloseCallback(func(r *Room) {
    // your custom logic
    sendPromobitMetrics(r)
})
```

**Benefits:**
- Zero modification to upstream code
- No conflicts at all
- Most maintainable approach

## Common Customization Scenarios

### Scenario 1: Adding New Metrics

**Good approach:**

```bash
# Create new file
touch pkg/telemetry/prometheus/promobi_metrics.go
```

```go
// pkg/telemetry/prometheus/promobi_metrics.go
package prometheus

import "github.com/prometheus/client_golang/prometheus"

var (
    promPromobitCustomMetric *prometheus.CounterVec
)

func InitPromobitMetrics(nodeID string) {
    promPromobitCustomMetric = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: livekitNamespace,
            Subsystem: "promobi",
            Name:      "custom_metric_total",
            ConstLabels: prometheus.Labels{"node_id": nodeID},
        },
        []string{"type"},
    )
    prometheus.MustRegister(promPromobitCustomMetric)
}

func IncrementPromobitMetric(metricType string) {
    promPromobitCustomMetric.WithLabelValues(metricType).Inc()
}
```

Then hook it into initialization (minimal change to existing files):

```go
// pkg/telemetry/prometheus/prometheus.go
func Init(nodeID string, nodeType livekit.NodeType) error {
    // ... upstream init code ...

    // Add one line:
    InitPromobitMetrics(nodeID)

    return nil
}
```

### Scenario 2: Adding New Configuration

```go
// pkg/config/promobi.go (new file)
package config

type PromobitConfig struct {
    EnableFeatureX bool   `yaml:"enable_feature_x"`
    APIEndpoint    string `yaml:"api_endpoint"`
    APIKey         string `yaml:"api_key"`
}

func (c *PromobitConfig) Validate() error {
    if c.EnableFeatureX && c.APIEndpoint == "" {
        return errors.New("promobi.api_endpoint required when feature X is enabled")
    }
    return nil
}
```

```go
// pkg/config/config.go (existing file, minimal change)
type Config struct {
    // ... all upstream fields ...

    // Add one field at the end:
    Promobi *PromobitConfig `yaml:"promobi,omitempty"`
}

func (c *Config) Validate() error {
    // ... upstream validation ...

    // Add validation for custom config:
    if c.Promobi != nil {
        if err := c.Promobi.Validate(); err != nil {
            return err
        }
    }
    return nil
}
```

### Scenario 3: Hooking into Existing Events

**Instead of modifying event handlers directly:**

```go
// pkg/service/promobi_events.go (new file)
package service

import "github.com/livekit/protocol/livekit"

type PromobitEventHandler struct {
    // your dependencies
}

func NewPromobitEventHandler() *PromobitEventHandler {
    return &PromobitEventHandler{}
}

func (h *PromobitEventHandler) OnParticipantJoined(p *ParticipantImpl) {
    // your custom logic when participant joins
}

func (h *PromobitEventHandler) OnRoomClosed(r *Room) {
    // your custom logic when room closes
}
```

Then integrate with minimal changes:

```go
// pkg/service/room.go (existing file)
type Room struct {
    // ... upstream fields ...

    // Add one field:
    promobiHandler *PromobitEventHandler
}

func NewRoom(...) *Room {
    r := &Room{
        // ... upstream init ...
    }

    // Add custom handler:
    r.promobiHandler = NewPromobitEventHandler()

    return r
}

func (r *Room) onParticipantJoined(p *ParticipantImpl) {
    // ... upstream code ...

    // Call your handler:
    if r.promobiHandler != nil {
        r.promobiHandler.OnParticipantJoined(p)
    }
}
```

### Scenario 4: Adding Database/External Service Integration

```go
// pkg/service/promobi/client.go (new directory & file)
package promobi

type Client struct {
    endpoint string
    apiKey   string
}

func NewClient(endpoint, apiKey string) *Client {
    return &Client{
        endpoint: endpoint,
        apiKey:   apiKey,
    }
}

func (c *Client) ReportRoomMetrics(roomID string, metrics *Metrics) error {
    // your custom API calls
}
```

Use dependency injection:

```go
// pkg/service/room.go
type Room struct {
    // ... upstream fields ...

    promobiClient *promobi.Client
}

func NewRoom(config *config.Config, ...) *Room {
    r := &Room{
        // ... upstream init ...
    }

    // Conditional initialization:
    if config.Promobi != nil && config.Promobi.EnableFeatureX {
        r.promobiClient = promobi.NewClient(
            config.Promobi.APIEndpoint,
            config.Promobi.APIKey,
        )
    }

    return r
}
```

## Testing Your Customizations

### 1. Local Testing

```bash
# Build
mage build

# Run tests
mage test

# Or specific tests
go test ./pkg/service/... -v

# Run with your custom config
./bin/livekit-server --config config-promobi.yaml
```

### 2. Create Custom Test Files

```go
// pkg/service/promobi_test.go (new file)
package service

import "testing"

func TestPromobitFeatureX(t *testing.T) {
    // your tests
}
```

### 3. Integration Testing

Create a test configuration:

```yaml
# config-promobi-test.yaml
# ... standard LiveKit config ...

promobi:
  enable_feature_x: true
  api_endpoint: "http://localhost:8080"
  api_key: "test-key"
```

## Documenting Your Customizations

### 1. Keep a Changelog

Create or update:

```markdown
# PROMOBI_CUSTOMIZATIONS.md

## Current Customizations

### 1. Room Bandwidth Metrics (Added: 2025-01-15)
- **Files**: `pkg/telemetry/prometheus/packets.go`, `pkg/telemetry/stats.go`
- **Purpose**: Track per-room bandwidth usage for billing
- **Config**: None required, always enabled
- **Conflicts**: Medium risk - modifies core telemetry

### 2. Redis Error Handling (Added: 2025-01-18)
- **Files**: `pkg/service/redismanager.go`
- **Purpose**: Graceful degradation when Redis is unavailable
- **Config**: None required
- **Conflicts**: Low risk - isolated change

### 3. Promobi Custom Metrics (Added: 2025-02-01)
- **Files**: `pkg/telemetry/prometheus/promobi_metrics.go` (new)
- **Purpose**: Track custom business metrics
- **Config**: `promobi.enable_custom_metrics`
- **Conflicts**: None - new file
```

### 2. Comment Your Code

```go
// PROMOBI CUSTOMIZATION: Track custom participant events
// This metric is used for billing and analytics
// Added: 2025-02-01
// Ticket: PROJ-123
func (r *Room) trackPromobitEvent(eventType string) {
    prometheus.IncrementPromobitMetric(eventType)
}
```

### 3. Update README

Keep `FORK_MANAGEMENT_README.md` updated with new customizations.

## What Happens During Next Sync

When you run `./sync-upstream.sh`:

1. **Fetch upstream changes**
2. **Update master** (your customizations are safe, they're not on master)
3. **Rebase promobi-customizations** on new master
   - Your commits are replayed on top of new upstream
   - Conflicts may occur if upstream changed files you modified
   - **Well-isolated customizations = fewer conflicts**

### Handling Conflicts During Sync

If conflicts occur during sync:

```bash
# Sync will stop and show conflicts
./sync-upstream.sh
# ... conflict occurs ...

# Fix conflicts (see CONFLICT_RESOLUTION_GUIDE.md)
# Edit conflicted files
vim pkg/service/room.go

# Mark as resolved
git add pkg/service/room.go

# Continue rebase
git rebase --continue

# Repeat if more conflicts

# When done, run sync again to push
./sync-upstream.sh
```

**If you followed isolation strategies, most conflicts will be simple to resolve.**

## Quick Reference Commands

```bash
# Before adding customizations
./git-helpers.sh status              # Check if synced
./sync-upstream.sh                   # Sync if needed

# Switch to custom branch
git checkout promobi-customizations

# Make changes
# ... edit files ...

# Test
mage build
mage test

# Commit (atomic, descriptive)
git add pkg/service/myfile.go
git commit -m "feat(service): add feature X for Promobi"

# Push
git push origin promobi-customizations

# Deploy
./git-helpers.sh deploy-prep
# or
mage build
```

## Anti-Patterns to Avoid

### ❌ Don't: Commit directly to master

```bash
git checkout master
git commit -m "add feature"  # WRONG!
```

Master should only be updated via `sync-upstream.sh`.

### ❌ Don't: Make massive changes to core files

Modifying 100+ lines in core files = high conflict risk.

### ❌ Don't: Use generic variable names for custom code

```go
// Bad
var customMetric prometheus.Counter

// Good
var promobiCustomMetric prometheus.Counter
```

### ❌ Don't: Skip testing before committing

Always build and test locally first.

### ❌ Don't: Create huge commits

Break features into logical commits for easier conflict resolution.

### ❌ Don't: Ignore conflicts

If sync has conflicts, resolve them immediately. Don't delay.

## Summary

### ✅ DO:
1. Always work on `promobi-customizations` branch
2. Sync with upstream before adding new features
3. Isolate custom code in separate files when possible
4. Use clear commit messages
5. Test locally before pushing
6. Document your customizations
7. Keep commits atomic and focused

### ❌ DON'T:
1. Commit to master directly
2. Make massive changes to core files
3. Skip syncing before adding features
4. Mix multiple features in one commit
5. Push without testing

### 🎯 Golden Rules:
- **Isolation = Fewer conflicts**
- **Small changes = Easier maintenance**
- **Good documentation = Future you will thank you**

## Need Help?

- **For conflict resolution**: See `CONFLICT_RESOLUTION_GUIDE.md`
- **For sync process**: See `FORK_MANAGEMENT_README.md`
- **For CI/CD questions**: See `CI_WORKFLOWS_GUIDE.md`
