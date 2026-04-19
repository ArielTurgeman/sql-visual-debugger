# SQL Visual Debugger Publish Checklist

This checklist is focused on public release quality for the first marketplace publish.

## Must Have Before Publish

- [x] Narrow marketplace description in `package.json`
- [x] Launch-facing README with clear product boundaries
- [x] Marketplace copy draft aligned with current product behavior
- [x] Supported SQL section written clearly
- [x] Limitations section written clearly
- [x] Safety and trust wording written clearly
- [x] Connection behavior documented
- [x] Password handling documented
- [x] License file added
- [ ] Final extension icon added and referenced from `package.json`
- [ ] Repository URL added to `package.json`
- [ ] Homepage URL added to `package.json`
- [ ] Bugs/support URL added to `package.json`
- [ ] Marketplace screenshots added
- [ ] Confirm publisher account details in the VS Code Marketplace
- [ ] Build fresh publish artifact from clean release state
- [ ] Install the final VSIX locally and smoke-test the release build

## Should Have

- [ ] One primary screenshot showing SQL editor plus debugger panel
- [ ] One screenshot showing step-by-step clause navigation
- [ ] One screenshot showing a filter or grouping explanation state
- [ ] Final pass on command names and capitalization across docs
- [ ] Final pass on empty states and unsupported-query error wording
- [ ] Final audit that README and marketplace copy do not overclaim SQL support

## Optional Polish

- [ ] Short demo GIF if it looks polished and loads quickly
- [ ] `CHANGELOG.md` for first public release notes
- [ ] Separate contributor or architecture doc if deeper internal notes still need a home

## Visual Asset Checklist

### Icon

- [ ] Square icon that remains readable at small size
- [ ] Works on both light and dark marketplace backgrounds
- [ ] Avoids tiny SQL text that becomes unreadable in the extensions list
- [ ] Matches product positioning: trustworthy, technical, clear

### Screenshots

- [ ] Clean sample schema and data
- [ ] No personal or production connection details visible
- [ ] Use realistic but readable example queries
- [ ] Show clause highlighting and intermediate results clearly
- [ ] Crop tightly enough to emphasize the debugger experience

## Release Notes For Remaining Manual Inputs

The repo currently has no configured git remote and no checked-in icon or screenshot assets. Because of that, these fields were not added yet:

- `repository`
- `homepage`
- `bugs`
- `icon`

Add those only after you have the real URLs and final asset paths.
