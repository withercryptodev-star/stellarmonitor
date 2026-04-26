# StellarMonitor Roadmap

StellarMonitor is a real-time on-chain intelligence platform for the Stellar network. This roadmap describes the milestones we are working toward. Items are ordered by priority, not by fixed dates — we ship when the work is ready.

The roadmap lives in two places: here on GitHub as the source of truth, and at [stellarmonitor.xyz/docs](https://stellarmonitor.xyz/docs) as a visual version for the community.

---

## Milestone 1 — News Hub *(partially live, in testing)*

A curated News section is now live at [stellarmonitor.xyz/news](https://stellarmonitor.xyz/news), replacing the previous Soroban page. Manual publishing through an authenticated admin panel works today. AI-summarized publishing, weekly newsletter, and quarterly research reports are in active development.

**Status**

- ✅ News page live at `/news` with category filters, search, and time-based tabs (Today / Week / All)
- ✅ Admin panel with manual publishing live at `/admin/news/` (basic auth protected)
- ✅ `/soroban` automatically redirects to `/news`
- ⏳ AI-summarized publishing endpoint built, awaiting Anthropic API key activation
- ⏳ Twitter source integration for semi-automated curation
- ⏳ Weekly newsletter "Stellar Pulse" — content pipeline being designed
- ⏳ Quarterly research reports — first edition planned for Q3 2026

**Deliverables**

- News aggregation pipeline (Twitter, blogs, official Stellar channels)
- Editorial layer for curation and context
- Tags and filtering (Protocol, DeFi, RWA, Partnerships, Funding)
- Developer activity tracker — GitHub commits, releases, new protocols on Stellar
- Weekly newsletter "Stellar Pulse" — numbers of the week, top trades, new projects
- Quarterly research reports on Stellar DeFi, RWA, and payments
- RSS feed and email digest option
- Archive with full history

---

## Milestone 2 — Stellar AI Integration

Bring AI directly into the product. A conversational layer that lets anyone ask natural-language questions about Stellar on-chain activity and get instant, sourced answers.

**Deliverables**

- Natural language queries over on-chain data
- AI-generated wallet summaries and risk reports
- Automated anomaly detection and narrative context
- Integration with LLM providers (Anthropic or open-source alternatives)
- Chat interface available on the site and via Telegram bot

---

## Milestone 3 — Token Analytics

Deep-dive pages for individual Stellar assets — price, holders, liquidity, DEX pairs, on-chain history.

**Deliverables**

- Per-token pages (top 100 Stellar assets to start)
- Holder distribution and concentration metrics
- Liquidity across SDEX and Soroban AMMs
- Historical activity and volume charts
- Transfer and trade history feed per asset
- Comparison view for multiple tokens side by side

---

## Milestone 4 — Smart Money Tracking

Follow the wallets that actually know what they are doing. Track top traders, top LPs, and early movers — then surface their activity in real time.

**Deliverables**

- Curated Smart Money list (manually verified + algorithmically discovered)
- Live feed of Smart Money transactions
- Copy-trade signals and alerts
- Historical performance per tracked wallet
- Leaderboards by category (DeFi, NFTs, RWA, memes)

---

## Milestone 5 — Portfolio Tracker

Connect a wallet — see full P&L, every DeFi position, every trade, every asset across the entire Stellar ecosystem.

**Deliverables**

- Wallet connection (Freighter, LOBSTR, WalletConnect)
- Real-time P&L across holdings
- DeFi position tracking (Blend, Aquarius, Soroswap, FxDAO, DeFindex)
- Transaction and trade history timeline
- Unrealized vs realized gains
- CSV export for tax reporting

---

## Milestone 6 — Wallet Intelligence v2 + Alerts Bot v2

Expand the Wallet Intelligence Score into a full reputation and monitoring layer.

**Deliverables**

- Public watchlists — follow a wallet, get notified on any move
- Expanded scoring signals (DeFi usage, contract diversity, age, volume tiers)
- Custom alert rules via Telegram
- Address tagging system (community-contributed labels)
- Webhook support for power users

---

## Milestone 7 — Collaborations with Ecosystem Projects

Build structured partnerships with other Stellar projects.

**Deliverables**

- Partnership framework document and outreach process
- Integrations with major Stellar protocols
- Co-marketing campaigns with aligned projects
- Featured Partner slot on the Ecosystem page
- Embedded StellarMonitor widgets on partner sites
- Joint AMA and event participation

---

## Milestone 8 — Global Security Audit

A full, independent security audit of the StellarMonitor infrastructure.

**Deliverables**

- Comprehensive third-party audit of backend, API, and data handling
- Review of wallet interaction surfaces (watchlists, alerts, webhooks, portfolio tracker)
- Penetration testing of the public site and subdomains
- Dependency and supply-chain audit
- Public security disclosure policy and bug bounty program
- Published audit report with findings and remediation timeline

---

## Milestone 9 — Potential Site & Design Refresh

If the product grows into what we think it can, the current site will eventually need a full visual refresh. Not a priority on its own.

**Deliverables (tentative)**

- Updated visual system and component library
- Refreshed page layouts
- Improved mobile experience
- Unified interaction patterns across features
- Design system published alongside the brand kit

---

## How to suggest ideas

Open an issue on this repo or reply to our pinned tweet on [@StellarMonitor](https://x.com/StellarMonitor). The roadmap is not fixed.

---

*Last updated: April 2026*
