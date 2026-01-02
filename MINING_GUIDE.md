# Mining Tracker Quick Start Guide

## What is the Mining Tracker?

The mining tracker is a separate entry point for tracking mining emissions from your Bittensor miner hotkey. It uses the same underlying logic as the smart contract tracker but maintains a separate Google Sheet for clean accounting sub-ledgers.

## Setup

### 1. Add Mining Configuration to .env

```bash
# Your miner hotkey address
export MINER_HOTKEY_SS58="5EAZ2NmDV5Pvz9CwFpRR2p8eXeAmoiX9CcGia31gHKG8AVW9"

# Google Sheet ID for mining tracker (create a new blank sheet)
export MINING_TRACKER_SHEET_ID="your-new-google-sheet-id"

# Optional: Customize Wave account name (defaults to "Mining Income - Alpha")
export WAVE_MINING_INCOME_ACCOUNT="Mining Income - Alpha"
```

### 2. Create Mining Google Sheet

1. Create a new Google Sheet for mining tracker
2. Copy the Sheet ID from the URL: `https://docs.google.com/spreadsheets/d/SHEET_ID_HERE/edit`
3. Grant your service account edit access
4. Add the Sheet ID to your `.env` file as `MINING_TRACKER_SHEET_ID`

### 3. Run Initial Sync

```bash
# First time: use --lookback to specify how far back to process
python -m emissions_tracker.mining --mode auto --lookback 90
```

## Daily Usage

```bash
# Run daily check (processes new transactions since last run)
python -m emissions_tracker.mining --mode auto
```

## What Gets Tracked?

### Mining Emissions (Income)
- **ALPHA tokens earned from mining**
- Detected as **balance increases** in the stake balance history API
- Queried using your **miner's hotkey + your coldkey**
- NOT shown as delegation events - they are direct balance increases
- The tracker calculates: `Emissions = Balance Increase - Manual Delegations + Undelegations`
- Tracked with cost basis = FMV at time of receipt
- Source Type: "Mining"

**Important Difference from Validator Mode:**
- Validator emissions: Query validator hotkey + your coldkey (delegated stake)
- Mining emissions: Query miner hotkey + your coldkey (direct mining rewards)

### Undelegations (Sales)
- ALPHA → TAO conversions when you undelegate
- FIFO/HIFO lot consumption
- Capital gains calculated

### Transfers
- TAO → Kraken (brokerage) transfers
- TAO lot consumption with capital gains
- Same as smart contract tracker

## Comparison: Smart Contract vs Mining

| Feature | Smart Contract | Mining |
|---------|----------------|--------|
| **Command** | `python -m emissions_tracker.main` | `python -m emissions_tracker.mining` |
| **Hotkey** | Validator hotkey (`VALIDATOR_SS58`) | Miner hotkey (`MINER_HOTKEY_SS58`) |
| **Google Sheet** | `TRACKER_SHEET_ID` | `MINING_TRACKER_SHEET_ID` |
| **Income Types** | Contract + Staking | Mining |
| **Wave Income Account** | Staking Income - Alpha | Mining Income - Alpha |
| **Sales Detection** | UNDELEGATE from validator | UNDELEGATE from miner |
| **Transfer Detection** | Same (TAO → Brokerage) | Same (TAO → Brokerage) |

## Sheet Structure

The mining tracker creates 5 sheets (same as smart contract):

1. **Income** - Mining emissions with cost basis
2. **Sales** - ALPHA → TAO undelegations
3. **TAO Lots** - TAO lots from undelegations
4. **Transfers** - TAO → Brokerage transfers
5. **Journal Entries** - Monthly Wave entries

## Example Workflow

1. Mine on your hotkey → ALPHA emissions recorded in "Income" sheet
2. Undelegate ALPHA → TAO → Recorded in "Sales" sheet with capital gains
3. Transfer TAO to Kraken → Recorded in "Transfers" sheet with capital gains
4. Generate monthly journal entries for Wave accounting

## Notes

- The mining tracker is completely independent from the smart contract tracker
- You can run both trackers - they maintain separate ledgers
- All transactions are tracked to the same brokerage address (`BROKER_SS58`)
- The same API (TaoStats) is used for both trackers
- FIFO/HIFO lot strategy applies to both trackers independently

## Troubleshooting

**Q: Do I need to run both trackers?**
A: Only if you have both validator staking AND mining activity you want to track separately.

**Q: Can I use the same Google Sheet?**
A: No, use separate sheets for clean sub-ledger separation.

**Q: What if I mine on multiple hotkeys?**
A: You can run the mining tracker multiple times with different config files, or track them in the same sheet (they'll all show up in the Income sheet with their respective hotkey addresses).

**Q: How do I know which undelegate events are from mining vs validator?**
A: The tracker filters by the hotkey address specified in `MINER_HOTKEY_SS58` vs `VALIDATOR_SS58`.
