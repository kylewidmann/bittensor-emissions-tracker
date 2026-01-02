# Bittensor Emissions Tracker

Tax accounting tracker for Bittensor emissions with support for both **Smart Contract** and **Mining** workflows.

## Overview

This tracker provides separate ledgers for tracking:

1. **Smart Contract Emissions** - Track validator staking rewards and contract income
2. **Mining Emissions** - Track mining rewards from your hotkey

Both trackers use the same underlying logic but maintain separate Google Sheets for clean accounting sub-ledgers.

## Features

- ✅ Track ALPHA income with automatic FMV calculation
- ✅ FIFO/HIFO lot consumption for cost basis tracking
- ✅ ALPHA → TAO conversion tracking (undelegate events)
- ✅ TAO → Brokerage transfer tracking with capital gains
- ✅ Monthly Wave journal entry generation
- ✅ Separate sub-ledgers for smart contract vs mining

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy the example `.env` file and configure your settings:

```bash
cp local/.env.example local/.env
```

Edit `local/.env` with your addresses:

```bash
# Required for both trackers
export BROKER_SS58="your-kraken-deposit-address"
export WALLET_SS58="your-coldkey-address"
export TRACKER_GOOGLE_CREDENTIALS="/path/to/credentials.json"
export TAOSTATS_API_KEY='your-taostats-api-key'

# Smart Contract Tracker
export VALIDATOR_SS58="your-validator-hotkey"
export SMART_CONTRACT_SS58="smart-contract-address"
export TRACKER_SHEET_ID="your-google-sheet-id"

# Mining Tracker (optional - only if tracking mining)
export MINER_HOTKEY_SS58="your-miner-hotkey"
export MINING_TRACKER_SHEET_ID="your-mining-google-sheet-id"
```

### 3. Set Up Google Sheets

Create separate Google Sheets for each tracker:
- One for Smart Contract emissions
- One for Mining emissions (if applicable)

Grant your service account edit access to both sheets.

## Usage

### Smart Contract Tracker

Track validator staking rewards and smart contract income:

```bash
# Run daily check (all transaction types)
python -m emissions_tracker.main --mode auto

# Process only income
python -m emissions_tracker.main --mode income --lookback 30

# Process only sales (ALPHA → TAO conversions)
python -m emissions_tracker.main --mode sales --lookback 14

# Process only transfers (TAO → Kraken)
python -m emissions_tracker.main --mode transfers --lookback 7

# Generate monthly journal entries
python -m emissions_tracker.main --mode journal --month 2025-11
```

### Mining Tracker

Track mining emissions from your hotkey:

```bash
# Run daily check (all transaction types)
python -m emissions_tracker.mining --mode auto

# Process only mining emissions
python -m emissions_tracker.mining --mode income --lookback 30

# Process only undelegations (ALPHA → TAO)
python -m emissions_tracker.mining --mode sales --lookback 14

# Process only transfers (TAO → Kraken)
python -m emissions_tracker.mining --mode transfers --lookback 7

# Generate monthly journal entries
python -m emissions_tracker.mining --mode journal --month 2025-11
```

## Architecture

### Common Flow

Both trackers follow the same flow:

```
ALPHA Income → Track Cost Basis → Undelegate (ALPHA → TAO) → Transfer to Brokerage → Journal Entries
```

### Key Differences

| Feature | Smart Contract Tracker | Mining Tracker |
|---------|----------------------|----------------|
| **Hotkey** | Validator hotkey | Miner hotkey |
| **Income Sources** | Contract + Staking | Mining emissions |
| **Google Sheet** | `TRACKER_SHEET_ID` | `MINING_TRACKER_SHEET_ID` |
| **Wave Account** | Staking Income - Alpha | Mining Income - Alpha |
| **Entry Point** | `emissions_tracker.main` | `emissions_tracker.mining` |

### Shared Components

Both trackers share:
- ✅ Price fetching logic
- ✅ Lot consumption (FIFO/HIFO)
- ✅ ALPHA → TAO conversion tracking
- ✅ TAO transfer tracking
- ✅ Capital gains calculation
- ✅ Journal entry generation

## Google Sheets Structure

Each tracker maintains 5 sheets:

1. **Income** - ALPHA income lots with cost basis
2. **Sales** - ALPHA → TAO conversions (undelegate events)
3. **TAO Lots** - TAO lots created from ALPHA sales
4. **Transfers** - TAO → Brokerage transfers
5. **Journal Entries** - Monthly Wave journal entries

## Lot Consumption Strategy

The tracker supports two lot consumption strategies (set via `LOT_STRATEGY` env var):

- **HIFO** (default) - Highest cost basis first (minimizes capital gains)
- **FIFO** - First in, first out

## Wave Accounting Integration

The tracker generates monthly journal entries for Wave accounting with separate income accounts:

- Smart Contract: `Contractor Income - Alpha` + `Staking Income - Alpha`
- Mining: `Mining Income - Alpha`

All other accounts (asset, gains/losses, fees) are shared.

## Development

### Running Tests

```bash
make test
```

### Code Structure

```
emissions_tracker/
├── main.py           # Smart Contract tracker entry point
├── mining.py         # Mining tracker entry point (NEW)
├── tracker.py        # Core BittensorEmissionTracker class
├── config.py         # Configuration settings
├── models.py         # Data models (AlphaLot, TaoLot, etc.)
└── clients/
    ├── price.py      # Price fetching interface
    ├── wallet.py     # Wallet/blockchain interface
    └── taostats.py   # TaoStats API client
```

## Troubleshooting

### "MINER_HOTKEY_SS58 environment variable is required"

Make sure you've set the mining configuration in your `.env` file:

```bash
export MINER_HOTKEY_SS58="your-miner-hotkey"
export MINING_TRACKER_SHEET_ID="your-mining-sheet-id"
```

### "Could not get TAO price"

The tracker requires TaoStats API access for pricing data. Ensure:
1. `TAOSTATS_API_KEY` is set correctly
2. Your API key is valid and active

### Google Sheets Permission Denied

Ensure your service account has edit access to both Google Sheets (smart contract and mining).

## License

See LICENSE file for details.
