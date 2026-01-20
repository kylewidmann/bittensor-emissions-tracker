# Test Data Sanitization Rules

This document outlines the rules for sanitizing Bittensor emissions tracker test data to remove real wallet addresses and blockchain identifiers before committing to source control.

## Overview

Test data files contain real blockchain data that should not be committed to public repositories. This includes:
- SS58 wallet addresses
- Hex wallet addresses
- Block numbers
- Extrinsic IDs
- Transaction hashes
- Composite IDs containing addresses/blocks

## Sanitization Process

### 1. Identify Addresses to Sanitize

#### Config-Referenced Addresses
These addresses are defined in `tests/fixtures/mock_config.py` and must match between config and data:

- **TEST_PAYOUT_COLDKEY_SS58** / **TEST_MINER_COLDKEY_SS58** - Primary coldkey for receiving payouts
- **TEST_VALIDATOR_SS58** - Validator/delegate hotkey (for contract tracking)
- **TEST_MINER_HOTKEY_SS58** - Miner hotkey (for mining tracking)
- **TEST_BROKER_SS58** - Exchange/brokerage address
- **TEST_SMART_CONTRACT_SS58** - Smart contract address (appears in transfer_address field)

#### Other Addresses in Data
Any SS58 addresses that appear in the data but aren't in config should also be sanitized:
- Exchange addresses
- Other wallet addresses
- Contract addresses

### 2. Address Mapping Format

Create fake addresses following this pattern:

```python
# SS58 Format: 5Fake{Purpose}1111111111111111111111111111
# Hex Format:  0x{aaaa}{1111}...{pattern}...{aa}

EXAMPLES:
"5FakeColdkey1111111111111111111111111111"    # Coldkey
"5FakeValidator1111111111111111111111111111"  # Validator
"5FakeMinerHotkey1111111111111111111111111"   # Miner hotkey
"5FakeBroker1111111111111111111111111111"     # Brokerage
"5FakeContract1111111111111111111111111111"   # Smart contract
"5FakeExchange1111111111111111111111111111"   # Exchange
"5FakeAddress11111111111111111111111111111"   # Other address 1
"5FakeAddress21111111111111111111111111111"   # Other address 2
```

Hex addresses should use predictable patterns:
```python
"0xcccc3333333333333333333333333333333333333333333333333333333333cc"  # Coldkey
"0xbbbb2222222222222222222222222222222222222222222222222222222222bb"  # Validator
"0xmmmm1111111111111111111111111111111111111111111111111111111111mm"  # Miner hotkey
"0xaaaa1111111111111111111111111111111111111111111111111111111111aa"  # Broker
"0xdddd4444444444444444444444444444444444444444444444444444444444dd"  # Contract
```

### 3. Block Number Mapping

Transform block numbers to a fake range while maintaining relationships:

```python
# Calculate offset
BLOCK_OFFSET = REAL_FIRST_BLOCK - FAKE_FIRST_BLOCK

# Suggested fake range: 1000000+
def map_block_number(real_block):
    return real_block - BLOCK_OFFSET
```

**Example:**
- Real range: 6327147 - 7222926
- Fake range: 1000000 - 1895779
- Offset: 6327147 - 1000000 = 5327147

### 4. Extrinsic IDs

Format: `{block_number}-{sequence}`

- Update the block number portion to use fake block numbers
- Preserve the sequence number to maintain ordering within blocks

**Example:**
- Real: `7215607-0008`
- Fake: `1888460-0008`

### 5. Transaction Hashes

Replace with predictable fake hashes:

```python
def generate_fake_tx_hash(counter):
    """Generate a fake 66-character transaction hash."""
    return f"0xfake{counter:08x}{'a' * 48}"
```

**Example:**
- Real: `0x75d0a14053459bb69aa4ccfe7d47c25039297676b4baaca0e0afc341b4d2dfa7`
- Fake: `0xfake00000001aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`

### 6. Composite IDs

#### Transfer IDs
Format: `finney-{block_number}-{index}`

Update block number component:
```python
# Real: finney-7215607-0051
# Fake: finney-1888460-0051
```

#### Stake Event IDs
Format: `finney-{netuid}-{delegate_hex}-{nominator_hex}-{block_number}-{index}`

Update all three components (2 hex addresses + block number):
```python
# Real: finney-64-0x5063a3000daa02d892617cda479bc20bb8acf430a8cb167e653c5395b9d4f834-0x44a8ddd000d4fc954b3eb037d7fd2b6edb23fc9d45a655278cc851d89183147e-7222926-47
# Fake: finney-64-0xbbbb2222222222222222222222222222222222222222222222222222222222bb-0xcccc3333333333333333333333333333333333333333333333333333333333cc-1895779-47
```

## Files to Sanitize

### Standard Dataset Files

1. **transfers.json**
   - `to.ss58`, `to.hex`
   - `from.ss58`, `from.hex`
   - `block_number`
   - `extrinsic_id`
   - `transaction_hash`
   - `id` (composite)

2. **stake_events.json**
   - `nominator.ss58`, `nominator.hex`
   - `delegate.ss58`, `delegate.hex`
   - `transfer_address.ss58`, `transfer_address.hex` (if present)
   - `block_number`
   - `extrinsic_id`
   - `id` (complex composite)

3. **stake_balance.json**
   - `hotkey.ss58`, `hotkey.hex`
   - `coldkey.ss58`, `coldkey.hex`
   - `block_number`

4. **account_history.json**
   - `address.ss58`, `address.hex`
   - `block_number`

5. **historical_tao_prices.json**
   - **NO CHANGES** - Contains only dates and prices

### Config File

**tests/fixtures/mock_config.py**
- Update address constants to match sanitized data
- Maintain consistency between config and data files

## Sanitization Script Template

```python
#!/usr/bin/env python3
"""Sanitize test data by replacing real addresses and identifiers."""
import json
from pathlib import Path

# Define your address mappings
ADDRESS_MAPPINGS = {
    # SS58 and corresponding hex addresses
    "REAL_SS58_ADDRESS": "5FakePurpose1111111111111111111111111111",
    "0xREAL_HEX_ADDRESS": "0xffff1111111111111111111111111111111111111111111111111111111111ff",
}

# Calculate block offset
BLOCK_OFFSET = REAL_FIRST_BLOCK - 1000000  # Fake range starts at 1000000

def map_block_number(real_block):
    return real_block - BLOCK_OFFSET

def generate_fake_tx_hash(counter):
    return f"0xfake{counter:08x}{'a' * 48}"

def sanitize_json_file(file_path):
    """Sanitize a JSON file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace all addresses
    for real_addr, fake_addr in ADDRESS_MAPPINGS.items():
        content = content.replace(real_addr, fake_addr)
    
    # Parse and update block numbers, extrinsics, etc.
    data = json.loads(content)
    
    tx_hash_counter = 1
    tx_hash_map = {}
    
    if 'data' in data:
        for record in data['data']:
            # Update block numbers
            if 'block_number' in record:
                old_block = record['block_number']
                new_block = map_block_number(old_block)
                record['block_number'] = new_block
                
                # Update extrinsic_id
                if 'extrinsic_id' in record:
                    parts = record['extrinsic_id'].split('-')
                    if len(parts) == 2:
                        record['extrinsic_id'] = f"{new_block}-{parts[1]}"
                
                # Update id fields
                if 'id' in record:
                    # Handle different ID formats (transfer vs stake_event)
                    # ... implementation depends on format
            
            # Update transaction hashes
            if 'transaction_hash' in record:
                old_hash = record['transaction_hash']
                if old_hash not in tx_hash_map:
                    tx_hash_map[old_hash] = generate_fake_tx_hash(tx_hash_counter)
                    tx_hash_counter += 1
                record['transaction_hash'] = tx_hash_map[old_hash]
    
    # Write back
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)
```

## Verification Checklist

After sanitization, verify:

- [ ] No real SS58 addresses remain in data files
- [ ] No real hex addresses remain in data files
- [ ] Block numbers are in fake range
- [ ] Extrinsic IDs use fake block numbers
- [ ] Transaction hashes use fake format
- [ ] Composite IDs are properly updated
- [ ] Config file addresses match data file addresses
- [ ] All tests still pass
- [ ] Timestamps are preserved (dates/times are okay)
- [ ] Price data is preserved

## Testing

After sanitization, run the full test suite to ensure data integrity:

```bash
# Using poetry
poetry run pytest tests/unit/ -v

# Using make
make test
```

All tests should pass with sanitized data, proving that:
1. Address mapping is consistent
2. Block number relationships are preserved
3. Data correlations remain intact

## Notes

- **Timestamps are safe**: Date/time values can remain as-is since they don't identify wallets
- **Amounts are safe**: RAO/TAO amounts and USD values can remain as-is
- **Preserve relationships**: Block numbers and extrinsic IDs must maintain their correlations
- **Sequential updates**: Always sanitize all related datasets together to maintain consistency
- **Test after sanitization**: Always run tests to verify data integrity

## Examples

See the git history for examples of sanitization:
- Commit: "Sanitize test data for contract tracking"
- Commit: "Sanitize test data for mining tracking"
