
### Smart Contract Transactions

1. Calculating contract income.
    1. Get all delegation events.
    2. Find all events where
        1. is_transfer = True and transfer_address.ss58 = smart contract address
        2. nominator is my wallet ss58
        3. delegate is the validator ss58
    3. For each of these events
        1. Create an alpha lot to reprsent the `alpha` property in the event
        2. Track the cost basis using the `usd` property
2. Tracking alpha sales
    1. Final all delegation events where
        1. action = UNDELEGATE
        2. is_transfer = null and transfer_address = null
        3. The nominator is my wallet ss58
        4. the delegate is the validator ss58
    2. Reduce alpha lots according to strategiy (FIFO vs HIFO) using the `alpha` property from the delegation event
    3. For each UNDELEGATE event
        1. Get the associated fee transfer. These are linked via the extrisic ID. 
        2. Create TAO lot from the sale based on the delegations `amount` - the transfer `amount` - the transfer `fee`
3. Tracking expenses
    1. Get all delegation events where
        1. action = UNDELEGATE
        2. is_transfer = True and transfer_address != the validator ss58
        3. The nominator is my wallet ss58
        4. the delegate is the validator ss58
    1. Reduce alpha lots according to strategiy (FIFO vs HIFO) using the `alpha` property from the delegation event
4. Calculating staking emissions
    1. Get all delegations
    2. Get the stake balance history for the wallet
    2. For each day in range
        1. Get the balance from midnight of the current day and midnight of the preivous day
        2. Get all DELEGATE actions where the nominator is my wallet ss58
        3. Get all UNDELEGATE actiosn where the nomiator is my wallet ss58
        4. Calculate emissions as current midnight balance - preivous day midnight balance - SUM(all delegate `alpha` properties) + SUM(all undelegate `alpha` properties)
5. Processing TAO Transfers
    1. Get all transfers where
        1. from is my wallet ss58
        2. to is the brokerage ss58
        3. For each transfer
            1. Reduce the TAO lots according to strategy (FIFO vs HIFO)
                1. Reduce the lot by transfer `amount` + `fee`

### Mining Transactions

1. Calculating staking emissions
    1. Get all delegations
    2. Get the stake balance history for the wallet
    2. For each day in range
        1. Get the balance from midnight of the current day and midnight of the preivous day
        2. Get all DELEGATE actions where the nominator is my wallet ss58
        3. Get all UNDELEGATE actiosn where the nomiator is my wallet ss58
        4. Calculate emissions as current midnight balance - preivous day midnight balance - SUM(all delegate `alpha` properties) + SUM(all undelegate `alpha` properties)
2. Tracking alpha sales
    1. Final all delegation events where
        1. action = UNDELEGATE
        2. is_transfer = null and transfer_address = null
        3. The nominator is my wallet ss58
        4. the delegate is the validator ss58
    2. Reduce alpha lots according to strategiy (FIFO vs HIFO) using the `alpha` property from the delegation event
    3. For each UNDELEGATE event
        1. Get the associated fee transfer. These are linked via the extrisic ID. 
        2. Create TAO lot from the sale based on the delegations `amount` - the transfer `amount` - the transfer `fee`
3. Tracking expenses
    1. Get all delegation events where
        1. action = UNDELEGATE
        2. is_transfer = True and transfer_address != the validator ss58
        3. The nominator is my wallet ss58
        4. the delegate is the validator ss58
    1. Reduce alpha lots according to strategiy (FIFO vs HIFO) using the `alpha` property from the delegation event
4. Processing TAO Transfers
    1. Get all transfers where
        1. from is my wallet ss58
        2. to is the brokerage ss58
        3. For each transfer
            1. Reduce the TAO lots according to strategy (FIFO vs HIFO)
                1. Reduce the lot by transfer `amount` + `fee`