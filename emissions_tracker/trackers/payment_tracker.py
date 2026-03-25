from datetime import datetime, timedelta, timezone
from typing import List, Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from emissions_tracker.config import TrackerSettings, WaveAccountSettings
from emissions_tracker.journal import aggregate_monthly_journal_entries
from emissions_tracker.models import (
    DisposalEvent,
    DisposalType,
    JournalEntry,
    LotStatus,
    TaoDeposit,
    TaoLot,
    TaoStatsDelegation,
    TaoStatsTransfer,
    TaoTransfer,
)
from emissions_tracker.trackers.bittensor_tracker import (
    SECONDS_PER_DAY,
    BittensorTracker,
)
from emissions_tracker.utils import initialize_sheets

RAO_PER_TAO = 10**9

DEPOSITS_SHEET = "Deposits"
TAO_LOTS_SHEET = "TAO Lots"
TRANSFERS_SHEET = "Transfers"
JOURNAL_SHEET = "Journal Entries"

SHEET_CONFIGS = [
    (DEPOSITS_SHEET, TaoDeposit.sheet_headers()),
    (TAO_LOTS_SHEET, TaoLot.sheet_headers()),
    (TRANSFERS_SHEET, TaoTransfer.sheet_headers()),
    (JOURNAL_SHEET, JournalEntry.sheet_headers()),
]


class PaymentTracker(BittensorTracker):
    """Tracker for a payment-only wallet.

    Handles a simple flow: receive TAO payments from a third party,
    then transfer those funds to a brokerage to cash out.  No ALPHA,
    no staking, no delegations.
    """

    def _initialize(self):
        """Load config, connect to Google Sheets, and hydrate in-memory state."""
        self.config = TrackerSettings()
        self.wave_config = WaveAccountSettings()

        self.coldkey_ss58 = self.config.payment_coldkey_ss58
        self.sheet_id = self.config.payment_tracker_sheet_id
        self.brokerage_ss58 = self.config.brokerage_ss58

        # Not used by this tracker, but the base class disposal framework
        # references these for delegation fetches.  We override
        # _fetch_disposal_events so they are never actually called.
        self.hotkey_ss58 = ""
        self.subnet_id = self.config.subnet_id

        print("Initializing Payment tracker:")
        print(f"  Payment Wallet: {self.coldkey_ss58}")
        print(f"  Brokerage: {self.brokerage_ss58}")

        # Connect to Google Sheets
        print("  Connecting to Google Sheets...")
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.config.tracker_google_credentials, scope
        )
        self.sheets_client = gspread.authorize(creds)
        self.sheet = self._open_sheet_with_retry(self.sheet_id)
        print("  ✓ Connected to Google Sheets")

        print("  Initializing sheets...")
        self._init_sheets()
        print("  ✓ Sheets initialized")

        print("  Loading state from sheets...")
        self._load_state()
        print("  ✓ State loaded")

        print("  Loading counters...")
        self._load_counters()
        print("  ✓ Counters loaded")

        print("  Loading data into memory...")
        self.deposits: List[TaoDeposit] = []
        self.tao_lots: List[TaoLot] = []
        self.transfers: List[TaoTransfer] = []
        # No ALPHA lots, sales, or expenses for payment tracker
        self.alpha_lots: list = []
        self.sales: list = []
        self._load_all_data_from_sheets()
        print(
            f"  ✓ Loaded {len(self.deposits)} deposits, "
            f"{len(self.tao_lots)} TAO lots, "
            f"{len(self.transfers)} transfers"
        )

    def _init_sheets(self):
        """Ensure all worksheet tabs exist with correct headers and store references."""
        initialize_sheets(self.sheet, SHEET_CONFIGS)
        self.deposits_sheet = self.sheet.worksheet(DEPOSITS_SHEET)
        self.tao_lots_sheet = self.sheet.worksheet(TAO_LOTS_SHEET)
        self.transfers_sheet = self.sheet.worksheet(TRANSFERS_SHEET)
        self.journal_sheet = self.sheet.worksheet(JOURNAL_SHEET)

    def _load_state(self):
        """Read existing sheet data to determine where processing last left off.

        Sets ``last_deposit_timestamp`` and ``last_disposal_timestamp`` so
        subsequent runs only fetch new events from TaoStats.
        """
        self.last_deposit_timestamp = 0
        self.last_income_timestamp = 0
        self.last_disposal_timestamp = 0

        try:
            records = self._get_records_with_retry(self.deposits_sheet)
            if records:
                self.last_deposit_timestamp = max(r["Timestamp"] for r in records)
                self.last_income_timestamp = self.last_deposit_timestamp
        except Exception as e:
            print(f"  Warning: Could not load deposit state: {e}")

        try:
            records = self._get_records_with_retry(self.transfers_sheet)
            if records:
                self.last_disposal_timestamp = max(r["Timestamp"] for r in records)
        except Exception as e:
            print(f"  Warning: Could not load transfer state: {e}")

    def _get_regen_disposal_sheets(self):
        """Return disposal sheets to clear during regeneration.

        Only the Transfers sheet contains disposal data for this tracker.
        """
        return [
            (self.transfers_sheet, "Transfers", "Timestamp"),
        ]

    def _reset_regen_timestamps(self, start_time: int) -> None:
        """Roll last-processed timestamps back so processing resumes from ``start_time``."""
        cutoff = start_time - 1
        if self.last_deposit_timestamp >= start_time:
            self.last_deposit_timestamp = cutoff
        if self.last_income_timestamp >= start_time:
            self.last_income_timestamp = cutoff
        if self.last_disposal_timestamp >= start_time:
            self.last_disposal_timestamp = cutoff

    def _load_counters(self):
        """Derive next auto-increment IDs (DEP-NNNN, TAO-NNNN, XFER-NNNN) from existing sheet rows."""
        try:
            records = self._get_records_with_retry(self.deposits_sheet)
            if records:
                ids = [
                    r["Deposit ID"]
                    for r in records
                    if r.get("Deposit ID", "").startswith("DEP-")
                ]
                self.deposit_counter = (
                    max([int(i.split("-")[1]) for i in ids], default=0) + 1
                )
            else:
                self.deposit_counter = 1
        except Exception:
            self.deposit_counter = 1

        try:
            records = self._get_records_with_retry(self.tao_lots_sheet)
            if records:
                ids = [
                    r["TAO Lot ID"]
                    for r in records
                    if r.get("TAO Lot ID", "").startswith("TAO-")
                ]
                self.tao_lot_counter = (
                    max([int(i.split("-")[1]) for i in ids], default=0) + 1
                )
            else:
                self.tao_lot_counter = 1
        except Exception:
            self.tao_lot_counter = 1

        try:
            records = self._get_records_with_retry(self.transfers_sheet)
            if records:
                ids = [
                    r["Transfer ID"]
                    for r in records
                    if r.get("Transfer ID", "").startswith("XFER-")
                ]
                self.transfer_counter = (
                    max([int(i.split("-")[1]) for i in ids], default=0) + 1
                )
            else:
                self.transfer_counter = 1
        except Exception:
            self.transfer_counter = 1

        print(
            f"  Counters: DEP={self.deposit_counter}, TAO={self.tao_lot_counter}, XFER={self.transfer_counter}"
        )

    def _load_all_data_from_sheets(self):
        """Hydrate in-memory lists from the Google Sheets backing store."""
        try:
            for record in self._get_records_with_retry(self.deposits_sheet):
                self.deposits.append(TaoDeposit.from_record(record))
        except Exception as e:
            print(f"  Warning: Could not load deposits: {e}")

        try:
            for record in self._get_records_with_retry(self.tao_lots_sheet):
                self.tao_lots.append(TaoLot.from_record(record))
        except Exception as e:
            print(f"  Warning: Could not load TAO lots: {e}")

        try:
            for record in self._get_records_with_retry(self.transfers_sheet):
                self.transfers.append(TaoTransfer.from_record(record))
        except Exception as e:
            print(f"  Warning: Could not load transfers: {e}")

    # -------------------------------------------------------------------------
    # ID Generation
    # -------------------------------------------------------------------------

    def _next_deposit_id(self) -> str:
        dep_id = f"DEP-{self.deposit_counter:04d}"
        self.deposit_counter += 1
        return dep_id

    def _next_tao_lot_id(self) -> str:
        lot_id = f"TAO-{self.tao_lot_counter:04d}"
        self.tao_lot_counter += 1
        return lot_id

    def _next_transfer_id(self) -> str:
        transfer_id = f"XFER-{self.transfer_counter:04d}"
        self.transfer_counter += 1
        return transfer_id

    # -------------------------------------------------------------------------
    # Main Processing
    # -------------------------------------------------------------------------

    def run(self, start_time: Optional[int] = None, end_time: Optional[int] = None):
        """Run the payment tracker.

        1. Income phase: inbound TAO transfers -> TaoDeposit + TaoLot
        2. Disposal phase: outbound TAO transfers to brokerage -> TaoTransfer
        3. Write to sheets
        """
        self.process_payment_income(start_time=start_time, end_time=end_time)
        self.process_disposals(start_time=start_time, end_time=end_time)
        self.write_all_data_to_sheets()

    # -------------------------------------------------------------------------
    # Income Processing (inbound TAO payments)
    # -------------------------------------------------------------------------

    def process_payment_income(
        self, start_time: Optional[int] = None, end_time: Optional[int] = None
    ) -> list:
        """Fetch inbound TAO transfers and create deposit + lot records."""
        start_time, end_time = self._resolve_time_window(
            "payment income", self.last_deposit_timestamp, start_time, end_time
        )

        if start_time is None:
            print("ℹ️  Payment income already fully processed for requested time range")
            return []

        deposit_transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            receiver=self.coldkey_ss58,
        )

        if not deposit_transfers:
            print("ℹ️  No new payment deposits found")
            return []

        print(f"  Found {len(deposit_transfers)} inbound transfers, fetching prices on demand...")

        new_deposits, new_lots = self._create_payment_deposits(deposit_transfers)

        if new_deposits and new_lots:
            self.deposits.extend(new_deposits)
            self.tao_lots.extend(new_lots)
            self.last_deposit_timestamp = max(d.timestamp for d in new_deposits)
            self.last_income_timestamp = max(
                self.last_income_timestamp, self.last_deposit_timestamp
            )
            print(
                f"\n✓ Created {len(new_deposits)} payment deposits and {len(new_lots)} TAO lots"
            )
        else:
            print("ℹ️  No valid payment deposits to process")

        return new_deposits

    def _create_payment_deposits(
        self, transfers: list[TaoStatsTransfer]
    ) -> tuple[list[TaoDeposit], list[TaoLot]]:
        """Convert inbound TaoStats transfers into paired TaoDeposit + TaoLot records.

        Each incoming TAO payment becomes:
        - A ``TaoDeposit`` recording the income event and its USD fair-market value.
        - A ``TaoLot`` establishing the cost basis for future disposal/transfer.
        """
        deposits: list[TaoDeposit] = []
        tao_lots: list[TaoLot] = []

        for transfer in transfers:
            try:
                tao_price = self.price_client.get_price_at_timestamp(
                    "TAO", transfer.timestamp_unix
                )
            except Exception as e:
                print(
                    f"  Warning: Could not get price for deposit at {transfer.timestamp}: {e}"
                )
                continue

            tao_amount = transfer.amount_rao / RAO_PER_TAO
            usd_fmv = tao_amount * tao_price

            tao_lot_id = self._next_tao_lot_id()
            tao_lot = TaoLot(
                lot_id=tao_lot_id,
                timestamp=transfer.timestamp_unix,
                block_number=transfer.block_number,
                source_sale_id="",
                rao=transfer.amount_rao,
                rao_remaining=transfer.amount_rao,
                usd_per_tao=tao_price,
                usd_basis=usd_fmv,
                status=LotStatus.OPEN,
                extrinsic_id=transfer.extrinsic_id,
                notes=f"Payment from {transfer.from_address.ss58[:8]}...",
            )
            tao_lots.append(tao_lot)

            deposit = TaoDeposit(
                deposit_id=self._next_deposit_id(),
                timestamp=transfer.timestamp_unix,
                block_number=transfer.block_number,
                from_address=transfer.from_address.ss58,
                tao_amount=tao_amount,
                tao_amount_rao=transfer.amount_rao,
                tao_price_usd=tao_price,
                usd_fmv=usd_fmv,
                created_tao_lot_id=tao_lot_id,
                category="Payment",
                extrinsic_id=transfer.extrinsic_id,
                notes=f"Payment from {transfer.from_address.ss58[:8]}... at block {transfer.block_number}",
            )
            deposits.append(deposit)

        return deposits, tao_lots

    # -------------------------------------------------------------------------
    # Disposal Processing (outbound TAO transfers to brokerage)
    # -------------------------------------------------------------------------

    def _prefetch_disposal_prices(self, disposal_events) -> None:
        """Skip bulk price pre-fetch; per-event lookups are cheaper for sparse disposals."""
        print(f"  {len(disposal_events)} disposal events — prices will be fetched on demand")

    def _fetch_disposal_events(self, start_time, end_time):
        """Fetch raw blockchain data for disposal processing.

        Overrides the base class to skip delegation lookups (this wallet
        never stakes). Returns only outbound TAO transfers.
        """
        all_transfers = self.wallet_client.get_transfers(
            account_address=self.coldkey_ss58,
            start_time=start_time,
            end_time=end_time,
            sender=self.coldkey_ss58,
        )
        return [], all_transfers

    def _create_disposal_events(
        self,
        all_delegations: List[TaoStatsDelegation],
        all_transfers: List[TaoStatsTransfer],
    ) -> List[DisposalEvent]:
        """Filter outbound transfers to only those sent to the brokerage address.

        Each qualifying transfer becomes a ``DisposalEvent`` that will consume
        TAO lots (FIFO/HIFO) and record the resulting capital gain or loss.
        """
        disposal_events: List[DisposalEvent] = []

        for t in all_transfers:
            if t.to_address and t.to_address.ss58 == self.brokerage_ss58:
                disposal_events.append(
                    DisposalEvent(
                        timestamp=t.timestamp_unix,
                        disposal_type=DisposalType.TRANSFER,
                        event=t,
                        process=lambda t=t: self._create_tao_transfer(t),
                    )
                )

        return disposal_events

    # -------------------------------------------------------------------------
    # Journal Entry Generation
    # -------------------------------------------------------------------------

    def generate_monthly_journal_entries(
        self, year_month: Optional[str] = None
    ) -> List[JournalEntry]:
        """Build double-entry journal entries for a single month and append to the Journal sheet.

        Deposits are credited to the ``payment_income_account`` (not Business
        Checking) because inbound TAO represents income, not a fiat purchase.
        """
        if not year_month:
            today = datetime.now()
            year_month = f"{today.year}-{today.month:02d}"

        try:
            period_start = datetime.strptime(year_month, "%Y-%m").replace(
                tzinfo=timezone.utc
            )
        except ValueError as exc:
            raise ValueError(
                f"Invalid month format '{year_month}', expected YYYY-MM"
            ) from exc

        first_day_next_month = (
            period_start.replace(day=28) + timedelta(days=4)
        ).replace(day=1)
        start_ts = int(period_start.timestamp())
        end_ts = int(first_day_next_month.timestamp())

        print(f"\n{'='*60}")
        print(f"Generating journal entries for {year_month}...")
        print(f"{'='*60}")

        deposit_records = self.deposits_sheet.get_all_records()
        transfer_records = self.transfers_sheet.get_all_records()

        entries, summary = aggregate_monthly_journal_entries(
            year_month,
            income_records=[],
            sales_records=[],
            expense_records=[],
            transfer_records=transfer_records,
            deposit_records=deposit_records,
            wave_config=self.wave_config,
            start_ts=start_ts,
            end_ts=end_ts,
            deposit_income_account=self.wave_config.payment_income_account,
        )

        if entries:
            rows = [entry.to_sheet_row() for entry in entries]
            self._append_rows_with_retry(self.journal_sheet, rows)
            self._print_journal_summary(year_month, len(entries), summary)
        else:
            print(f"  No data for {year_month}, skipping")

        return entries

    def generate_yearly_journal_entries(self, year: int) -> List[JournalEntry]:
        """Generate and write journal entries for every month in the given year."""
        print(f"\n{'='*60}")
        print(f"Generating journal entries for entire year {year}")
        print(f"{'='*60}")

        print("\nLoading data from sheets...")
        deposit_records = self.deposits_sheet.get_all_records()
        transfer_records = self.transfers_sheet.get_all_records()
        print("✓ Data loaded\n")

        all_entries: list[JournalEntry] = []
        all_rows: list[list] = []

        for month in range(1, 13):
            year_month = f"{year}-{month:02d}"
            try:
                period_start = datetime.strptime(year_month, "%Y-%m").replace(
                    tzinfo=timezone.utc
                )
                first_day_next_month = (
                    period_start.replace(day=28) + timedelta(days=4)
                ).replace(day=1)
                start_ts = int(period_start.timestamp())
                end_ts = int(first_day_next_month.timestamp())
            except ValueError:
                continue

            print(f"\n{'='*60}")
            print(f"Generating journal entries for {year_month}...")
            print(f"{'='*60}")

            try:
                entries, summary = aggregate_monthly_journal_entries(
                    year_month,
                    income_records=[],
                    sales_records=[],
                    expense_records=[],
                    transfer_records=transfer_records,
                    deposit_records=deposit_records,
                    wave_config=self.wave_config,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    deposit_income_account=self.wave_config.payment_income_account,
                )

                if entries:
                    for entry in entries:
                        all_rows.append(entry.to_sheet_row())
                        all_entries.append(entry)
                    self._print_journal_summary(year_month, len(entries), summary)
                else:
                    print(f"  No data for {year_month}, skipping")

            except ValueError as e:
                print(f"  Skipping {year_month}: {e}")
                continue

        if all_rows:
            print(f"\nWriting {len(all_rows)} journal entries to sheet...")
            self._append_rows_with_retry(self.journal_sheet, all_rows)
            print("✓ Journal entries written")
        else:
            print("\nNo journal entries to write")

        print(f"\n✓ Generated {len(all_entries)} total journal entries for {year}")
        return all_entries

    def _print_journal_summary(self, year_month: str, entry_count: int, summary: dict):
        """Pretty-print the per-account debit/credit totals for a month's journal entries."""
        print(f"\n📊 Journal Summary for {year_month}:")
        print(f"  Total entries: {entry_count}")
        if summary:
            for key, value in summary.items():
                if isinstance(value, (int, float)):
                    print(f"  {key}: ${value:,.2f}")
                else:
                    print(f"  {key}: {value}")

    # -------------------------------------------------------------------------
    # Sheet Management
    # -------------------------------------------------------------------------

    def write_all_data_to_sheets(self):
        """Overwrite Deposits, TAO Lots, and Transfers sheets with in-memory data.

        Clears existing data rows (preserving headers) then appends the
        current sorted lists. Called at the end of each ``run()`` cycle.
        """
        print("\n💾 Writing all data to sheets...")

        self.deposits.sort(key=lambda x: x.timestamp)
        self.tao_lots.sort(key=lambda x: x.timestamp)
        self.transfers.sort(key=lambda x: x.timestamp)

        sheets_to_clear = [
            (self.deposits_sheet, "Deposits", len(self.deposits)),
            (self.tao_lots_sheet, "TAO Lots", len(self.tao_lots)),
            (self.transfers_sheet, "Transfers", len(self.transfers)),
        ]

        for worksheet, name, _count in sheets_to_clear:
            try:
                all_values = worksheet.get_all_values()
                if len(all_values) > 1:
                    last_row = len(all_values)
                    worksheet.batch_clear([f"A2:Z{last_row}"])
            except Exception as e:
                print(f"  Warning: Could not clear {name} sheet: {e}")

        if self.deposits:
            rows = [d.to_sheet_row() for d in self.deposits]
            self._append_rows_with_retry(self.deposits_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} deposit records")

        if self.tao_lots:
            rows = [lot.to_sheet_row() for lot in self.tao_lots]
            self._append_rows_with_retry(self.tao_lots_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} TAO lot records")

        if self.transfers:
            rows = [t.to_sheet_row() for t in self.transfers]
            self._append_rows_with_retry(self.transfers_sheet, rows)
            print(f"  ✓ Wrote {len(rows)} transfer records")

        print("✓ All data written to sheets\n")

    def clear_all_sheets(self):
        """Delete all data rows from every sheet and reset in-memory state and counters."""
        print("\n🗑️  Clearing all sheets...")

        sheets_to_clear = [
            (self.deposits_sheet, "Deposits"),
            (self.tao_lots_sheet, "TAO Lots"),
            (self.transfers_sheet, "Transfers"),
            (self.journal_sheet, "Journal Entries"),
        ]

        for worksheet, name in sheets_to_clear:
            try:
                all_values = worksheet.get_all_values()
                if len(all_values) > 1:
                    last_row = len(all_values)
                    worksheet.batch_clear([f"A2:Z{last_row}"])
                    print(f"  ✓ Cleared {name} sheet ({last_row - 1} rows)")
                else:
                    print(f"  ✓ {name} sheet already empty")
            except Exception as e:
                print(f"  Warning: Could not clear {name} sheet: {e}")

        self.deposits = []
        self.tao_lots = []
        self.transfers = []

        self.deposit_counter = 1
        self.tao_lot_counter = 1
        self.transfer_counter = 1

        self.last_deposit_timestamp = 0
        self.last_income_timestamp = 0
        self.last_disposal_timestamp = 0

        print("✓ All sheets cleared\n")

    def create_opening_lots(self, start_time: int):
        """Create an opening TAO lot from account history before start_time."""
        print(f"\nCreating opening lots for start date...")
        self._create_opening_tao_lot(start_time)
        print("✓ Opening lots created\n")

    def _create_opening_tao_lot(self, start_time: int):
        """Query the wallet balance just before ``start_time`` and create a single lot for it.

        This establishes the cost basis for any TAO already held when tracking
        begins, using the TAO price at the time of the balance snapshot.
        """
        print("  Creating opening TAO lot from account history...")

        prev_day_start = start_time - (2 * SECONDS_PER_DAY)
        prev_day_end = start_time - 1

        account_histories = self.wallet_client.get_account_history(
            address=self.coldkey_ss58, start_time=prev_day_start, end_time=prev_day_end
        )

        if not account_histories:
            print(
                "    No account history found for previous day, skipping opening TAO lot"
            )
            return

        opening_history = account_histories[-1]
        tao_balance_rao = opening_history.balance_free_rao

        if tao_balance_rao == 0:
            print("    Opening balance is zero, skipping opening TAO lot")
            return

        tao_price = self.price_client.get_price_at_timestamp(
            "TAO", opening_history.timestamp_unix
        )

        tao_amount = tao_balance_rao / RAO_PER_TAO
        usd_basis = tao_amount * tao_price

        lot = TaoLot(
            lot_id=self._next_tao_lot_id(),
            timestamp=opening_history.timestamp_unix,
            block_number=opening_history.block_number,
            rao=tao_balance_rao,
            rao_remaining=tao_balance_rao,
            usd_basis=usd_basis,
            usd_per_tao=tao_price,
            source_sale_id="",
            extrinsic_id="",
            status=LotStatus.OPEN,
            notes="Opening balance lot",
        )

        self.tao_lots.append(lot)
        print(
            f"  Created opening TAO lot: {lot.lot_id} with {tao_amount:.4f} TAO (${usd_basis:.2f})"
        )

    def regenerate_from(self, start_time: int, end_time: Optional[int] = None) -> None:
        """Re-process events from ``start_time`` by rewinding sheet and in-memory state.

        Resets TAO lots to their pre-``start_time`` balances, deletes deposit
        and transfer rows at or after ``start_time``, and rolls back timestamps
        so the next ``run()`` re-fetches and reprocesses that window.
        """
        import time as _time
        from datetime import timezone as _tz

        resolved_end = end_time if end_time is not None else int(_time.time())
        print(
            f"\n⚠️  Regenerating from {datetime.fromtimestamp(start_time, tz=_tz.utc).date()} "
            f"to {datetime.fromtimestamp(resolved_end, tz=_tz.utc).date()}..."
        )

        self._reset_tao_lots_from(self.tao_lots_sheet, start_time, end_time=None)

        for worksheet, label, ts_col in self._get_regen_disposal_sheets():
            self._delete_sheet_rows_where_timestamp_gte(
                worksheet, ts_col, start_time, label
            )

        self._delete_sheet_rows_where_timestamp_gte(
            self.deposits_sheet, "Timestamp", start_time, "Deposits"
        )

        if end_time is not None:
            self._delete_sheet_rows_where_timestamp_gt(
                self.tao_lots_sheet, "Timestamp", end_time, "TAO Lots"
            )
            self._delete_sheet_rows_where_timestamp_gt(
                self.deposits_sheet, "Timestamp", end_time, "Deposits"
            )

        self._reset_regen_timestamps(start_time)
        print("✓ Regenerate complete\n")
