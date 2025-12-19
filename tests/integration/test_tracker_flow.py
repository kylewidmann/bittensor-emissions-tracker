from types import SimpleNamespace

import pytest

from emissions_tracker.models import AlphaLot, TaoLot
from emissions_tracker.tracker import BittensorEmissionTracker, RAO_PER_TAO


SALE_TS = 1_763_920_800  # 2025-11-23T18:00:00Z
TRANSFER_TS = SALE_TS + 3_600
SALE_FEE_TAO = 0.002
SALE_FEE_RAO = int(SALE_FEE_TAO * RAO_PER_TAO)
TRANSFER_FEE_TAO = 0.002
TRANSFER_FEE_RAO = int(TRANSFER_FEE_TAO * RAO_PER_TAO)


def column_letter_to_index(letters: str) -> int:
    value = 0
    for ch in letters.upper():
        if not ch.isalpha():
            continue
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


class InMemoryWorksheet:
    def __init__(self, headers):
        self.headers = headers
        self.rows = []

    def seed_records(self, records):
        for record in records:
            row = [record.get(header, "") for header in self.headers]
            self.rows.append(row)

    def get_all_records(self):
        results = []
        for row in self.rows:
            record = {}
            for idx, header in enumerate(self.headers):
                record[header] = row[idx]
            results.append(record)
        return results

    def append_row(self, row):
        padded = list(row) + [""] * max(0, len(self.headers) - len(row))
        self.rows.append(padded[:len(self.headers)])

    def append_rows(self, rows):
        for row in rows:
            self.append_row(row)

    def sort(self, *_args, **_kwargs):
        # Sorting is unnecessary for deterministic in-memory testing
        return

    def update_cell(self, row_num: int, column_letters: str, value):
        column_index = column_letter_to_index(column_letters) - 1
        row_index = row_num - 2  # Skip header row
        if 0 <= row_index < len(self.rows) and 0 <= column_index < len(self.headers):
            self.rows[row_index][column_index] = value


class FakeSpreadsheet:
    def __init__(self):
        self.worksheets = {}

    def register(self, name: str, worksheet: InMemoryWorksheet):
        self.worksheets[name] = worksheet

    def values_batch_update(self, body):
        for update in body.get("data", []):
            sheet_name, cell_range = update["range"].split("!")
            worksheet = self.worksheets[sheet_name]
            start_cell = cell_range.split(":")[0]
            letters = "".join([c for c in start_cell if c.isalpha()])
            numbers = "".join([c for c in start_cell if c.isdigit()])
            row_num = int(numbers)
            value = update["values"][0][0]
            worksheet.update_cell(row_num, letters, value)


class StubPriceClient:
    def __init__(self, price_map):
        self.price_map = price_map

    def get_price_at_timestamp(self, _symbol, timestamp):
        return self.price_map[timestamp]


class StubWalletClient:
    def __init__(self, delegations, transfers):
        self._delegations = delegations
        self._transfers = transfers

    def get_delegations(self, **_kwargs):
        return list(self._delegations)

    def get_transfers(self, **_kwargs):
        return list(self._transfers)


def build_tracker(price_client, wallet_client, alpha_records, *, last_sale_ts, last_transfer_ts):
    tracker = BittensorEmissionTracker.__new__(BittensorEmissionTracker)
    tracker.config = SimpleNamespace(lot_strategy="FIFO")
    tracker.wave_config = SimpleNamespace()
    tracker.price_client = price_client
    tracker.wallet_client = wallet_client
    tracker.wallet_address = "wallet-ss58"
    tracker.validator_address = "validator-ss58"
    tracker.brokerage_address = "brokerage-ss58"
    tracker.smart_contract_address = "contract-ss58"
    tracker.subnet_id = 64

    tracker.alpha_lot_counter = 100
    tracker.sale_counter = 1
    tracker.tao_lot_counter = 1
    tracker.transfer_counter = 1

    tracker.last_contract_income_timestamp = 0
    tracker.last_staking_income_timestamp = 0
    tracker.last_income_timestamp = 0
    tracker.last_sale_timestamp = last_sale_ts
    tracker.last_transfer_timestamp = last_transfer_ts

    income_sheet = InMemoryWorksheet(AlphaLot.sheet_headers())
    income_sheet.seed_records(alpha_records)
    tao_lots_sheet = InMemoryWorksheet(TaoLot.sheet_headers())
    sales_sheet = InMemoryWorksheet([
        "Sale ID", "Date", "Timestamp", "Block", "Alpha Disposed",
        "TAO Received", "TAO Price USD", "USD Proceeds", "Cost Basis",
        "Realized Gain/Loss", "Gain Type", "TAO Expected", "TAO Slippage",
        "Slippage USD", "Slippage Ratio", "Network Fee (TAO)", "Network Fee (USD)",
        "Consumed Lots", "Created TAO Lot ID", "Extrinsic ID", "Notes"
    ])
    transfers_sheet = InMemoryWorksheet([
        "Transfer ID", "Date", "Timestamp", "Block", "TAO Amount",
        "TAO Price USD", "USD Proceeds", "Cost Basis", "Realized Gain/Loss",
        "Gain Type", "Consumed TAO Lots", "Transaction Hash", "Extrinsic ID",
        "Notes", "Total Outflow TAO", "Fee TAO", "Fee Cost Basis USD"
    ])
    journal_sheet = InMemoryWorksheet([])

    sheet = FakeSpreadsheet()
    sheet.register("Income", income_sheet)
    sheet.register("TAO Lots", tao_lots_sheet)
    sheet.register("Sales", sales_sheet)
    sheet.register("Transfers", transfers_sheet)

    tracker.sheet = sheet
    tracker.income_sheet = income_sheet
    tracker.tao_lots_sheet = tao_lots_sheet
    tracker.sales_sheet = sales_sheet
    tracker.transfers_sheet = transfers_sheet
    tracker.journal_sheet = journal_sheet
    return tracker


def test_sales_and_transfers_capture_network_fees():
    price_client = StubPriceClient({
        SALE_TS: 25.0,
        TRANSFER_TS: 30.0,
    })

    delegations = [
        {
            "action": "UNDELEGATE",
            "is_transfer": None,
            "timestamp": SALE_TS,
            "block_number": 999999,
            "alpha": 10.0,
            "tao_amount": 4.002,
            "usd": 100.05,
            "slippage": 0.01,
            "extrinsic_id": "0xSALE",
                "fee": SALE_FEE_RAO,
        }
    ]

    transfers = [
        {
            "timestamp": TRANSFER_TS,
            "block_number": 888888,
            "from": "wallet-ss58",
            "to": "brokerage-ss58",
            "amount": 4.0,
            "transaction_hash": "0xXFER",
            "extrinsic_id": "0xXFER",
                "fee": TRANSFER_FEE_RAO,
        }
    ]

    wallet_client = StubWalletClient(delegations, transfers)

    alpha_records = [
        {
            "Lot ID": "ALPHA-TEST",
            "Date": "2025-11-10 00:00:00",
            "Timestamp": SALE_TS - 10_000,
            "Block": 123456,
            "Source Type": "Contract",
            "Transfer Address": "",
            "Extrinsic ID": "alpha-ext",
            "Alpha Quantity": 10.0,
            "Alpha Remaining": 10.0,
            "USD FMV": 500.0,
            "USD/Alpha": 50.0,
            "TAO Equivalent": 4.1,
            "Long Term Date": "2026-11-10",
            "Status": "Open",
            "Notes": "",
        }
    ]

    tracker = build_tracker(
        price_client,
        wallet_client,
        alpha_records,
        last_sale_ts=SALE_TS - 1000,
        last_transfer_ts=TRANSFER_TS - 1000,
    )

    sales = tracker.process_sales()
    assert len(sales) == 1
    sale = sales[0]
    assert sale.network_fee_tao == pytest.approx(SALE_FEE_TAO)
    assert sale.network_fee_usd == pytest.approx(SALE_FEE_TAO * 25.0)
    assert sale.realized_gain_loss == pytest.approx(-400.0)

    sale_rows = tracker.sales_sheet.get_all_records()
    assert len(sale_rows) == 1
    assert sale_rows[0]["Network Fee (TAO)"] == pytest.approx(SALE_FEE_TAO)
    assert sale_rows[0]["Network Fee (USD)"] == pytest.approx(SALE_FEE_TAO * 25.0)

    transfers = tracker.process_transfers()
    assert len(transfers) == 1
    transfer = transfers[0]
    assert transfer.fee_tao == pytest.approx(TRANSFER_FEE_TAO)
    assert transfer.total_outflow_tao == pytest.approx(transfer.tao_amount + TRANSFER_FEE_TAO)
    assert transfer.fee_cost_basis_usd > 0.0

    transfer_rows = tracker.transfers_sheet.get_all_records()
    assert len(transfer_rows) == 1
    assert transfer_rows[0]["Fee TAO"] == pytest.approx(TRANSFER_FEE_TAO)
    assert transfer_rows[0]["Fee Cost Basis USD"] > 0.0
