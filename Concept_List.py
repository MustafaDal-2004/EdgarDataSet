# concept_list.py
# -----------------------------
# Driver concept keywords for 3-statement modeling

concept_keywords = {
    "Revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax"],
    "OperatingIncome": ["OperatingIncomeLoss"],
    "NetIncome": ["NetIncomeLoss"],
    "Capex": ["PropertyPlantAndEquipment", "Acquisitions", "CapitalExpenditures",
              "PropertyPlantAndEquipmentNet", "PaymentsToAcquireProductiveAssets"],
    "Depreciation": ["DepreciationDepletionAndAmortization", "Depreciation"],
    "AccountsReceivable": ["AccountsReceivable", "Receivables"],
    "Inventory": ["Inventory"],
    "AccountsPayable": ["AccountsPayable", "Payables"],
    "Cash": ["CashAndCashEquivalents", "CashCashEquivalents"],
    "Debt": ["Debt", "NotesPayable", "LongTermDebt"]
}