SECONDS_IN_MINUTE = 60
SECONDS_IN_DAY = 24 * 60 * SECONDS_IN_MINUTE
INDICES_WITH_INTERVALS = ["accountsesdt", "tokens", "blocks", "receipts", "transactions", "miniblocks", "rounds", "accountshistory", "scresults", "accountsesdthistory", "scdeploys", "logs", "operations"]
INDICES_WITHOUT_INTERVALS = set(["accounts", "rating", "validators", "epochinfo", "tags", "delegators"]) - set(["rating", "tags"])
