SECONDS_IN_MINUTE = 60
SECONDS_IN_DAY = 24 * 60 * SECONDS_IN_MINUTE
INDEXES_WITH_INTERVALS = ["accountsesdt", "tokens", "blocks", "receipts", "transactions", "miniblocks", "rounds", "accountshistory", "scresults", "accountsesdthistory", "scdeploys", "logs", "operations"]
INDEXES_WITHOUT_INTERVALS = set(["accounts", "rating", "validators", "epochinfo", "tags", "delegators"]) - set(["rating", "tags"])
