SECONDS_IN_MINUTE = 60
SECONDS_IN_DAY = 24 * 60 * SECONDS_IN_MINUTE
MIN_DISTANCE_FROM_CURRENT_TIME_FOR_EXTRACTION = 30 * SECONDS_IN_MINUTE
INDICES_WITH_INTERVALS = ["accountsesdt", "tokens", "blocks", "receipts", "transactions", "miniblocks", "rounds", "accountshistory", "scresults", "accountsesdthistory", "scdeploys", "logs", "operations"]
INDICES_WITHOUT_INTERVALS = list(set(["accounts", "rating", "validators", "epochinfo", "tags", "delegators"]) - set(["rating", "tags"]))
