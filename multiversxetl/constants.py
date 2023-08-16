SECONDS_IN_MINUTE = 60
SECONDS_IN_DAY = 24 * 60 * SECONDS_IN_MINUTE
SECONDS_IN_THIRTY_DAYS = 30 * SECONDS_IN_DAY
SECONDS_IN_ONE_YEAR = 365 * SECONDS_IN_DAY
MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION = 30 * SECONDS_IN_MINUTE
INDICES_WITH_INTERVALS = ["accountsesdt", "tokens", "blocks", "receipts", "transactions", "miniblocks", "rounds", "accountshistory", "scresults", "accountsesdthistory", "scdeploys", "logs", "operations"]
INDICES_WITHOUT_INTERVALS = list(set(["accounts", "rating", "validators", "epochinfo", "tags", "delegators"]) - set(["rating", "tags"]))
