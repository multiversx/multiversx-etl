from multiversxetl.schema import map_elastic_search_schema_to_bigquery_schema


def test_map_elastic_search_schema_to_bigquery_schema_basic():
    input_schema = {
        "mappings": {
            "properties": {
                "foo": {
                    "type": "keyword"
                },
                "bar": {
                    "type": "double"
                }
            }
        }
    }

    expected_output_schema = [
        {
            "name": "_id",
            "type": "STRING"
        },
        {
            "name": "foo",
            "type": "STRING"
        },
        {
            "name": "bar",
            "type": "FLOAT64"
        }
    ]

    actual_output_schema = map_elastic_search_schema_to_bigquery_schema(input_schema)

    assert actual_output_schema == expected_output_schema


def test_map_elastic_search_schema_to_bigquery_schema_with_repeated():
    input_schema = {
        "mappings": {
            "properties": {
                "foo": {
                    "type": "keyword",
                    "isArray": True
                }
            }
        }
    }

    expected_output_schema = [
        {
            "name": "_id",
            "type": "STRING"
        },
        {
            "name": "foo",
            "mode": "REPEATED",
            "type": "STRING"
        }
    ]

    actual_output_schema = map_elastic_search_schema_to_bigquery_schema(input_schema)

    assert actual_output_schema == expected_output_schema


def test_map_elastic_search_schema_to_bigquery_schema_with_record():
    input_schema = {
        "mappings": {
            "properties": {
                "foobar": {
                    "properties": {
                        "foo": {
                            "type": "keyword"
                        },
                        "bar": {
                            "type": "double"
                        }
                    },
                    "isObject": True
                }
            }
        }
    }

    expected_output_schema = [
        {
            "name": "_id",
            "type": "STRING"
        },
        {
            "name": "foobar",
            "fields": [
                {
                    "name": "foo",
                    "type": "STRING"
                },
                {
                    "name": "bar",
                    "type": "FLOAT64"
                }
            ],
            "type": "RECORD"
        }
    ]

    actual_output_schema = map_elastic_search_schema_to_bigquery_schema(input_schema)

    assert actual_output_schema == expected_output_schema


def test_map_elastic_search_schema_to_bigquery_schema_on_accounts():
    input_schema = {
        "mappings": {
            "properties": {
                "address": {
                    "type": "keyword"
                },
                "userName": {
                    "type": "keyword"
                },
                "balance": {
                    "type": "keyword"
                },
                "balanceNum": {
                    "type": "double"
                },
                "nonce": {
                    "type": "double"
                },
                "shardID": {
                    "type": "long"
                },
                "timestamp": {
                    "format": "epoch_second",
                    "type": "date"
                },
                "developerRewards": {
                    "type": "keyword"
                },
                "developerRewardsNum": {
                    "type": "double"
                },
                "totalBalanceWithStake": {
                    "type": "keyword"
                },
                "totalBalanceWithStakeNum": {
                    "type": "double"
                },
                "currentOwner": {
                    "type": "keyword"
                }
            }
        }
    }

    expected_output_schema = [
        {
            "name": "_id",
            "type": "STRING"
        },
        {
            "name": "address",
            "type": "STRING"
        },
        {
            "name": "userName",
            "type": "STRING"
        },
        {
            "name": "balance",
            "type": "STRING"
        },
        {
            "name": "balanceNum",
            "type": "FLOAT64"
        },
        {
            "name": "nonce",
            "type": "FLOAT64"
        },
        {
            "name": "shardID",
            "type": "NUMERIC"
        },
        {
            "name": "timestamp",
            "type": "TIMESTAMP"
        },
        {
            "name": "developerRewards",
            "type": "STRING"
        },
        {
            "name": "developerRewardsNum",
            "type": "FLOAT64"
        },
        {
            "name": "totalBalanceWithStake",
            "type": "STRING"
        },
        {
            "name": "totalBalanceWithStakeNum",
            "type": "FLOAT64"
        },
        {
            "name": "currentOwner",
            "type": "STRING"
        }
    ]

    actual_output_schema = map_elastic_search_schema_to_bigquery_schema(input_schema)

    assert actual_output_schema == expected_output_schema
