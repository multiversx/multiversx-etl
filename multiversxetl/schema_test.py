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
            "type": "STRING",
            "description": ""
        },
        {
            "name": "foo",
            "type": "STRING",
            "description": ""
        },
        {
            "name": "bar",
            "type": "FLOAT",
            "description": ""
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
            "type": "STRING",
            "description": ""
        },
        {
            "name": "foo",
            "mode": "REPEATED",
            "type": "STRING",
            "description": ""
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
                    "type": "STRING",
                    "description": ""
                },
                {
                    "name": "bar",
                    "type": "FLOAT",
                    "description": ""
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
            "type": "STRING",
            "description": ""
        },
        {
            "name": "address",
            "type": "STRING",
            "description": ""
        },
        {
            "name": "userName",
            "type": "STRING",
            "description": ""
        },
        {
            "name": "balance",
            "type": "STRING",
            "description": ""
        },
        {
            "name": "balanceNum",
            "type": "FLOAT",
            "description": ""
        },
        {
            "name": "nonce",
            "type": "FLOAT",
            "description": ""
        },
        {
            "name": "shardID",
            "type": "NUMERIC",
            "description": ""
        },
        {
            "name": "timestamp",
            "type": "TIMESTAMP",
            "description": ""
        },
        {
            "name": "developerRewards",
            "type": "STRING",
            "description": ""
        },
        {
            "name": "developerRewardsNum",
            "type": "FLOAT",
            "description": ""
        },
        {
            "name": "totalBalanceWithStake",
            "type": "STRING",
            "description": ""
        },
        {
            "name": "totalBalanceWithStakeNum",
            "type": "FLOAT",
            "description": ""
        },
        {
            "name": "currentOwner",
            "type": "STRING",
            "description": ""
        }
    ]

    actual_output_schema = map_elastic_search_schema_to_bigquery_schema(input_schema)

    assert actual_output_schema == expected_output_schema
