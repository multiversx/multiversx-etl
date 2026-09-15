from multiversxetl.transformers import (AccountsTransformer, EventsTransformer,
                                        ExecutionResultsTransformer,
                                        TransformersRegistry)


def test_accounts_transformer():
    transformer = AccountsTransformer()

    transformed = transformer.transform({
        "_id": "abba",
        "address": "erd1qyu5wthldzr8wx5c9ucg8kjagg0jfs53s8nr3zpz3hypefsdd8ssycr6th",
        "api_test": "foobar"
    })

    assert transformed == {
        "_id": "abba",
        "address": "erd1qyu5wthldzr8wx5c9ucg8kjagg0jfs53s8nr3zpz3hypefsdd8ssycr6th",
    }


def test_events_transformer():
    transformer = EventsTransformer()

    transformed = transformer.transform({
        "_id": "abba",
        "identifier": "foobar",
        "topics": ["foo", None, "bar"],
        "additionalData": ["bar", None, "foo"]
    })

    assert transformed == {
        "_id": "abba",
        "identifier": "foobar",
        "topics": ["foo", "", "bar"],
        "additionalData": ["bar", "", "foo"]
    }


def test_execution_results_transformer_derives_timestamp_from_timestamp_ms():
    transformer = ExecutionResultsTransformer()

    transformed = transformer.transform({
        "_id": "abba",
        "timestampMs": 1789158274200,
    })

    assert transformed == {
        "_id": "abba",
        "timestampMs": 1789158274200,
        # Truncated to seconds (not rounded).
        "timestamp": 1789158274,
    }


def test_execution_results_transformer_without_timestamp_ms():
    transformer = ExecutionResultsTransformer()

    transformed = transformer.transform({
        "_id": "abba",
    })

    assert transformed == {
        "_id": "abba",
    }


def test_execution_results_transformer_is_registered():
    registry = TransformersRegistry()

    assert isinstance(registry.get_transformer("executionresults"), ExecutionResultsTransformer)
