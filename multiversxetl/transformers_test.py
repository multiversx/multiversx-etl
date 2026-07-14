import json

from multiversxetl.transformers import AccountsTransformer, EventsTransformer, Transformer


def test_transformer_ignores_explicitly_ignored_fields():
    transformer = Transformer()

    transformed = json.loads(transformer.transform_json(
        json.dumps({
            "_id": "abba",
            "fang_write_test": "ignored",
            "test_field": "ignored",
        }),
        ignored_fields=["fang_write_test", "test_field"]
    ))

    assert transformed == {
        "_id": "abba",
    }


def test_transformer_preserves_other_unknown_fields():
    transformer = Transformer()

    transformed = json.loads(transformer.transform_json(
        json.dumps({
            "_id": "abba",
            "some_other_field": "preserved",
        }),
        ignored_fields=["fang_write_test", "test_field"]
    ))

    assert transformed == {
        "_id": "abba",
        "some_other_field": "preserved",
    }


def test_ignored_fields_are_removed_after_index_specific_transformation():
    transformer = AccountsTransformer()

    transformed = json.loads(transformer.transform_json(
        json.dumps({
            "_id": "abba",
            "api_test": "ignored by accounts transformer",
            "fang_write_test": "ignored globally",
        }),
        ignored_fields=["fang_write_test"]
    ))

    assert transformed == {
        "_id": "abba",
    }


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
