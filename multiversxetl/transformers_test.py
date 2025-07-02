from multiversxetl.transformers import AccountsTransformer, EventsTransformer


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
