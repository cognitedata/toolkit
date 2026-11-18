from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    SandboxSubscriptionFilter,
    SignalSubscriptionResponse,
)


def test_signal_subscription_parses_sandbox_filter() -> None:
    subscription = SignalSubscriptionResponse.model_validate(
        {
            "externalId": "my_subscription",
            "sink": {"type": "current_user"},
            "filter": {"topic": "cognite_sandbox", "tag": "my_tag"},
            "createdTime": 0,
            "lastUpdatedTime": 0,
        }
    )

    assert subscription.filter == SandboxSubscriptionFilter(topic="cognite_sandbox", tag="my_tag")
