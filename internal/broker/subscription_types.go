package broker

import (
	"fmt"

	"minipulsar/internal/storage"
	pulsar "minipulsar/pb"
)

// subscriptionTypeForStorage maps wire-level subscription types to storage enums.
func subscriptionTypeForStorage(subType pulsar.CommandSubscribe_SubType) (storage.SubscriptionType, error) {
	switch subType {
	case pulsar.CommandSubscribe_Exclusive:
		return storage.SubscriptionTypeExclusive, nil
	case pulsar.CommandSubscribe_Shared:
		return storage.SubscriptionTypeShared, nil
	case pulsar.CommandSubscribe_Failover:
		return storage.SubscriptionTypeFailover, nil
	case pulsar.CommandSubscribe_Key_Shared:
		return "", fmt.Errorf("key_shared subscription type not supported")
	default:
		return "", fmt.Errorf("unsupported subscription type %s", subType)
	}
}
