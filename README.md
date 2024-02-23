# UpdateSecretIssueTest


Rabbit Version 3.13.0
Erlang 26.2.2
We used IDS for OAuth, please use your token provider.

Issue Description-

Update secret call works in case of normal stream. Token is updated (visible in logs or RabbitMQ) and connection remains active.
However in case of Superstream (used with three partitions), update secret call reaches to RabbitMQ. But token is not updated. Seems Connection keeps on using old token as connection breaks exactly when intial toke gets expired.

