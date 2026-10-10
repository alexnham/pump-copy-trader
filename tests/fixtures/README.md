# Transaction fixtures

Place captured, redacted LaserStream `SubscribeUpdateTransaction` protobuf
fixtures in the protocol subdirectories. Fixtures must contain metadata exactly
as received from Helius; never include API keys.

`pumpswap-buy.json` and `pumpswap-sell.json` contain public swap instructions
from journal copy attempts 10 and 11 (October 6, 2026). They contain no private
keys or credentials and exercise transaction size with the observed account layouts.

`pump_fun_legacy_buy_volume.json` captures the public source buy instruction for
`3cQ8BbQy9GF4ztkoh6J7gicq7CKoiWUN6hSxpbjie4hGRvqKuXbDZXH2JzhArggfLfCNAxQFFMvJUBFZLxhGxmcb`.
Its copied transaction `2mUT6rLqivnqaniNZ3WhwyKPf1PvjcaPZsANVTs1ZHQFqyYkA2t3ow1xJXUr8mEt6JgxjRtzZZNmzD8Q2AgMPxVg` failed at slot 454960831 because legacy buy
account 13 retained the source wallet's `user_volume_accumulator`. The fixture
checks remapping to the copier PDA reported by the program's ConstraintSeeds
error while retaining every unrelated account and its flags.
