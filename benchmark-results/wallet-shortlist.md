# Active wallet research — October 9, 2026

Candidate for observation/paper testing: Numer0 (trench/arc)

Address: A3W8psibkTUvjxs4LRscbnjux6TFDXdvD4m4GsGpQ2KJ
Profile: https://kolscan.io/account/A3W8psibkTUvjxs4LRscbnjux6TFDXdvD4m4GsGpQ2KJ

The rendered Kolscan profile showed 57.7% win rate, 17-minute average duration,
$34,744.9 realized profits and $377.2 unrealized profits. These are the site's
profile statistics, not audited follower returns; the exact scope of the stats
was not independently established, so they are not labelled 7-day/30-day PnL.
The displayed closed-position counts were 101 wins / 74 losses.

Three recent source-signed transactions were independently read from mainnet RPC:
- 2026-10-09 19:09:07 UTC: CAT buy; wallet native SOL delta -1.01251884.
- 2026-10-09 19:10:51 UTC: WALTER buy; native SOL delta -1.01251884.
- 2026-10-09 19:11:19 UTC: CAT sell; native SOL delta +1.408307327.
All succeeded and invoked Pump.fun; none had a create-instruction log. CAT
buy-to-sell interval was 132 seconds. Net wallet SOL differences include fees,
rent and any other native transfers, so they are not treated as a complete
portfolio PnL model.

Relevant caveat for this trader: the profile also showed bursts of many 0.021 SOL
repeat buys in the same token within seconds. The average holding time hides
short trades as well. This is an imperfect fit for scarce TPS and a one-nonce
pool; per-token entry limits and delayed-entry paper results should be assessed
before live copying. No configuration was changed and no trade was submitted.

Other screened wallets were not selected:
- Cooker: displayed positive realized profit but very large unrealized losses,
  and many 1–8 second exits in the history.
- Iced: displayed negative realized PnL.
- narc: displayed unrealized losses larger than realized profits.
- LUKEY: attractive displayed statistics, but the inspected PDOOM entry included
  token initialization on Raydium LaunchLab, outside this bot's Pump.fun/PumpSwap
  support. It was not considered a suitable direct target from that evidence.

Raw public chain samples are in wallet-candidate-signatures.json,
wallet-candidate-transactions.json, lukey-onchain-check.json and
numero-onchain-check.json. Public-site statistics can change after this snapshot.
