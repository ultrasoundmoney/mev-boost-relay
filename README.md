# ultra sound money: optimistic relay

The default branch of this repo, called `prod-optimistic-relaying` is the modification of https://github.com/flashbots/mev-boost-relay to enable optimistic relaying 
for the ultra sound [relay](relay.ultrasound.money). 

We have also opened a PR with the same set of changes on the main repo https://github.com/flashbots/mev-boost-relay/pull/285 to (a) help see the diff from the base relay implementation, (b) maintain the history of the change discussed in that PR, and (c) continue the discussion of upstreaming this change. 

## Operational documentation
see [docs/optmistic](docs/optimistic) for the following:

<<<<<<< HEAD
- [Builder onboarding](docs/optimistic/builder-onboarding.md)
- [Event log](docs/optimistic/event-log.md)
- [An optimistic weekend](docs/optimistic/an-optimistic-weekend.md)

## Extended discussion
- [Optimistic relay roadmap](https://github.com/michaelneuder/optimistic-relay-documentation/blob/main/towards-epbs.md)
- [mev-boost community call #0](https://collective.flashbots.net/t/mev-boost-community-call-0-23-feb-2023/1348)
- [mev-boost community call #1](https://collective.flashbots.net/t/mev-boost-community-call-1-9-mar-2023/1367)
=======
* [boost-relay.flashbots.net](https://boost-relay.flashbots.net) (also on [Goerli](https://boost-relay-sepolia.flashbots.net) and [Sepolia](https://boost-relay-goerli.flashbots.net))
* [relay.ultrasound.money](https://relay.ultrasound.money), [agnostic-relay.net](https://agnostic-relay.net), bloXroute relays ([light fork](https://github.com/bloXroute-Labs/mev-relay))
* [mainnet.aestus.live](https://mainnet.aestus.live), [relay.edennetwork.io/info](https://relay.edennetwork.io/info), [mainnet-relay.securerpc.com](https://mainnet-relay.securerpc.com)

Alternatives (not audited or endorsed): [blocknative/dreamboat](https://github.com/blocknative/dreamboat), [manifold/mev-freelay](https://github.com/manifoldfinance/mev-freelay)
