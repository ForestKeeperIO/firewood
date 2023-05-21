# Firewood: non-archival blockchain key-value store with hyper-fast recent state retrieval.

[![Latest version](https://img.shields.io/crates/v/firewood.svg)](https://crates.io/crates/firewood)
[![Ecosystem license](https://img.shields.io/badge/License-Ecosystem-blue.svg)](./LICENSE.md)

> :warning: firewood is alpha-level software and is not ready for production
> use. Do not use firewood to store production data. See the
> [license](./LICENSE.md) for more information regarding firewood usage.

Firewood is an embedded key-value store, optimized to store blockchain state.
It prioritizes access to latest state, by providing extremely fast reads, but
also provides a limited view into past state. It does not copy-on-write the
state trie to generate an ever growing forest of tries like other databases,
but instead keeps one latest version of the trie index on disk and apply
in-place updates to it. This ensures that the database size is small and stable
during the course of running firewood. Firewood was first conceived to provide
a very fast storage layer for the EVM but could be used on any blockchain that
requires authenticated state.

Firewood is a robust database implemented from the ground up to directly store
trie nodes and user data. Unlike most (if not all) of the solutions in the field,
it is not built on top of a generic KV store such as LevelDB/RocksDB. Like a
B+-tree based store, firewood directly uses the tree structure as the index on
disk. Thus, there is no additional “emulation” of the logical trie to flatten
out the data structure to feed into the underlying DB that is unaware of the
data being stored.

Firewood provides OS-level crash recovery via a write-ahead log (WAL). The WAL
guarantees atomicity and durability in the database, but also offers
“reversibility”: some portion of the old WAL can be optionally kept around to
allow a fast in-memory rollback to recover some past versions of the entire
store back in memory. While running the store, new changes will also contribute
to the configured window of changes (at batch granularity) to access any past
versions with no additional cost at all.

Firewood provides two isolated storage spaces which can be both or selectively
used the user. The account model portion of the storage offers something very similar
to StateDB in geth, which captures the address-“state key” style of two-level access for
an account’s (smart contract’s) state. Therefore, it takes minimal effort to
delegate all state storage from an EVM implementation to firewood. The other
portion of the storage supports generic trie storage for arbitrary keys and
values. When unused, there is no additional cost.

## License
firewood is licensed by the Ecosystem License. For more information, see the
[LICENSE file](./LICENSE.md).

## Vendored Crates
The following crates are vendored in this repository to allow for making
modifications without requiring upstream approval:
* [`growth-ring`](https://github.com/Determinant/growth-ring)
* [`libaio-futures`](https://github.com/Determinant/libaio-futures)
* [`shale`](https://github.com/Determinant/shale)

These crates will either be heavily modified or removed prior to the production
launch of firewood. If they are retained, all changes made will be shared
upstream.

## Termimology

* `Revision` - A point-in-time state/version of the trie. This represents the entire
  trie, including all `Key`/`Value`s at that point in time, and all `Node`s.
* `View` - This is a synonym for a `Revision`.
* `Node` - A node is a portion of a trie. A trie consists of nodes that are linked
  together. Nodes can point to other nodes and/or contain `Key`/`Value` pairs.
* `Hash` - In this context, this refers to the merkle hash for a specific node.
* `Root Hash` - The hash of the root node for a specific revision.
* `Key` - Represents an individual byte array used to index into a trie. A `Key`
  usually has a specific `Value`.
* `Value` - Represents a byte array for the value of a specific `Key`. Values can
  contain 0-N bytes. In particular, a zero-length `Value` is valid.
* `Key Proof` - A proof that a `Key` exists within a specific revision of a trie.
  This includes the hash for the node containing the `Key` as well as all parents.
* `Range Proof` - A proof that consists of two `Key Proof`s, one for the start of
  the range, and one for the end of the range, as well as a list of all `Key`/`Value`
  pairs in between the two. A `Range Proof` can be validated independently of an
  actual database by constructing a trie from the `Key`/`Value`s provided.
* `Change Proof` - A proof that consists of a set of all changes between two
  revisions.
* `Put` - An operation for a `Key`/`Value` pair. A put means "create if it doesn't
  exist, or update it if it does. A put operation is how you add a `Value` for a
  specific `Key`.
* `Delete` - A operation indicating that a `Key` that should be removed from the trie.
* `Batch Operation` - An operation of either `Put` or `Delete`.
* `Batch` - An ordered set of `Batch Operation`s.
* `Proposal` - A proposal consists of a base `Root Hash` and a `Batch`, but is not
  yet committed to the trie. In firewood's most recent API, a `Proposal` is required
  to `Commit`.
* `Commit` - The operation of applying one or more `Proposal`s to the most recent
  `Revision`.


## Roadmap
### Green Milestone
This milestone will focus on additional code cleanup, including supporting
concurrent access to a specific revision, as well as cleaning up the basic
reader and writer interfaces to have consistent read/write semantics.
- [ ] Concurrent readers of pinned revisions while allowing additional batches
to commit, to support parallel reads for the past consistent states. The revisions
are uniquely identified by root hashes.
- [ ] Pin a reader to a specific revision, so that future commits or other
operations do not see any changes.
- [ ] Be able to read-your-write in a batch that is not committed. Uncommitted
changes will not be shown to any other concurrent readers.

### Seasoned milestone
This milestone will add support for proposals, including proposed future
branches, with a cache to make committing these branches efficiently.
- [ ] Be able to propose a batch against the existing committed revision, or
propose a batch against any existing proposed revision.
- [ ] Be able to quickly commit a batch that has been proposed. Note that this
invalidates all other proposals that are not children of the committed proposed batch.

### Dried milestone
The focus of this milestone will be to support synchronization to other
instances to replicate the state. A synchronization library should also
be developed for this milestone.
- [ ] Support replicating the full state with corresponding range proofs that
verify the correctness of the data.
- [ ] Support replicating the delta state from the last sync point with
corresponding range proofs that verify the correctness of the data.
- [ ] Enforce limits on the size of the range proof as well as keys to make
  synchronization easier for clients.

## Build
Firewood currently is Linux-only, as it has a dependency on the asynchronous
I/O provided by the Linux kernel (see `libaio`). Unfortunately, Docker is not
able to successfully emulate the syscalls `libaio` relies on, so Linux or a
Linux VM must be used to run firewood. It is encouraged to enhance the project
with I/O supports for other OSes, such as OSX (where `kqueue` needs to be used
for async I/O) and Windows. Please contact us if you're interested in such contribution.

## Run
There are several examples, in the examples directory, that simulate real world
use-cases. Try running them via the command-line, via `cargo run --release
--example simple`.

To integrate firewood into a custom VM or other project, see the [firewood-connection](./firewood-connection/README.md) for a straightforward way to use firewood via custom message-passing.

## Release
See the [release documentation](./RELEASE.md) for detailed information on how to release firewood. 

## CLI
Firewood comes with a CLI tool called `fwdctl` that enables one to create and interact with a local instance of a firewood database. For more information, see the [fwdctl README](fwdctl/README.md).

## Test
```
cargo test --release
```
=======
<div align="center">
  <img src="resources/AvalancheLogoRed.png?raw=true">
</div>

---

Node implementation for the [Avalanche](https://avax.network) network -
a blockchains platform with high throughput, and blazing fast transactions.

## Installation

Avalanche is an incredibly lightweight protocol, so the minimum computer requirements are quite modest.
Note that as network usage increases, hardware requirements may change.

The minimum recommended hardware specification for nodes connected to Mainnet is:

- CPU: Equivalent of 8 AWS vCPU
- RAM: 16 GiB
- Storage: 1 TiB
- OS: Ubuntu 20.04/22.04 or macOS >= 12
- Network: Reliable IPv4 or IPv6 network connection, with an open public port.

If you plan to build AvalancheGo from source, you will also need the following software:

- [Go](https://golang.org/doc/install) version >= 1.19.6
- [gcc](https://gcc.gnu.org/)
- g++

### Building From Source

#### Clone The Repository

Clone the AvalancheGo repository:

```sh
git clone git@github.com:ava-labs/avalanchego.git
cd avalanchego
```

This will clone and checkout the `master` branch.

#### Building AvalancheGo

Build AvalancheGo by running the build script:

```sh
./scripts/build.sh
```

The `avalanchego` binary is now in the `build` directory. To run:

```sh
./build/avalanchego
```

### Binary Repository

Install AvalancheGo using an `apt` repository.

#### Adding the APT Repository

If you already have the APT repository added, you do not need to add it again.

To add the repository on Ubuntu, run:

```sh
sudo su -
wget -qO - https://downloads.avax.network/avalanchego.gpg.key | tee /etc/apt/trusted.gpg.d/avalanchego.asc
source /etc/os-release && echo "deb https://downloads.avax.network/apt $UBUNTU_CODENAME main" > /etc/apt/sources.list.d/avalanche.list
exit
```

#### Installing the Latest Version

After adding the APT repository, install `avalanchego` by running:

```sh
sudo apt update
sudo apt install avalanchego
```

### Binary Install

Download the [latest build](https://github.com/ava-labs/avalanchego/releases/latest) for your operating system and architecture.

The Avalanche binary to be executed is named `avalanchego`.

### Docker Install

Make sure Docker is installed on the machine - so commands like `docker run` etc. are available.

Building the Docker image of latest `avalanchego` branch can be done by running:

```sh
./scripts/build_image.sh
```

To check the built image, run:

```sh
docker image ls
```

The image should be tagged as `avaplatform/avalanchego:xxxxxxxx`, where `xxxxxxxx` is the shortened commit of the Avalanche source it was built from. To run the Avalanche node, run:

```sh
docker run -ti -p 9650:9650 -p 9651:9651 avaplatform/avalanchego:xxxxxxxx /avalanchego/build/avalanchego
```

## Running Avalanche

### Connecting to Mainnet

To connect to the Avalanche Mainnet, run:

```sh
./build/avalanchego
```

You should see some pretty ASCII art and log messages.

You can use `Ctrl+C` to kill the node.

### Connecting to Fuji

To connect to the Fuji Testnet, run:

```sh
./build/avalanchego --network-id=fuji
```

### Creating a Local Testnet

See [this tutorial.](https://docs.avax.network/build/tutorials/platform/create-a-local-test-network/)

## Bootstrapping

A node needs to catch up to the latest network state before it can participate in consensus and serve API calls. This process (called bootstrapping) currently takes several days for a new node connected to Mainnet.

A node will not [report healthy](https://docs.avax.network/build/avalanchego-apis/health) until it is done bootstrapping.

Improvements that reduce the amount of time it takes to bootstrap are under development.

The bottleneck during bootstrapping is typically database IO. Using a more powerful CPU or increasing the database IOPS on the computer running a node will decrease the amount of time bootstrapping takes.

## Generating Code

AvalancheGo uses multiple tools to generate efficient and boilerplate code.

### Running protobuf codegen

To regenerate the protobuf go code, run `scripts/protobuf_codegen.sh` from the root of the repo.

This should only be necessary when upgrading protobuf versions or modifying .proto definition files.

To use this script, you must have [buf](https://docs.buf.build/installation) (v1.11.0), protoc-gen-go (v1.28.0) and protoc-gen-go-grpc (v1.2.0) installed.

To install the buf dependencies:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
```

If you have not already, you may need to add `$GOPATH/bin` to your `$PATH`:

```sh
export PATH="$PATH:$(go env GOPATH)/bin"
```

If you extract buf to ~/software/buf/bin, the following should work:

```sh
export PATH=$PATH:~/software/buf/bin/:~/go/bin
go get google.golang.org/protobuf/cmd/protoc-gen-go
go get google.golang.org/protobuf/cmd/protoc-gen-go-grpc
scripts/protobuf_codegen.sh
```

For more information, refer to the [GRPC Golang Quick Start Guide](https://grpc.io/docs/languages/go/quickstart/).

### Running protobuf codegen from docker

```sh
docker build -t avalanche:protobuf_codegen -f api/Dockerfile.buf .
docker run -t -i -v $(pwd):/opt/avalanche -w/opt/avalanche avalanche:protobuf_codegen bash -c "scripts/protobuf_codegen.sh"
```

### Running mock codegen

To regenerate the [gomock](https://github.com/golang/mock) code, run `scripts/mock.gen.sh` from the root of the repo.

This should only be necessary when modifying exported interfaces or after modifying `scripts/mock.mockgen.txt`.

## Versioning

### Version Semantics

AvalancheGo is first and foremost a client for the Avalanche network. The versioning of AvalancheGo follows that of the Avalanche network.

- `v0.x.x` indicates a development network version.
- `v1.x.x` indicates a production network version.
- `vx.[Upgrade].x` indicates the number of network upgrades that have occurred.
- `vx.x.[Patch]` indicates the number of client upgrades that have occurred since the last network upgrade.

### Library Compatibility Guarantees

Because AvalancheGo's version denotes the network version, it is expected that interfaces exported by AvalancheGo's packages may change in `Patch` version updates.

### API Compatibility Guarantees

APIs exposed when running AvalancheGo will maintain backwards compatibility, unless the functionality is explicitly deprecated and announced when removed.

## Supported Platforms

AvalancheGo can run on different platforms, with different support tiers:

- **Tier 1**: Fully supported by the maintainers, guaranteed to pass all tests including e2e and stress tests.
- **Tier 2**: Passes all unit and integration tests but not necessarily e2e tests.
- **Tier 3**: Builds but lightly tested (or not), considered _experimental_.
- **Not supported**: May not build and not tested, considered _unsafe_. To be supported in the future.

The following table lists currently supported platforms and their corresponding
AvalancheGo support tiers:

| Architecture | Operating system | Support tier  |
| :----------: | :--------------: | :-----------: |
|    amd64     |      Linux       |       1       |
|    arm64     |      Linux       |       2       |
|    amd64     |      Darwin      |       2       |
|    amd64     |     Windows      |       3       |
|     arm      |      Linux       | Not supported |
|     i386     |      Linux       | Not supported |
|    arm64     |      Darwin      | Not supported |

To officially support a new platform, one must satisfy the following requirements:

| AvalancheGo continuous integration | Tier 1  | Tier 2  | Tier 3  |
| ---------------------------------- | :-----: | :-----: | :-----: |
| Build passes                       | &check; | &check; | &check; |
| Unit and integration tests pass    | &check; | &check; |         |
| End-to-end and stress tests pass   | &check; |         |         |

## Security Bugs

**We and our community welcome responsible disclosures.**

Please refer to our [Security Policy](SECURITY.md) and [Security Advisories](https://github.com/ava-labs/avalanchego/security/advisories).
