# Localzet Cluster

[Русская документация](README.ru.md)

Gateway/Business Worker clustering for Localzet Server: gateways own client connections, workers handle application events, and Register distributes gateway addresses.

## Compatibility

- PHP 8.2 or later; Unix multi-process operation needs `pcntl` and `posix`.
- Current source and the upcoming release require `localzet/server:^7.0`.
- PHP 8.2 or later; Unix multi-process operation needs `pcntl` and `posix`.

## Install

The package is not currently registered on Packagist. Add its Git repository and install the stable release:

```sh
composer config repositories.localzet-cluster vcs https://github.com/localzet/Cluster
composer require localzet/cluster:^1.0
```

For this repository's examples and regression tests:

```sh
composer install
export LOCALZET_CLUSTER_SECRET="$(php -r 'echo bin2hex(random_bytes(32));')"
php start.php start
```

Use the same secret on every host and keep it outside source control. The examples refuse missing secrets and secrets shorter than 32 bytes. By default, discovery and WebSocket listeners bind to loopback; the example WebSocket endpoint is `ws://127.0.0.1:7273`.

Commands: `php start.php stop`, `restart`, `reload`, and `status`. The example `Events.php` handles application callbacks. Adapt it before deploying your application.

## Components

- `Register`: authenticates gateways/workers and publishes gateway addresses.
- `Gateway`: accepts client connections and routes application messages.
- `Business`: maintains gateway connections and invokes your event handler.
- `Client`: sends application messages to clients, users and groups.

For a distributed deployment, configure reachable `registerAddress`, `lanIp` and unique gateway port ranges. Restrict internal discovery and gateway ports to trusted hosts. Shared-secret authentication does not encrypt traffic: use a trusted private network or an encrypted transport. The shipped examples are a local demonstration, not a complete production deployment.

## Verification

```sh
composer validate --strict
composer lint
composer test
```

The regression suite uses local socket pairs to check discovery broadcasts, class compatibility, authentication rejection and secret redaction against Server 7. CI resolves dependencies independently on PHP 8.2–8.5. It does not establish multi-host failover or production capacity.

### User object deserialization

Cluster encodes message bodies and sessions with PHP `serialize()`. Incoming PHP classes are not instantiated by `unserialize()` by default; arrays and scalar values remain supported, while serialized application objects decode as `__PHP_Incomplete_Class`. If the cluster is isolated, all nodes are trusted, and application sessions require objects, set `localzet\Cluster\Protocols\Cluster::$allowObjectSerialization` to `true` on every node. Do not enable this for untrusted peers.

## Documentation and contributing

The [documentation repository](https://github.com/localzet/cluster-docs) contains additional material; verify examples against the current source and supported Server version.

See [.github/CONTRIBUTING.md](.github/CONTRIBUTING.md), [.github/CODE_OF_CONDUCT.md](.github/CODE_OF_CONDUCT.md) and [.github/SECURITY.md](.github/SECURITY.md).

## Author and license

Ivan Zorin (`localzet`) — <creator@localzet.com> · https://www.localzet.com.
Source: https://github.com/localzet/Cluster. GNU AGPL v3 or later; see [LICENSE](LICENSE). Original copyright notices and third-party attribution remain applicable; see [.github/AUTHORS.md](.github/AUTHORS.md).
