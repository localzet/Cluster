# Localzet Cluster

[Русская документация](README.ru.md)

Gateway/Business Worker clustering for Localzet Server: gateways own client connections, workers handle application events, and Register distributes gateway addresses.

## Compatibility

- PHP 8.2 or later; Unix multi-process operation needs `pcntl` and `posix`.
- The current dependency is `localzet/server:^4.2` (tested locally with 4.5.3).
- Server's 7.x development branch is a separate runtime line. Compatibility has not been established; do not substitute it without integration testing.

## Install

```sh
composer require localzet/cluster
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

The regression suite uses local socket pairs to check discovery broadcasts, class compatibility, authentication rejection and secret redaction. It does not establish distributed failover, load capacity or Server 7.x compatibility. CI resolves dependencies independently on PHP 8.2–8.5.

## Documentation and contributing

The [documentation repository](https://github.com/localzet/cluster-docs) contains additional material; verify examples against the current source and supported Server version.

See [.github/CONTRIBUTING.md](.github/CONTRIBUTING.md), [.github/CODE_OF_CONDUCT.md](.github/CODE_OF_CONDUCT.md) and [.github/SECURITY.md](.github/SECURITY.md).

## Author and license

Ivan Zorin (`localzet`) — <creator@localzet.com> · https://www.localzet.com.
Source: https://github.com/localzet/Cluster. GNU AGPL v3 or later; see [LICENSE](LICENSE). Original copyright notices and third-party attribution remain applicable; see [.github/AUTHORS.md](.github/AUTHORS.md).
