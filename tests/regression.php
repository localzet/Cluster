<?php

declare(strict_types=1);

/**
 * @package     Localzet Cluster
 * @link        https://github.com/localzet/Cluster
 *
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright (c) 2026 Localzet Group
 * @license     https://www.gnu.org/licenses/agpl AGPL-3.0 license
 *
 *              This program is free software: you can redistribute it and/or modify
 *              it under the terms of the GNU Affero General Public License as
 *              published by the Free Software Foundation, either version 3 of the
 *              License, or (at your option) any later version.
 *
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *              GNU Affero General Public License for more details.
 *
 *              You should have received a copy of the GNU Affero General Public License
 *              along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

require dirname(__DIR__) . '/vendor/autoload.php';

use localzet\Cluster\Business;
use localzet\Cluster\Client;
use localzet\Cluster\Gateway;
use localzet\Cluster\Protocols\Cluster;
use localzet\Cluster\Register;
use localzet\Server;
use localzet\Server\Connection\TcpConnection;
use localzet\Server\Events\EventInterface;
use localzet\Timer;

final class TestGateway extends Gateway
{
    public static array $messages = [];

    public static function log(mixed $msg, bool $decorated = true): void
    {
        self::$messages[] = (string) $msg;
    }
}

function check(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
    echo "OK: $message\n";
}

function connection(EventInterface $loop): array
{
    $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    if ($pair === false) {
        throw new RuntimeException('Cannot create test socket pair.');
    }
    stream_set_blocking($pair[1], false);
    return [new TcpConnection($loop, $pair[0], '127.0.0.1:9000'), $pair[1]];
}

$loopClass = class_exists('localzet\\Server\\Events\\Select')
    ? 'localzet\\Server\\Events\\Select'
    : 'localzet\\Server\\Events\\Windows';
$loop = new $loopClass();
Timer::init($loop);
$log = tempnam(sys_get_temp_dir(), 'cluster-test-');
Server::$logFile = $log;
Server::$daemonize = true;
$secret = 'test-secret-do-not-log';

try {
    foreach ([Register::class, Gateway::class, Business::class, Client::class] as $class) {
        check(class_exists($class), "$class loads with the supported Server dependency");
    }
    $register = new Register('text://127.0.0.1:0', [], $secret);
    [$worker, $workerPeer] = connection($loop);
    $register->onConnect($worker);
    $register->onMessage($worker, json_encode(['event' => 'server_connect', 'secret_key' => $secret]));
    $reply = json_decode((string) fread($workerPeer, 65536), true);
    check($reply === ['event' => 'broadcast_addresses', 'addresses' => []], 'New worker receives exactly one address broadcast');

    [$gateway, $gatewayPeer] = connection($loop);
    $register->onConnect($gateway);
    $register->onMessage($gateway, json_encode(['event' => 'gateway_connect', 'secret_key' => $secret, 'address' => '127.0.0.1:2800']));
    $reply = json_decode((string) fread($workerPeer, 65536), true);
    check($reply['addresses'] === ['127.0.0.1:2800'], 'Registered worker receives gateway updates');
    $register->onMessage($worker, '{"event":"ping"}');
    check(is_resource($worker->getSocket()), 'Authenticated worker can ping without resending the secret');

    foreach (['null', '[]', '{"event":[]}', '{"event":"server_connect","secret_key":[]}', '{"event":"ping"}', '{"event":"unknown","secret_key":"' . $secret . '"}'] as $request) {
        [$bad, $peer] = connection($loop);
        $register->onConnect($bad);
        $register->onMessage($bad, $request);
        check(feof($peer) || fread($peer, 1) === '', 'Invalid registration cannot receive discovery data');
        check(!is_resource($bad->getSocket()), 'Invalid registration is closed');
        $register->onClose($bad);
        fclose($peer);
    }
    check(!str_contains(file_get_contents($log), $secret), 'Register never logs request secrets');

    $server = new TestGateway('text://127.0.0.1:0', [], $secret);
    foreach ([Cluster::CMD_SERVER_CONNECT, Cluster::CMD_GATEWAY_CLIENT_CONNECT] as $cmd) {
        foreach ([[], ['secret_key' => []], ['secret_key' => 'wrong']] as $info) {
            [$bad, $peer] = connection($loop);
            $server->onServerMessage($bad, ['cmd' => $cmd, 'body' => json_encode($info)]);
            check(!is_resource($bad->getSocket()), 'Invalid gateway authentication is closed');
            fclose($peer);
        }
    }
    check(!str_contains(implode('\n', TestGateway::$messages), $secret), 'Gateway authentication logs never disclose the expected secret');
    [$client, $clientPeer] = connection($loop);
    $server->onServerMessage($client, ['cmd' => Cluster::CMD_GATEWAY_CLIENT_CONNECT, 'body' => json_encode(['secret_key' => $secret])]);
    check($client->authorized === true, 'Valid gateway client authentication succeeds');
    $client->close();
    fclose($clientPeer);
    $worker->close();
    $gateway->close();
    fclose($workerPeer);
    fclose($gatewayPeer);
    Timer::delAll();
} finally {
    @unlink($log);
}
echo "All Cluster regression tests passed.\n";
