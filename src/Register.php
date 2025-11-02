<?php

declare(strict_types=1);

/**
 * @package     Localzet Cluster
 * @link        https://github.com/localzet/Cluster
 *
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright (c) 2018-2024 Localzet Group
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

namespace localzet\Cluster;

use localzet\Server;
use localzet\Server\Connection\TcpConnection;
use localzet\Server\Protocols\Text;
use localzet\Timer;
use Throwable;

/**
 * Регистратор
 * Отвечает за регистрацию и аутентификацию соединений в Localzet Cluster.
 */
class Register extends Server
{
    /**
     * @var array Массив соединений со шлюзами.
     */
    protected array $_gatewayConnections = [];

    /**
     * @var array Массив соединений с серверами.
     */
    protected array $_serverConnections = [];

    /**
     * @var int Время запуска процесса.
     */
    protected int $_startTime = 0;

    public string $name = 'Регистратор';

    public bool $reloadable = false;

    public ?string $protocol = Text::class;

    public bool $reusePort = false;

    /**
     * @param string|null $socketName
     * @param array $socketContext
     * @param string|null $secretKey Секретный ключ для аутентификации соединений
     */
    public function __construct(?string $socketName = null, array $socketContext = [], public ?string $secretKey = null)
    {
        parent::__construct($socketName, $socketContext);

        $this->secretKey ??= base64_encode(sha1(sprintf('%04x%04x%04x%04x%04x%04x%04x%04x',
            mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0x0fff) | 0x4000,
            mt_rand(0, 0x3fff) | 0x8000, mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff)), true));
    }

    /**
     * Запускает регистратор.
     */
    public function run(): void
    {
        $this->onConnect = [$this, 'onConnect'];
        $this->onMessage = [$this, 'onMessage'];
        $this->onClose = [$this, 'onClose'];

        $this->_startTime = time();
        parent::run();
    }

    /**
     * Устанавливает таймер для закрытия соединения, если не была получена аутентификация.
     *
     * @param TcpConnection $connection Соединение.
     */
    public function onConnect(TcpConnection $connection): void
    {
        $connection->timeout_timerid = Timer::add(10, function () use ($connection) {
            Server::log("Регистратор: Время аутентификации истекло (" . $connection->getRemoteIp() . ")");
            $connection->close();
        }, null, false);
    }

    /**
     * Обрабатывает полученное сообщение.
     *
     * @param TcpConnection $connection Соединение.
     * @param string $buffer Буфер с сообщением.
     * @throws Throwable
     */
    public function onMessage(TcpConnection $connection, string $buffer): void
    {
        // Удаляем таймер для закрытия соединения.
        Timer::del($connection->timeout_timerid);

        // Распаковываем данные из JSON.
        $data = @json_decode($buffer, true);

        if (empty($data['event'])) {
            $error = "Невернный запрос для Регистратора. Информация о запросе (IP: " . $connection->getRemoteIp() . ", Буфер запроса: $buffer)";
            Server::log($error);
            $connection->close($error);
            return;
        }

        $event = $data['event'];
        $secret_key = $data['secret_key'] ?? '';

        switch ($event) {
            case 'gateway_connect':
                if (empty($data['address'])) {
                    Server::log("Регистратор: Адрес не найден от " . $connection->getRemoteIp());
                    $connection->close();
                    return;
                }

                if ($this->secretKey && $secret_key !== $this->secretKey) {
                    Server::log("Регистратор: Ключ не соответствует от " . $connection->getRemoteIp());
                    $connection->close();
                    return;
                }

                $this->_gatewayConnections[$connection->id] = $data['address'];
                $this->broadcastAddresses();
                break;
            case 'server_connect':
                if ($this->secretKey && $secret_key !== $this->secretKey) {
                    Server::log("Регистратор: Ключ не соответствует от " . $connection->getRemoteIp());
                    $connection->close();
                    return;
                }

                $this->_serverConnections[$connection->id] = $connection;
                $this->broadcastAddresses($connection);
                break;
            case 'ping':
                break;
            default:
                Server::log("Регистратор: неизвестное событие: $event IP: " . $connection->getRemoteIp() . " Буфер: $buffer");
                $connection->close();
        }
    }

    /**
     * Обработчик закрытия соединения.
     *
     * @param TcpConnection $connection Соединение.
     */
    public function onClose(TcpConnection $connection): void
    {
        // Удаляем соединение со списков серверов.
        if (isset($this->_serverConnections[$connection->id])) {
            unset($this->_serverConnections[$connection->id]);
        }

        // Удаляем соединение со списков шлюзов и обновляем список адресов.
        if (isset($this->_gatewayConnections[$connection->id])) {
            unset($this->_gatewayConnections[$connection->id]);
            $this->broadcastAddresses();
        }
    }

    /**
     * Отправляет сообщение с адресами шлюзов всем серверам.
     *
     * @param TcpConnection|null $connection Соединение, для которого отправляется сообщение.
     */
    public function broadcastAddresses(TcpConnection $connection = null): void
    {
        $data = [
            'event' => 'broadcast_addresses',
            'addresses' => array_unique(array_values($this->_gatewayConnections)),
        ];

        foreach ($connection ? (array)$connection : $this->_serverConnections as $connection) {
            $connection->send(json_encode($data));
        }
    }
}
