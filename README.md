# Localzet Cluster

**Localzet Cluster** — распределенная система на базе Localzet Server для создания масштабируемых кластерных приложений с Gateway-Worker архитектурой.

## Возможности

- **Gateway-Worker архитектура**: Разделение клиентских соединений и бизнес-логики
- **Горизонтальное масштабирование**: Распределение нагрузки между несколькими серверами
- **Service Discovery**: Автоматическое обнаружение и регистрация серверов через Register
- **Группы и сессии**: Управление группами пользователей и их сессиями на уровне кластера
- **UID привязка**: Привязка client_id к идентификаторам пользователей для отправки сообщений
- **Высокая производительность**: Оптимизированный бинарный протокол для внутренней коммуникации

## Установка

```bash
composer require localzet/cluster
```

## Требования

- PHP >= 8.2
- Localzet Server >= 4.2
- Расширения: sockets, pcntl (Linux/Unix), posix (Linux/Unix), json

## Быстрый старт

Создайте файлы для запуска кластера:

**register.php:**
```php
<?php
use localzet\Cluster\Register;

$register = new Register('text://0.0.0.0:1236');
$register->name = 'Register';
$register->secretKey = 'your-secret-key';
```

**gateway.php:**
```php
<?php
use localzet\Cluster\Gateway;

$gateway = new Gateway("Websocket://0.0.0.0:7273");
$gateway->name = 'ChatGateway';
$gateway->count = 4;
$gateway->registerAddress = '127.0.0.1:1236';
$gateway->secretKey = 'your-secret-key';
```

**business.php:**
```php
<?php
use localzet\Cluster\Business;

$worker = new Business();
$worker->name = 'ChatBusinessWorker';
$worker->count = 4;
$worker->registerAddress = '127.0.0.1:1236';
$worker->secretKey = 'your-secret-key';
```

Запустите:
```bash
php start.php start
```

## Документация

Полная документация доступна на [cluster.localzet.com](https://cluster.localzet.com)

## Компоненты

- **Gateway**: Обрабатывает клиентские соединения и маршрутизирует сообщения
- **Business Worker**: Обрабатывает бизнес-логику приложения
- **Register**: Обеспечивает service discovery и координацию компонентов
- **Client API**: Программный интерфейс для внешних приложений

## Применение

- Масштабируемые чаты и мессенджеры
- Real-time игры и приложения
- IoT платформы
- Системы мониторинга
- Микросервисы и распределенные системы

## Лицензия

AGPL-3.0-or-later

## Автор

Ivan Zorin <creator@localzet.com>

## Ссылки

- [Документация Cluster](https://cluster.localzet.com)
- [Документация Server](https://server.localzet.com)
- [GitHub](https://github.com/localzet/Cluster)
