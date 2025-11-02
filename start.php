<?php
/**
 * Запуск всех компонентов Cluster
 * 
 * Использование:
 *   php start.php start    - запуск
 *   php start.php stop     - остановка
 *   php start.php restart  - перезапуск
 *   php start.php reload   - перезагрузка (hot reload)
 *   php start.php status   - статус
 */

define('GLOBAL_START', 1);

require_once __DIR__ . '/register.php';
require_once __DIR__ . '/gateway.php';
require_once __DIR__ . '/business.php';

