<?php
namespace common\components;

use yii\redis\Cache;
use yii\redis\Connection;
use yii\di\Instance;
use Yii;

/**
 * Обёртка над дефолтным \yii\redis\Cache, предназначенная для балансировки кэша на нескольких серверах.
 *
 * Вызова следующего метода в рамках скриптов админки достаточно, чтобы очистить выбранную
 * базу данных Redis на перечисленных в Yii::$app->params['cacheServers'] хостах:
 * 
 * Для использования в конфиге:
 * 
 * ~~~
 * [
 *     'components' => [
 *         'cache' => [
 *             'class' => 'common\components\RedisBalancerV2',
 *             'redis' => [ 'hostname' => '127.0.0.1', 'port' => 6379, 'database' => 0, 'unixSocket' => '/var/run/redis/redis1.sock' ],
 *             'servers' => [
 *                 'master' => [ 'hostname' => '127.0.0.1', 'port' => 6379, 'database' => 0, 'unixSocket' => '/var/run/redis/redis1.sock' ],
 *                 [ 'hostname' => '127.0.0.2', 'port' => 6379, 'database' => 0, 'unixSocket' => '/var/run/redis/redis2.sock' ],
 *             ],
 *         ],
 *     ],
 * ]
 * ~~~
 *
 * @author Dmitrii Krotkov <krotkovd@gmail.com>
 * @since 2.0
 */

class RedisBalancerV2 extends Cache
{
    // KEY LOCK STATES
    const NOT_LOCKED = 0;
    const NOT_EXISTS_ON_MASTER = 5;
    const LOCKED_ON_MASTER = 80;
    const LOCKED_BY_THIS_CLIENT = 90;

    /**
     * @var array Пул Redis-хостов, первый из которых станет master-сервером, если тот не указан явно
     * либо в redis, либо с помощью ключа 'master'
     */
    public $servers = [];
    /**
     * @var array Пул Redis-slave-хостов
     */
    public $slaves = [];
    /**
     * @var int Время блокировки ключа при его отсутствии (ключ блокируется исключительно на master-сервере),
     * по сути это максимальное время ожидания значение из кэша на уровне редиса, по истечению которого
     * следующий в очереди ожидающий клиент начинает сам собирать данные
     */
    public $lockTime = 10;
    /**
     * @var float Однако очередь может копиться довольно долго, поэтому процент пропуска для самостоятельного
     * сбора данных (возможно не самая лучшая затея, как показал себя RedisLoadDivider)
     */
    public $skipQueuePercentage = 3;
    /**
     * @var int Интервал перепроверок блокированного ключа последующих запросов
     */
    public $interval = 150;
    /**
     * @var bool Проверять доступность слэйвов, убирать из списка недоступные для предотвращения
     * Database Exception 
     */
    public $dropDeadSlaves = false;
    /**
     * @var bool Включение/отключение сжатия
     */
    public $compressEnabled = false;
    /**
     * @var int Минимальная длина данных, подлежащих сжатию [в байтах]
     */
    public $compressMinLength = 2048;

    /**
     * @var int Кол-во серверов
     */
    private $serversCount = 0;
    /**
     * @var int Кол-во слейвов
     */
    private $slavesCount = 0;
    /**
     * @var array перемешанный массив ключей массива серверов
     */
    private $shuffledServersIndexes = [];
    /**
     * @var array перемешанный массив ключей массива слейвов
     */
    private $shuffledSlavesIndexes = [];
    /**
     * @var array массив залоченных текущим клиентом ключей
     */
    private $lockedValues = [];
    
    /**
     * @inheritdoc
     */
    public function init()
    {
        if (!is_array($this->redis)) {
            if (isset($this->servers["master"])) {
                $this->redis = $this->servers["master"];
                unset($this->servers["master"]);
            } else {
                $this->redis = array_shift($this->servers);
            }
        }

        $this->slaves = $this->servers;
        $this->servers = [];

        if (isset($this->redis["unixSocket"]) && !file_exists($this->redis["unixSocket"])) {
            $this->redis["unixSocket"] = null;
        }

        parent::init();
        $this->servers[] = &$this->redis;

        foreach ($this->slaves as $i => $server) {
            if (isset($server["unixSocket"]) && !file_exists($server["unixSocket"])) {
                $server["unixSocket"] = null;
            }
            $this->slaves[$i] = Instance::ensure($server, Connection::className());

            if ($this->dropDeadSlaves) {
                try {
                    $this->slaves[$i]->open();
                } catch (\Exception $e) {
                    unset($this->slaves[$i]);
                    continue;
                }
            }

            $this->servers[] = &$this->slaves[$i];
        }

        $this->shuffledServersIndexes = range(0, count($this->servers) - 1, 1);
        shuffle($this->shuffledServersIndexes);
        $this->shuffledSlavesIndexes = range(0, count($this->slaves) - 1, 1);
        shuffle($this->shuffledSlavesIndexes);

        $this->serversCount = count($this->servers);
        $this->slavesCount = count($this->slaves);
    }

    public function __destruct()
    {
        foreach ($this->lockedValues as $key => $hash) {
            if ($this->keyLockState($key, $hash) === self::LOCKED_BY_THIS_CLIENT) {
                $this->deleteValue($key);
            }
        }
    }

    public function compressData($data)
    {
        if ($this->compressEnabled) {
            $data = serialize($data);
            if (strlen($data) >= $this->compressMinLength)  {
                $data = gzdeflate($data, 9);
            }
        }
        return $data;
    }

    public function decompressData($data)
    {
        if ($this->compressEnabled) {
            $data = @gzinflate($data);
            $data = unserialize($data);
        }
        return $data;
    }

    public function exists($key)
    {
        if ($this->serversCount == 0) {
            return false;
        } else {
            return ($this->getRandomServer()->executeCommand('EXISTS', [ $key ]) && $this->keyLockState($key) !== self::LOCKED_ON_MASTER);
        }
    }

    protected function getValue($key)
    {
        if ($this->serversCount == 0) return false;

        $lockState = $this->keyLockState($key);

        if ($lockState === self::NOT_LOCKED) {

            return $this->decompressData($this->getRandomServer()->executeCommand('GET', [$key]));

        } elseif ($lockState === self::NOT_EXISTS_ON_MASTER) {

            $this->lock($key);
            return false;

        } elseif ($lockState === self::LOCKED_ON_MASTER) {

            if ($this->slavesCount == 0 || !$this->getRandomSlave()->executeCommand('EXISTS', [ $key ])) {
                if (rand(0, 100) <= $this->skipQueuePercentage) {
                    return false;
                }
                $this->runSleeping($key);
                return $this->decompressData($this->getRandomServer()->executeCommand('GET', [$key]));
            } else {
                return $this->decompressData($this->getRandomSlave()->executeCommand('GET', [$key]));
            }

        }

        return false;
    }

    protected function setValue($key, $value, $expire = 0, $mode = "SET")
    {
        if ($this->serversCount == 0) return false;
        
        if ($expire == 0) {
            $setArr = [ $key, $this->compressData($value) ];
        } else {
            $setArr = [ $key, $this->compressData($value), 'EX', (int)$expire ];
        }

        if ($mode == "ADD") {
            $setArr[] = 'NX';
            $this->unlockValue($key);
        } else {
            if (!$this->isAllowedToSet($key)) {
                return false;
            } else {
                if (isset($this->lockedValues[$key])) unset($this->lockedValues[$key]);
            }
        }

        $result = false;

        foreach ($this->servers as $server) {
            $result = $server->executeCommand('SET', $setArr);
        }

        return (bool) $result;
    }

    protected function addValue($key, $value, $expire = 0)
    {
        return $this->setValue($key, $value, $expire, "ADD");
    }

    protected function deleteValue($key)
    {
        $result = false;
        foreach ($this->servers as $server) {
            $result = $server->executeCommand('DEL', [$key]);
        }
        return (bool) $result;
    }

    protected function lock(string $key, int $lockTime = 0)
    {
        $lockTime = $lockTime === 0 ? $this->lockTime : $lockTime;

        if ($this->serversCount == 0 || $lockTime == 0) return false;
        
        $hash = sha1($key . microtime());

        if ($this->redis->executeCommand('SET', [$key, "LOCK|||$hash", 'NX', 'EX', $lockTime])) {
            $this->lockedValues[$key] = $hash;
            return true;
        }

        return false;
    }

    protected function unlockValue(string $key)
    {        
        if (!$this->lockTime) return true;
        if ($this->isAllowedToSet($key)) {
            $result = $this->deleteValue($key);
            if ($result && isset($this->lockedValues[$key])) {
                unset($this->lockedValues[$key]);
            }
            return $result;
        }
        return false;
    }


    protected function isAllowedToSet(string $key)
    {
        if (isset($this->lockedValues[$key])) {
            return true;
        }

        return false;
    }

    protected function keyLockState(string $key, string $hash = "")
    {
        $val = $this->redis->executeCommand('GETRANGE', [$key, 0, 46]);

        if ($val === "") {
            return self::NOT_EXISTS_ON_MASTER;
        } elseif ($val === "LOCK|||$hash") {
            return self::LOCKED_BY_THIS_CLIENT;
        } elseif (substr($val, 0, 7) == "LOCK|||") {
            return self::LOCKED_ON_MASTER;
        } else {
            return self::NOT_LOCKED;
        }
    }

    protected function runSleeping($key)
    {
        if ($this->lockTime > 0) {
            usleep($this->interval * 1000);
            while ($this->keyLockState($key) === self::LOCKED_ON_MASTER) usleep($this->interval * 1000);
        }
    }

    protected function getRandomServer()
    {
        return $this->servers[current($this->shuffledServersIndexes)];
    }

    protected function getRandomSlave()
    {
        return $this->slavesCount ? $this->slaves[current($this->shuffledSlavesIndexes)] : $this->getRandomServer();
    }
 
}
