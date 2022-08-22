<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    private static \PDO $testDbAdapter;

    private static string $driver;

    final protected static function getTestDbAdapter(): \PDO
    {
        if (!isset(self::$testDbAdapter)) {
            // @todo allow tests to use alternative to MySQL
            $dsn = 'mysql:host=' . getenv('DB_HOST') . ';port=' . getenv('DB_PORT') . ';dbname=' . getenv('DB_DATABASE');
            static::$driver = 'mysql';
            static::$testDbAdapter = new \PDO(
                $dsn,
                getenv('DB_USERNAME'),
                getenv('DB_PASSWORD'),
                [
                    \PDO::ATTR_TIMEOUT => 2,
                    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                ]
            );
        }
        return self::$testDbAdapter;
    }

    final protected static function getDbDriverName(): string
    {
        return static::$driver;
    }

    protected function setUp(): void
    {
        if (!getenv('INTEGRATION_ENABLED')) {
            static::markTestSkipped();
        }

        parent::setUp();

        static::getTestDbAdapter()->query('TRUNCATE flysystem_path');
        static::getTestDbAdapter()->query('TRUNCATE flysystem_chunk');
    }

    final protected static function assertRowCount(int $expectedCount, string $tableName, string $message = ''): void
    {
        $sql = 'SELECT COUNT(*) FROM ' . $tableName;
        $rowCount = (int)self::getTestDbAdapter()->query($sql)->fetchColumn();

        static::assertSame($expectedCount, $rowCount, $message);
    }
}
