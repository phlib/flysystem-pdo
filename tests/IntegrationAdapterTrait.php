<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

trait IntegrationAdapterTrait
{
    private static \PDO $testDbAdapter;

    private static string $driver;

    final protected static function getTestDbAdapter(): \PDO
    {
        if (!isset(static::$testDbAdapter)) {
            static::$driver = getenv('DB_DRIVER');
            switch (static::$driver) {
                case 'mysql':
                    $dsn = 'mysql:host=' . getenv('DB_HOST') . ';port=' . getenv('DB_PORT') . ';dbname=' . getenv('DB_DATABASE');
                    $createSqlFile = __DIR__ . '/../schema/mysql.sql';
                    break;
                case 'sqlite':
                    $dsn = 'sqlite::memory:';
                    $createSqlFile = __DIR__ . '/../schema/sqlite.sql';
                    break;
                default:
                    throw new \DomainException('Unsupported DB driver');
            }

            static::$testDbAdapter = new \PDO(
                $dsn,
                getenv('DB_USERNAME'),
                getenv('DB_PASSWORD'),
                [
                    \PDO::ATTR_TIMEOUT => 2,
                    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                ],
            );

            static::$testDbAdapter->query('DROP TABLE IF EXISTS flysystem_chunk');
            static::$testDbAdapter->query('DROP TABLE IF EXISTS flysystem_path');

            $createSql = explode(';', file_get_contents($createSqlFile));
            // Remove the element that contains only the file's trailing line
            array_pop($createSql);
            foreach ($createSql as $sql) {
                static::$testDbAdapter->query($sql);
            }

            if (static::$driver === 'mysql') {
                $timezone = date_default_timezone_get();
                static::$testDbAdapter->query("SET time_zone = '{$timezone}'");
            }
        }

        return static::$testDbAdapter;
    }

    final protected static function getDbDriverName(): string
    {
        return static::$driver;
    }

    final protected static function setUpAdapter(): void
    {
        $db = static::getTestDbAdapter();
        if (static::getDbDriverName() === 'sqlite') {
            $db->query('DELETE FROM flysystem_path');
            $db->query('DELETE FROM flysystem_chunk');
        } else {
            $db->query('TRUNCATE flysystem_path');
            $db->query('TRUNCATE flysystem_chunk');
        }
    }
}
