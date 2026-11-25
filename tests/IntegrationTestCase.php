<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    /**
     * Lowercase letters and spaces only, so test file content is always detected as text/plain.
     */
    private const RANDOM_CHARACTERS = 'abcdefghijklmnopqrstuvwxyz ';

    private static \PDO $testDbAdapter;

    private static string $driver;

    protected static array $tempFileSize = [];

    protected static array $tempFiles = [];

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

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        if (!getenv('INTEGRATION_ENABLED')) {
            // Integration test not enabled
            return;
        }

        // create files
        $tmpDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR;
        foreach (static::$tempFileSize as $name => $size) {
            $filename = $tmpDir . uniqid('flysystempdo-test-' . $name, true);
            static::fillFile($filename, $size);
            static::$tempFiles[$name] = $filename;
        }
    }

    public static function tearDownAfterClass(): void
    {
        foreach (static::$tempFiles as $file) {
            if (!empty($file) && is_file($file)) {
                unlink($file);
            }
        }

        parent::tearDownAfterClass();
    }

    private static function fillFile($filename, $sizeKb): void
    {
        // The content must always be detected as text/plain. Random characters can look like the start of another
        // format to libmagic (e.g. HP-GL, troff, PBM), so start with a fixed plain text line, then use only random
        // lowercase letters and spaces, with no newlines or punctuation.
        $header = "flysystem-pdo test file\n";
        $chunkSize = 1024;
        $handle = fopen($filename, 'wb+');
        for ($i = 0; $i < $sizeKb; $i += $chunkSize) {
            $chunk = ($i === 0) ? $header : '';
            $chunk .= static::randomString($chunkSize - strlen($chunk));
            fwrite($handle, $chunk);
        }
        fclose($handle);
    }

    private static function randomString(int $length): string
    {
        $end = strlen(self::RANDOM_CHARACTERS) - 1;

        $string = '';
        for ($i = 0; $i < $length; $i++) {
            $string .= self::RANDOM_CHARACTERS[random_int(0, $end)];
        }
        return $string;
    }

    protected function setUp(): void
    {
        if (!getenv('INTEGRATION_ENABLED')) {
            static::markTestSkipped();
        }

        parent::setUp();

        $db = static::getTestDbAdapter();
        if (static::getDbDriverName() === 'sqlite') {
            $db->query('DELETE FROM flysystem_path');
            $db->query('DELETE FROM flysystem_chunk');
        } else {
            $db->query('TRUNCATE flysystem_path');
            $db->query('TRUNCATE flysystem_chunk');
        }
    }

    final protected static function assertRowCount(int $expectedCount, string $tableName, string $message = ''): void
    {
        $sql = 'SELECT COUNT(*) FROM ' . $tableName;
        $rowCount = (int) static::getTestDbAdapter()->query($sql)->fetchColumn();

        static::assertSame($expectedCount, $rowCount, $message);
    }
}
