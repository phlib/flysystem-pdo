<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    use IntegrationAdapterTrait;

    protected static array $tempFileSize = [];

    protected static array $tempFiles = [];

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
        $chunkSize = 1024;
        $handle = fopen($filename, 'wb+');
        for ($i = 0; $i < $sizeKb; $i += $chunkSize) {
            fwrite($handle, static::randomString($chunkSize), $chunkSize);
        }
        fclose($handle);
    }

    private static function randomString($length): string
    {
        static $characters;
        static $charLength;
        if (!$characters) {
            $characters = array_merge(range(0, 9), range('a', 'z'), range('A', 'Z'), [',', '.', ' ', "\n"]);
            $charLength = count($characters);
            shuffle($characters);
        }

        $string = '';
        $end = ($charLength - 1);
        for ($i = 0; $i < $length; $i++) {
            $string .= $characters[rand(0, $end)];
        }
        return $string;
    }

    protected function setUp(): void
    {
        if (!getenv('INTEGRATION_ENABLED')) {
            static::markTestSkipped();
        }

        parent::setUp();

        self::setUpAdapter();
    }

    final protected static function assertRowCount(int $expectedCount, string $tableName, string $message = ''): void
    {
        $sql = 'SELECT COUNT(*) FROM ' . $tableName;
        $rowCount = (int) static::getTestDbAdapter()->query($sql)->fetchColumn();

        static::assertSame($expectedCount, $rowCount, $message);
    }
}
