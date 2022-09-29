<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\Config;
use Phlib\Flysystem\Pdo\PdoAdapter;

/**
 * @group integration
 */
class MemoryTest extends IntegrationTestCase
{
    protected static array $tempFileSize = [
        's' => 10 * 1024,
        'xl' => 10 * 1024 * 1024,
    ];

    private const MEMORY_LIMIT = '250M';

    private PdoAdapter $adapter;

    private Config $emptyConfig;

    private string $previousMemoryLimit;

    protected function setUp(): void
    {
        parent::setUp();

        $this->adapter = new PdoAdapter(static::getTestDbAdapter());

        $config = [];
        if (static::getDbDriverName() === 'mysql') {
            $config['disable_mysql_buffering'] = true;
        }
        $this->emptyConfig = new Config($config);

        // Set up memory limit
        $current = ini_get('memory_limit');
        if ($current !== self::MEMORY_LIMIT) {
            $this->previousMemoryLimit = ini_set('memory_limit', self::MEMORY_LIMIT);
        }
    }

    protected function tearDown(): void
    {
        // Restore previous memory limit
        if (isset($this->previousMemoryLimit)) {
            ini_set('memory_limit', $this->previousMemoryLimit);
            unset($this->previousMemoryLimit);
        }

        unset($this->emptyConfig);
        unset($this->adapter);

        parent::tearDown();
    }

    public function testMemoryUsageOnWritingStream(): void
    {
        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $variation = 1; // MiB
        $this->memoryTest(function () use ($path, $file): void {
            $this->adapter->writeStream($path, $file, $this->emptyConfig);
        }, $variation);
    }

    public function testMemoryUsageOnReadingStreamWithBuffering(): void
    {
        $config = $this->emptyConfig;
        if (static::getDbDriverName() === 'mysql') {
            $config = new Config([
                'enable_mysql_buffering' => true,
            ]);
        }
        $adapter = new PdoAdapter(static::getTestDbAdapter(), $config);

        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $adapter->writeStream($path, $file, $this->emptyConfig);

        $variation = 1; // MiB
        $this->memoryTest(function () use ($adapter, $path): void {
            $adapter->readStream($path);
        }, $variation);
    }

    public function testMemoryUsageOnReadingStreamWithoutBuffering(): void
    {
        if (static::getDbDriverName() !== 'mysql') {
            static::markTestSkipped('Cannot test buffering on non mysql driver.');
        }

        $config = new Config([
            'enable_mysql_buffering' => false,
        ]);
        $adapter = new PdoAdapter(static::getTestDbAdapter(), $config);

        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $adapter->writeStream($path, $file, $this->emptyConfig);

        $variation = 1; // MiB
        $this->memoryTest(function () use ($adapter, $path): void {
            $adapter->readStream($path);
        }, $variation);
    }

    public function testMemoryUsageOnUpdateStream(): void
    {
        $path = '/path/to/file.txt';
        $file = fopen(static::$tempFiles['s'], 'r');
        $this->adapter->writeStream($path, $file, $this->emptyConfig);
        fclose($file);

        $file = fopen(static::$tempFiles['xl'], 'r');

        $variation = 1; // MiB
        $this->memoryTest(function () use ($path, $file): void {
            $this->adapter->updateStream($path, $file, $this->emptyConfig);
        }, $variation);
    }

    private function memoryTest(\Closure $unit, int $variationMiB = 1): void
    {
        // convert variation from mebibytes to bytes
        $variation = $variationMiB * 1048576;
        if ($variation === PHP_INT_MAX) {
            throw new \InvalidArgumentException('Specified variation exceeds PHP_INT_MAX');
        }

        $initial = memory_get_peak_usage();
        $unit();
        $final = memory_get_peak_usage();

        $difference = $final - $initial;

        $message = sprintf(
            'The memory was exceeded by %dMiB above the %dMiB variation limit.',
            round(($difference - $variation) / 1024 / 1024, 1),
            $variationMiB,
        );
        static::assertLessThanOrEqual($variation, $difference, $message);
    }
}
