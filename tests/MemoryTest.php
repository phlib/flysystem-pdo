<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\Config;
use Phlib\Flysystem\Pdo\PdoAdapter;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
class MemoryTest extends IntegrationTestCase
{
    protected static array $tempFileSize = [
        's' => 10 * 1024,
        'xl' => 10 * 1024 * 1024,
    ];

    private const MEMORY_LIMIT = '250M';

    /**
     * Each chunk is briefly held in memory several times over while it is sent to/from the DB (~4x for MySQL),
     * so use a chunk size small enough to keep this overhead well below the size of the 'xl' file.
     */
    private const CHUNK_SIZE = 256 * 1024;

    /**
     * Allowed increase in peak memory (MiB) for streaming the 10MiB 'xl' file. Streaming has a fixed overhead that
     * does not grow with file size: a few multiples of CHUNK_SIZE, plus up to 2MiB held in memory by the
     * php://temp stream returned from a read. Loading the whole file into memory would exceed this by far.
     */
    private const MEMORY_VARIATION = 4;

    private string $previousMemoryLimit;

    protected function setUp(): void
    {
        parent::setUp();

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

        parent::tearDown();
    }

    public function testMemoryUsageOnWritingStream(): void
    {
        $adapter = $this->createAdapter();

        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $this->warmUp($adapter);

        $variation = self::MEMORY_VARIATION;
        $this->memoryTest(function () use ($adapter, $path, $file): void {
            $adapter->writeStream($path, $file, new Config());
        }, $variation);
    }

    /**
     * This asserts a minimum memory increase, which a higher peak reached earlier in the process would hide.
     * Without memory_reset_peak_usage() (PHP < 8.2) the only way to avoid that is a fresh process.
     * @todo php82
     *
     * @runInSeparateProcess
     * @preserveGlobalState disabled
     */
    public function testMemoryUsageOnReadingStreamWithBuffering(): void
    {
        if (static::getDbDriverName() !== 'mysql') {
            static::markTestSkipped('Cannot test buffering on non-MySQL driver.');
        }

        $pdo = static::getTestDbAdapter();
        $adapter = $this->createAdapter([
            'disable_mysql_buffering' => false,
        ]);

        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $adapter->writeStream($path, $file, new Config());
        $this->warmUp($adapter);

        // The adapter never enables buffering, and the shared connection has had it disabled by other adapters,
        // so enable it directly to check the effect of a buffered read
        $pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);
        try {
            $difference = $this->measurePeakMemory(function () use ($adapter, $path): void {
                $adapter->readStream($path);
            });
        } finally {
            $pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        }

        // Buffering loads the whole (compressed) file into memory, which is much more than the unbuffered read
        $expectedMinimum = (int) (filesize($filename) / 2);
        static::assertGreaterThanOrEqual(
            $expectedMinimum,
            $difference,
            'A buffered read is expected to load the whole result set into memory',
        );
    }

    public function testMemoryUsageOnReadingStreamWithoutBuffering(): void
    {
        if (static::getDbDriverName() !== 'mysql') {
            static::markTestSkipped('Cannot test buffering on non-MySQL driver.');
        }

        $adapter = $this->createAdapter([
            'disable_mysql_buffering' => true,
        ]);

        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $adapter->writeStream($path, $file, new Config());
        $this->warmUp($adapter);

        $variation = self::MEMORY_VARIATION;
        $this->memoryTest(function () use ($adapter, $path): void {
            $adapter->readStream($path);
        }, $variation);
    }

    public function testMemoryUsageOnUpdateStream(): void
    {
        $adapter = $this->createAdapter();

        $path = '/path/to/file.txt';
        $file = fopen(static::$tempFiles['s'], 'r');
        $adapter->writeStream($path, $file, new Config());
        fclose($file);

        $file = fopen(static::$tempFiles['xl'], 'r');
        $this->warmUp($adapter);

        $variation = self::MEMORY_VARIATION;
        $this->memoryTest(function () use ($adapter, $path, $file): void {
            $adapter->updateStream($path, $file, new Config());
        }, $variation);
    }

    private function createAdapter(array $config = []): PdoAdapter
    {
        return new PdoAdapter(static::getTestDbAdapter(), new Config($config + [
            'chunk_size' => self::CHUNK_SIZE,
        ]));
    }

    /**
     * Run each operation once on a small file, so one-off costs (autoloading, mimetype map, DB driver buffers)
     * are not counted against the operation being measured
     */
    private function warmUp(PdoAdapter $adapter): void
    {
        $path = '/warm-up.txt';
        $file = fopen(static::$tempFiles['s'], 'r');
        $adapter->writeStream($path, $file, new Config());
        rewind($file);
        $adapter->updateStream($path, $file, new Config());
        fclose($file);
        $read = $adapter->readStream($path);
        fclose($read['stream']);
        $adapter->delete($path);
    }

    private function memoryTest(\Closure $unit, int $variationMiB = 1): void
    {
        // convert variation from mebibytes to bytes
        $variation = $variationMiB * 1048576;
        if ($variation === PHP_INT_MAX) {
            throw new \InvalidArgumentException('Specified variation exceeds PHP_INT_MAX');
        }

        $difference = $this->measurePeakMemory($unit);

        $message = sprintf(
            'The memory was exceeded by %dMiB above the %dMiB variation limit.',
            round(($difference - $variation) / 1024 / 1024, 1),
            $variationMiB,
        );
        static::assertLessThanOrEqual($variation, $difference, $message);
    }

    /**
     * @return int Increase in peak memory usage, in bytes
     */
    private function measurePeakMemory(\Closure $unit): int
    {
        // A peak reached earlier in the process would hide the memory used by $unit, so reset where possible (PHP 8.2+)
        // @todo php82
        if (function_exists('memory_reset_peak_usage')) {
            memory_reset_peak_usage();
        }

        $initial = memory_get_peak_usage();
        $unit();
        $final = memory_get_peak_usage();

        return $final - $initial;
    }
}
