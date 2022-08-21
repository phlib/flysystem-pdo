<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use Phlib\Flysystem\Pdo\PdoAdapter;
use PHPUnit\DbUnit\Database\Connection;
use PHPUnit\DbUnit\DataSet\ArrayDataSet;
use PHPUnit\DbUnit\DataSet\IDataSet;
use PHPUnit\DbUnit\TestCase;

/**
 * @group integration
 */
class IntegrationTest extends TestCase
{
    use MemoryTestTrait;

    /**
     * @var \PDO
     */
    protected static $pdo;

    /**
     * @var string
     */
    protected static $driver;

    /**
     * @var array
     */
    protected static $tempFiles = [];

    /**
     * @var PdoAdapter
     */
    protected $adapter;

    /**
     * @var Config
     */
    protected $emptyConfig;

    /**
     * @var array
     */
    protected $tempHandles = [];

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        if (!getenv('INTEGRATION_ENABLED')) {
            // Integration test not enabled
            return;
        }

        // @todo allow tests to use alternative to MySQL
        $dsn = 'mysql:host=' . getenv('DB_HOST') . ';port=' . getenv('DB_PORT') . ';dbname=' . getenv('DB_DATABASE');
        static::$driver = 'mysql';
        static::$pdo = new \PDO(
            $dsn,
            getenv('DB_USERNAME'),
            getenv('DB_PASSWORD'),
            [
                \PDO::ATTR_TIMEOUT => 2,
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            ]
        );

        // create files
        $tmpDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR;
        $emptyFilename = $tmpDir . uniqid('flysystempdo-test-00B-', true);
        $tenByteFilename = $tmpDir . uniqid('flysystempdo-test-10B-', true);
        $tenKayFilename = $tmpDir . uniqid('flysystempdo-test-10K-', true);
        $xlFilename = $tmpDir . uniqid('flysystempdo-test-xl-', true);
        static::fillFile($emptyFilename, 0);
        static::fillFile($tenByteFilename, 10);
        static::fillFile($tenKayFilename, 10 * 1024);
        static::fillFile($xlFilename, 10 * 1024 * 1024);
        static::$tempFiles = [
            '00B' => $emptyFilename,
            '10B' => $tenByteFilename,
            '10K' => $tenKayFilename,
            'xl' => $xlFilename,
        ];
    }

    public static function tearDownAfterClass(): void
    {
        static::$driver = null;
        static::$pdo = null;
        foreach (static::$tempFiles as $file) {
            if (is_file($file)) {
                unlink($file);
            }
        }
        parent::tearDownAfterClass();
    }

    protected function setUp(): void
    {
        if (!static::$pdo instanceof \PDO) {
            static::markTestSkipped();
            return;
        }

        parent::setUp();

        $this->adapter = new PdoAdapter(static::$pdo);

        $config = [];
        if (static::$driver === 'mysql') {
            $config['disable_mysql_buffering'] = true;
        }
        $this->emptyConfig = new Config($config);
    }

    protected function tearDown(): void
    {
        foreach ($this->tempHandles as $tempHandle) {
            if (is_resource($tempHandle)) {
                fclose($tempHandle);
            }
        }

        $this->emptyConfig = null;
        $this->adapter = null;
        parent::tearDown();
    }

    public function getConnection(): Connection
    {
        return $this->createDefaultDBConnection(static::$pdo, getenv('DB_DATABASE'));
    }

    protected function getDataSet(): IDataSet
    {
        switch (static::$driver) {
            case 'mysql':
                // mysqldump -hdhost --xml -t -uroot -p dbname flysystem_chunk flysystem_path > tests/_files/mysql-integration.xml
                $dataSetFile = __DIR__ . '/_files/mysql-integration.xml';
                return $this->createMySQLXMLDataSet($dataSetFile);
            case 'sqlite':
                $dataSetFile = __DIR__ . '/_files/sqlite-integration.xml';
                return $this->createXMLDataSet($dataSetFile);
            default:
                $driver = static::$driver;
                throw new \Exception("Missing data set for '{$driver}'");
        }
    }

    public function testWritingEmptyFile(): void
    {
        $filename = static::$tempFiles['00B'];
        $handle = fopen($filename, 'r');
        $this->adapter->writeStream('/path/to/file.txt', $handle, $this->emptyConfig);
        static::assertSame(0, $this->getConnection()->getRowCount('flysystem_chunk'));
    }

    /**
     * @dataProvider writtenAndReadAreTheSameFileDataProvider
     */
    public function testWrittenAndReadAreTheSameFile(
        callable $fileCallback,
        string $writeMethod,
        string $readMethod,
        Config $config
    ): void {
        $filename = static::$tempFiles['10K'];
        $file = call_user_func($fileCallback, $filename);

        $path = '/path/to/file.txt';
        $this->adapter->{$writeMethod}($path, $file, $config);
        $meta = $this->adapter->{$readMethod}($path);

        if (is_resource($file)) {
            rewind($file);
            $file = stream_get_contents($file);
        }
        if (isset($meta['stream'])) {
            $meta['contents'] = stream_get_contents($meta['stream']);
        }

        static::assertSame($file, $meta['contents']);
    }

    public function writtenAndReadAreTheSameFileDataProvider(): array
    {
        $compressionConfig = new Config([
            'enable_compression' => true,
        ]);
        $uncompressedConfig = new Config([
            'enable_compression' => false,
        ]);
        return [
            [[$this, 'createResource'], 'writeStream', 'readStream', $compressionConfig],
            [[$this, 'createResource'], 'writeStream', 'read', $compressionConfig],
            ['file_get_contents', 'write', 'readStream', $compressionConfig],
            ['file_get_contents', 'write', 'read', $compressionConfig],
            [[$this, 'createResource'], 'writeStream', 'readStream', $uncompressedConfig],
            [[$this, 'createResource'], 'writeStream', 'read', $uncompressedConfig],
            ['file_get_contents', 'write', 'readStream', $uncompressedConfig],
            ['file_get_contents', 'write', 'read', $uncompressedConfig],
        ];
    }

    /**
     * @dataProvider updatedAndReadAreTheSameFileDataProvider
     */
    public function testUpdatedAndReadAreTheSameFile(
        callable $fileCallback,
        string $updateMethod,
        string $readMethod,
        Config $config
    ): void {
        $path = '/path/to/file.txt';
        $this->adapter->write($path, file_get_contents(static::$tempFiles['10B']), $this->emptyConfig);

        $filename = static::$tempFiles['10K'];
        $file = call_user_func($fileCallback, $filename);

        $this->adapter->{$updateMethod}($path, $file, $config);
        $meta = $this->adapter->{$readMethod}($path);

        if (is_resource($file)) {
            rewind($file);
            $file = stream_get_contents($file);
        }
        if (isset($meta['stream'])) {
            $meta['contents'] = stream_get_contents($meta['stream']);
        }
        static::assertSame($file, $meta['contents']);
    }

    public function updatedAndReadAreTheSameFileDataProvider(): array
    {
        $compressionConfig = new Config([
            'enable_compression' => true,
        ]);
        $uncompressedConfig = new Config([
            'enable_compression' => false,
        ]);
        return [
            [[$this, 'createResource'], 'updateStream', 'readStream', $compressionConfig],
            [[$this, 'createResource'], 'updateStream', 'read', $compressionConfig],
            ['file_get_contents', 'update', 'readStream', $compressionConfig],
            ['file_get_contents', 'update', 'read', $compressionConfig],
            [[$this, 'createResource'], 'updateStream', 'readStream', $uncompressedConfig],
            [[$this, 'createResource'], 'updateStream', 'read', $uncompressedConfig],
            ['file_get_contents', 'update', 'readStream', $uncompressedConfig],
            ['file_get_contents', 'update', 'read', $uncompressedConfig],
        ];
    }

    public function testCopyingFile(): void
    {
        $path1 = '/first.txt';
        $path2 = '/second.txt';
        $this->adapter->write($path1, file_get_contents(static::$tempFiles['10B']), $this->emptyConfig);
        $this->adapter->copy($path1, $path2);

        $meta1 = $this->adapter->read($path1);
        $meta2 = $this->adapter->read($path2);

        static::assertSame($meta1['contents'], $meta2['contents']);
    }

    public function testCompressionIsSetOnThePath(): void
    {
        $filename = static::$tempFiles['10B'];
        $file = $this->createResource($filename);
        $path = '/path/to/file.txt';
        $meta = $this->adapter->writeStream($path, $file, new Config([
            'enable_compression' => true,
        ]));

        $rows = [[
            'is_compressed' => 1,
        ]];
        $sql = "SELECT is_compressed FROM flysystem_path WHERE path_id = {$meta['path_id']}";
        $expected = (new ArrayDataSet([
            'flysystem_path' => $rows,
        ]))->getTable('flysystem_path');
        $actual = $this->getConnection()->createQueryTable('flysystem_path', $sql);

        static::assertTablesEqual($expected, $actual);
    }

    public function testCopyingPathMakesAccurateCopy(): void
    {
        $origPath = '/path/to/file.txt';
        $content = file_get_contents(static::$tempFiles['10B']);
        $this->adapter->write($origPath, $content, $this->emptyConfig);

        $copyPath = '/path/to/copy.txt';
        $this->adapter->copy($origPath, $copyPath);

        $connection = $this->getConnection();
        $select = 'SELECT type, mimetype, visibility, size, is_compressed FROM flysystem_path WHERE path = "%s"';
        $origDataSet = $connection->createQueryTable('flysystem_path', sprintf($select, $origPath));
        $copyDataSet = $connection->createQueryTable('flysystem_path', sprintf($select, $copyPath));

        static::assertTablesEqual($origDataSet, $copyDataSet);
    }

    public function testCopyingPathMakesAccurateCopyOfChunks(): void
    {
        $origPath = '/path/to/file.txt';
        $content = file_get_contents(static::$tempFiles['10B']);
        $uncompressedConfig = new Config([
            'enable_compression' => false,
        ]);
        $this->adapter->write($origPath, $content, $uncompressedConfig);

        $copyPath = '/path/to/copy.txt';
        $this->adapter->copy($origPath, $copyPath);

        $connection = $this->getConnection();
        $select = 'SELECT chunk_no, content FROM flysystem_chunk JOIN flysystem_path USING (path_id) WHERE path = "%s"';
        $origDataSet = $connection->createQueryTable('flysystem_chunk', sprintf($select, $origPath));
        $copyDataSet = $connection->createQueryTable('flysystem_chunk', sprintf($select, $copyPath));

        static::assertTablesEqual($origDataSet, $copyDataSet);
    }

    public function testMemoryUsageOnWritingStream(): void
    {
        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $variation = 1048576; // 1MiB
        $this->memoryTest(function () use ($path, $file) {
            $this->adapter->writeStream($path, $file, $this->emptyConfig);
        }, $variation);
    }

    public function testMemoryUsageOnReadingStreamWithBuffering(): void
    {
        $config = $this->emptyConfig;
        if (static::$driver === 'mysql') {
            $config = new Config([
                'enable_mysql_buffering' => true,
            ]);
        }
        $adapter = new PdoAdapter(static::$pdo, $config);

        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $adapter->writeStream($path, $file, $this->emptyConfig);

        $variation = 1048576; // 1MiB
        $this->memoryTest(function () use ($adapter, $path) {
            $adapter->readStream($path);
        }, $variation);
    }

    public function testMemoryUsageOnReadingStreamWithoutBuffering(): void
    {
        if (static::$driver !== 'mysql') {
            static::markTestSkipped('Cannot test buffering on non mysql driver.');
            return;
        }

        $config = new Config([
            'enable_mysql_buffering' => false,
        ]);
        $adapter = new PdoAdapter(static::$pdo, $config);

        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $adapter->writeStream($path, $file, $this->emptyConfig);

        $variation = 1048576; // 1MiB
        $this->memoryTest(function () use ($adapter, $path) {
            $adapter->readStream($path);
        }, $variation);
    }

    public function testMemoryUsageOnUpdateStream(): void
    {
        $path = '/path/to/file.txt';
        $file = fopen(static::$tempFiles['10K'], 'r');
        $this->adapter->writeStream($path, $file, $this->emptyConfig);
        fclose($file);

        $file = fopen(static::$tempFiles['xl'], 'r');

        $variation = 1048576; // 1MiB
        $this->memoryTest(function () use ($path, $file) {
            $this->adapter->updateStream($path, $file, $this->emptyConfig);
        }, $variation);
    }

    /**
     * @dataProvider pathsDataProvider
     */
    public function testAddingPaths(array $paths, int $expectedRows): void
    {
        foreach ($paths as $path) {
            if ($path['type'] === 'dir') {
                $this->adapter->createDir($path['name'], $this->emptyConfig);
            } else {
                $this->adapter->write($path['name'], '', $this->emptyConfig);
            }
        }
        static::assertSame($expectedRows, $this->getConnection()->getRowCount('flysystem_path'));
    }

    /**
     * @dataProvider pathsDataProvider
     */
    public function testListContentsMeetsExpectedOutput(array $paths, int $expectedRows): void
    {
        foreach ($paths as $path) {
            if ($path['type'] === 'dir') {
                $this->adapter->createDir($path['name'], $this->emptyConfig);
            } else {
                $this->adapter->write($path['name'], '', $this->emptyConfig);
            }
        }

        // extraneous data to throw off the listings
        $this->adapter->createDir('/not/this', $this->emptyConfig);
        $this->adapter->createDir('/not/that', $this->emptyConfig);
        $this->adapter->write('/not/this/too.txt', '', $this->emptyConfig);

        static::assertCount($expectedRows, $this->adapter->listContents('/test', true));
    }

    public function pathsDataProvider(): array
    {
        $dir1 = [
            'type' => 'dir',
            'name' => '/test',
        ];
        $dir2 = [
            'type' => 'dir',
            'name' => '/test/sub1',
        ];
        $dir3 = [
            'type' => 'dir',
            'name' => '/test/sub2',
        ];
        $file1 = [
            'type' => 'file',
            'name' => '/test/file1.txt',
        ];
        $file2 = [
            'type' => 'file',
            'name' => '/test/file2.txt',
        ];
        $file3 = [
            'type' => 'file',
            'name' => '/test/file3.txt',
        ];

        return [
            [[$dir1], 1],
            [[$dir1, $dir2], 2],
            [[$dir1, $dir2, $dir3], 3],
            [[$file1], 1],
            [[$file1, $file2], 2],
            [[$file1, $file2, $file3], 3],
            [[$dir1, $file1], 2],
            [[$dir1, $file1, $file2], 3],
            [[$dir1, $dir2, $file1], 3],
            [[$dir1, $dir2, $dir3, $file1, $file2, $file3], 6],
        ];
    }

    public function testDeletingDirectoryClearsAllFiles(): void
    {
        $this->adapter->createDir('/test', $this->emptyConfig);
        $this->adapter->write('/test/file.txt', '', $this->emptyConfig);

        static::assertSame(2, $this->getConnection()->getRowCount('flysystem_path'));
        $this->adapter->deleteDir('/test');
        static::assertSame(0, $this->getConnection()->getRowCount('flysystem_path'));
    }

    public function testDeletingFileClearsAllChunks(): void
    {
        $file = file_get_contents(static::$tempFiles['xl']);
        $this->adapter->write('/test.txt', $file, $this->emptyConfig);

        static::assertGreaterThan(0, $this->getConnection()->getRowCount('flysystem_chunk'));
        $this->adapter->delete('/test.txt');
        static::assertSame(0, $this->getConnection()->getRowCount('flysystem_chunk'));
    }

    public function testReadingNonExistentPath(): void
    {
        static::assertFalse($this->adapter->read('/path/does/not/exist.txt'));
    }

    public function testReadingStreamForNonExistentPath(): void
    {
        static::assertFalse($this->adapter->readStream('/path/does/not/exist.txt'));
    }

    public function testHasForNonExistentPath(): void
    {
        static::assertFalse($this->adapter->has('/path/does/not/exist.txt'));
    }

    public function testHasForExistingPath(): void
    {
        $path = '/this/path/does/exist.txt';
        $this->adapter->write($path, 'some text', $this->emptyConfig);
        static::assertTrue($this->adapter->has($path));
    }

    public function testCopyingNonExistentPath(): void
    {
        static::assertFalse($this->adapter->copy('/this/does/not/exist.txt', '/my/new/path.txt'));
    }

    public function testSettingVisibility(): void
    {
        $path = '/test.txt';
        $config = new Config([
            'visibility' => AdapterInterface::VISIBILITY_PUBLIC,
        ]);
        $meta = $this->adapter->write($path, 'Some Content', $config);

        $this->adapter->setVisibility($path, AdapterInterface::VISIBILITY_PRIVATE);

        $rows = [[
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
        ]];
        $expected = (new ArrayDataSet([
            'flysystem_path' => $rows,
        ]))->getTable('flysystem_path');
        $select = "SELECT visibility FROM flysystem_path WHERE path_id = {$meta['path_id']}";
        $actual = $this->getConnection()->createQueryTable('flysystem_path', $select);

        static::assertTablesEqual($expected, $actual);
    }

    protected static function fillFile($filename, $sizeKb): void
    {
        $chunkSize = 1024;
        $handle = fopen($filename, 'wb+');
        for ($i = 0; $i < $sizeKb; $i += $chunkSize) {
            fwrite($handle, static::randomString($chunkSize), $chunkSize);
        }
        fclose($handle);
    }

    protected static function randomString($length): string
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

    /**
     * @return resource
     */
    protected function createResource(string $filename)
    {
        $handle = fopen($filename, 'r');
        $this->tempHandles[] = $handle;
        return $handle;
    }
}
