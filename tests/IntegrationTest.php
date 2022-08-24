<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use Phlib\Flysystem\Pdo\PdoAdapter;

/**
 * @group integration
 */
class IntegrationTest extends IntegrationTestCase
{
    use MemoryTestTrait;

    protected static array $tempFileSize = [
        '00B' => 0,
        '10B' => 10,
        '10K' => 10 * 1024,
        'xl' => 10 * 1024 * 1024,
    ];

    private PdoAdapter $adapter;

    private Config $emptyConfig;

    private array $tempHandles = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->adapter = new PdoAdapter(static::getTestDbAdapter());

        $config = [];
        if (static::getDbDriverName() === 'mysql') {
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

        unset($this->emptyConfig);
        unset($this->adapter);
        parent::tearDown();
    }

    public function testWritingEmptyFile(): void
    {
        $filename = static::$tempFiles['00B'];
        $handle = fopen($filename, 'r');
        $this->adapter->writeStream('/path/to/file.txt', $handle, $this->emptyConfig);
        static::assertRowCount(0, 'flysystem_chunk');
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

        $sql = "SELECT is_compressed FROM flysystem_path WHERE path_id = {$meta['path_id']}";
        $actual = static::getTestDbAdapter()->query($sql)->fetchColumn();

        // @todo ^php81: Native types in php81 mean that `$actual` will already be an int
        static::assertSame(1, (int)$actual);
    }

    public function testCopyingPathMakesAccurateCopy(): void
    {
        $origPath = '/path/to/file.txt';
        $content = file_get_contents(static::$tempFiles['10B']);
        $this->adapter->write($origPath, $content, $this->emptyConfig);

        $copyPath = '/path/to/copy.txt';
        $this->adapter->copy($origPath, $copyPath);

        $select = 'SELECT type, mimetype, visibility, size, is_compressed FROM flysystem_path WHERE path = "%s"';
        $origDataSet = static::getTestDbAdapter()->query(sprintf($select, $origPath))->fetchAll();
        $copyDataSet = static::getTestDbAdapter()->query(sprintf($select, $copyPath))->fetchAll();

        static::assertSame($origDataSet, $copyDataSet);
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

        $select = 'SELECT chunk_no, content FROM flysystem_chunk JOIN flysystem_path USING (path_id) WHERE path = "%s"';
        $origDataSet = static::getTestDbAdapter()->query(sprintf($select, $origPath))->fetchAll();
        $copyDataSet = static::getTestDbAdapter()->query(sprintf($select, $copyPath))->fetchAll();

        static::assertSame($origDataSet, $copyDataSet);
    }

    public function testMemoryUsageOnWritingStream(): void
    {
        $filename = static::$tempFiles['xl'];
        $file = fopen($filename, 'r');
        $path = '/path/to/file.txt';

        $variation = 1048576; // 1MiB
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

        $variation = 1048576; // 1MiB
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

        $variation = 1048576; // 1MiB
        $this->memoryTest(function () use ($adapter, $path): void {
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
        $this->memoryTest(function () use ($path, $file): void {
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
        static::assertRowCount($expectedRows, 'flysystem_path');
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

        static::assertRowCount(2, 'flysystem_path');
        $this->adapter->deleteDir('/test');
        static::assertRowCount(0, 'flysystem_path');
    }

    public function testDeletingFileClearsAllChunks(): void
    {
        $file = file_get_contents(static::$tempFiles['xl']);
        $this->adapter->write('/test.txt', $file, $this->emptyConfig);

        static::assertRowCount(8, 'flysystem_chunk');
        $this->adapter->delete('/test.txt');
        static::assertRowCount(0, 'flysystem_chunk');
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

        $select = "SELECT visibility FROM flysystem_path WHERE path_id = {$meta['path_id']}";
        $actual = static::getTestDbAdapter()->query($select)->fetchColumn();

        static::assertSame(AdapterInterface::VISIBILITY_PRIVATE, $actual);
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
