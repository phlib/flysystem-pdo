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
    protected static array $tempFileSize = [
        '00B' => 0,
        '10B' => 10,
        '10K' => 10 * 1024,
        'l' => 2 * 1024 * 1024,
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

    public function testWrittenAndReadWithExpiryFuture(): void
    {
        $path = '/path/to/file.txt';
        $expiry = date('Y-m-d H:i:s', strtotime('+2 day'));
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $config);
        $actual = $this->adapter->read($path);

        static::assertSame($expiry, $actual['expiry']);
    }

    public function testWrittenAndReadWithExpiryPast(): void
    {
        $path = '/path/to/file.txt';
        $expiry = date('Y-m-d H:i:s', strtotime('-2 day'));
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $config);
        $actual = $this->adapter->read($path);

        static::assertFalse($actual);
    }

    public function testWrittenAndReadWithAdditional(): void
    {
        $path = '/path/to/file.txt';
        $additional = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config = new Config([
            'meta' => $additional,
        ]);

        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $config);
        $actual = $this->adapter->read($path);

        static::assertSame($additional, $actual['meta']);
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

    public function testUpdateWhenExpiryFuture(): void
    {
        $path = '/path/to/file.txt';
        $expiry = date('Y-m-d H:i:s', strtotime('+2 day'));
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content1 = sha1(uniqid('Test content 1'));
        $content2 = sha1(uniqid('Test content 2'));

        $this->adapter->write($path, $content1, $config);

        $this->adapter->update($path, $content2, $this->emptyConfig);

        $actual = $this->adapter->read($path);

        static::assertSame($expiry, $actual['expiry']);
    }

    public function testUpdateWhenExpiryPast(): void
    {
        $path = '/path/to/file.txt';
        $expiry = date('Y-m-d H:i:s', strtotime('-2 day'));
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content1 = sha1(uniqid('Test content 1'));
        $content2 = sha1(uniqid('Test content 2'));

        $this->adapter->write($path, $content1, $config);

        $actual = $this->adapter->update($path, $content2, $this->emptyConfig);

        static::assertFalse($actual);
    }

    public function testUpdateWithExpiryAddToFuture(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $this->emptyConfig);

        $expiry = date('Y-m-d H:i:s', strtotime('+2 day'));
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $this->adapter->update($path, $content, $config);

        $actual = $this->adapter->read($path);

        static::assertSame($expiry, $actual['expiry']);
    }

    public function testUpdateWithExpiryAddToPast(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $this->emptyConfig);

        $expiry = date('Y-m-d H:i:s', strtotime('-2 day'));
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $actual = $this->adapter->update($path, $content, $config);

        static::assertFalse($actual);
    }

    public function testUpdateWithExpiryChangeToFuture(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $expiry1 = date('Y-m-d H:i:s', strtotime('+2 day'));
        $config1 = new Config([
            'expiry' => $expiry1,
        ]);

        $this->adapter->write($path, $content, $config1);

        $expiry2 = date('Y-m-d H:i:s', strtotime('+5 day'));
        $config2 = new Config([
            'expiry' => $expiry2,
        ]);

        $this->adapter->update($path, $content, $config2);

        $actual = $this->adapter->read($path);

        static::assertSame($expiry2, $actual['expiry']);
    }

    public function testUpdateWithExpiryChangeToPast(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $expiry1 = date('Y-m-d H:i:s', strtotime('+2 day'));
        $config1 = new Config([
            'expiry' => $expiry1,
        ]);

        $this->adapter->write($path, $content, $config1);

        $expiry2 = date('Y-m-d H:i:s', strtotime('-2 day'));
        $config2 = new Config([
            'expiry' => $expiry2,
        ]);

        $actual = $this->adapter->update($path, $content, $config2);

        static::assertFalse($actual);
    }

    public function testUpdateWhenAdditionalSet(): void
    {
        $path = '/path/to/file.txt';
        $additional = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config = new Config([
            'meta' => $additional,
        ]);

        $content1 = sha1(uniqid('Test content 1'));
        $content2 = sha1(uniqid('Test content 2'));

        $this->adapter->write($path, $content1, $config);

        $this->adapter->update($path, $content2, $this->emptyConfig);

        $actual = $this->adapter->read($path);

        static::assertSame($additional, $actual['meta']);
    }

    public function testUpdateWithAdditionalAdd(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $this->emptyConfig);

        $additional = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config = new Config([
            'meta' => $additional,
        ]);

        $this->adapter->update($path, $content, $config);

        $actual = $this->adapter->read($path);

        static::assertSame($additional, $actual['meta']);
    }

    public function testUpdateWithAdditionalChange(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $additional1 = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config1 = new Config([
            'meta' => $additional1,
        ]);

        $this->adapter->write($path, $content, $config1);

        $additional2 = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config2 = new Config([
            'meta' => $additional2,
        ]);

        $this->adapter->update($path, $content, $config2);

        $actual = $this->adapter->read($path);

        static::assertSame($additional2, $actual['meta']);
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

    public function testRenameFile(): void
    {
        $path1 = '/first.txt';
        $path2 = '/second.txt';

        $testContent = file_get_contents(static::$tempFiles['10B']);

        $this->adapter->write($path1, $testContent, $this->emptyConfig);

        // Check successfully written, first
        $actualOld = $this->adapter->read($path1);
        static::assertSame($testContent, $actualOld['contents']);

        // Perform rename; check original path is now missing and new path is present
        $this->adapter->rename($path1, $path2);

        $actual1New = $this->adapter->read($path1);
        $actual2New = $this->adapter->read($path2);

        static::assertFalse($actual1New);
        static::assertSame($testContent, $actual2New['contents']);
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

    public function testCreateDirectoryWithAdditional(): void
    {
        $path = '/new/directory';
        $additional = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config = new Config([
            'meta' => $additional,
        ]);

        $this->adapter->createDir($path, $config);
        $actual = $this->adapter->read($path);

        static::assertSame($additional, $actual['meta']);
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
        $file = file_get_contents(static::$tempFiles['l']);
        $this->adapter->write('/test.txt', $file, $this->emptyConfig);

        static::assertRowCount(2, 'flysystem_chunk');
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

    public function testDeleteExpired(): void
    {
        $content = sha1(uniqid('Test content'));

        // File with no expiry
        $pathNone = '/path/with/expiry-none.txt';
        $this->adapter->write($pathNone, $content, $this->emptyConfig);

        // File with past expiry
        $pathPast = '/path/with/expiry-past.txt';
        $expiryPast = date('Y-m-d H:i:s', strtotime('-2 day'));
        $configPast = new Config([
            'expiry' => $expiryPast,
        ]);
        $this->adapter->write($pathPast, $content, $configPast);

        // File with future expiry
        $pathFuture = '/path/with/expiry-future.txt';
        $expiryFuture = date('Y-m-d H:i:s', strtotime('+2 day'));
        $configFuture = new Config([
            'expiry' => $expiryFuture,
        ]);
        $this->adapter->write($pathFuture, $content, $configFuture);

        $listBefore = $this->adapter->listContents('/path/with');
        static::assertCount(3, $listBefore);

        $this->adapter->deleteExpired();

        $listAfter = $this->adapter->listContents('/path/with');
        static::assertCount(2, $listAfter);

        // Check it's the expired one that has gone
        $actual = $this->adapter->read($pathPast);
        static::assertFalse($actual);
    }

    /**
     * @return resource
     */
    private function createResource(string $filename)
    {
        $handle = fopen($filename, 'r');
        $this->tempHandles[] = $handle;
        return $handle;
    }
}
