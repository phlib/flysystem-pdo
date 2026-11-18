<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\Visibility;
use Phlib\Flysystem\Pdo\PdoAdapter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
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

    #[DataProvider('dataWriteReadStreamCompression')]
    public function testWrittenAndReadAreTheSameFile(
        ?callable $fileCallback,
        string $writeMethod,
        string $readMethod,
        Config $config,
    ): void {
        $filename = static::$tempFiles['10K'];
        $file = is_callable($fileCallback) ? $fileCallback($filename) : $this->createResource($filename);

        $path = '/path/to/file.txt';
        $this->adapter->{$writeMethod}($path, $file, $config);
        $actual = $this->adapter->{$readMethod}($path);

        if (is_resource($file)) {
            rewind($file);
            $file = stream_get_contents($file);
        }
        if (is_resource($actual)) {
            $actual = stream_get_contents($actual);
        }

        static::assertSame($file, $actual);
    }

    public static function dataWriteReadStreamCompression(): array
    {
        $compressionConfig = new Config([
            'enable_compression' => true,
        ]);
        $uncompressedConfig = new Config([
            'enable_compression' => false,
        ]);
        return [
            [null, 'writeStream', 'readStream', $compressionConfig],
            [null, 'writeStream', 'read', $compressionConfig],
            ['file_get_contents', 'write', 'readStream', $compressionConfig],
            ['file_get_contents', 'write', 'read', $compressionConfig],
            [null, 'writeStream', 'readStream', $uncompressedConfig],
            [null, 'writeStream', 'read', $uncompressedConfig],
            ['file_get_contents', 'write', 'readStream', $uncompressedConfig],
            ['file_get_contents', 'write', 'read', $uncompressedConfig],
        ];
    }

    public function testWrittenAndReadWithExpiryFuture(): void
    {
        $path = '/path/to/file.txt';
        $expiry = new \DateTimeImmutable('+2 day');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $config);
        $actual = $this->adapter->expiry($path)->expiry();

        static::assertSame($expiry->format('Y-m-d H:i:s'), $actual->format('Y-m-d H:i:s'));
    }

    public function testWrittenAndReadWithExpiryPast(): void
    {
        $path = '/path/to/file.txt';
        $expiry = new \DateTimeImmutable('-2 day');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $config);

        // An already-expired file is not stored
        static::assertFalse($this->adapter->fileExists($path));
        static::assertRowCount(0, 'flysystem_path');
        static::assertRowCount(0, 'flysystem_chunk');

        $this->expectException(UnableToRetrieveMetadata::class);
        $this->adapter->expiry($path);
    }

    public function testWriteOverExpiredPath(): void
    {
        $path = '/path/to/file.txt';
        $content1 = sha1(uniqid('Test content 1'));
        $content2 = sha1(uniqid('Test content 2'));

        $this->writeExpiredFile($path, $content1);
        $this->adapter->write($path, $content2, $this->emptyConfig);

        static::assertSame($content2, $this->adapter->read($path));
        static::assertNull($this->adapter->expiry($path)->expiry());

        // The expired file is replaced
        static::assertRowCount(1, 'flysystem_path');
        static::assertRowCount(1, 'flysystem_chunk');
        static::assertSame(0, $this->adapter->deleteExpired());
    }

    public function testPathsAreCaseSensitive(): void
    {
        $this->adapter->write('/path/file.txt', 'lower', $this->emptyConfig);
        $this->adapter->write('/path/FILE.txt', 'upper', $this->emptyConfig);

        static::assertSame('lower', $this->adapter->read('/path/file.txt'));
        static::assertSame('upper', $this->adapter->read('/path/FILE.txt'));
        static::assertFalse($this->adapter->fileExists('/path/File.txt'));
        static::assertCount(2, iterator_to_array($this->adapter->listContents('/path')));
        static::assertCount(0, iterator_to_array($this->adapter->listContents('/PATH')));
    }

    public function testPathIsUnique(): void
    {
        $this->adapter->write('/path/file.txt', 'content', $this->emptyConfig);
        // Writing to an existing path updates the file
        $this->adapter->write('/path/file.txt', 'other', $this->emptyConfig);
        static::assertSame('other', $this->adapter->read('/path/file.txt'));
        static::assertRowCount(1, 'flysystem_path');

        // The index prevents a duplicate, e.g. from a race between two writers
        $this->expectException(\PDOException::class);
        $this->expectExceptionCode('23000');
        static::getTestDbAdapter()->exec(
            "INSERT INTO flysystem_path (type, path) VALUES ('file', '/path/file.txt')",
        );
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
        $actual = $this->adapter->extra($path)->extraMetadata();

        static::assertSame($additional, $actual);
    }

    #[DataProvider('dataWriteReadStreamCompression')]
    public function testUpdatedAndReadAreTheSameFile(
        ?callable $fileCallback,
        string $updateMethod,
        string $readMethod,
        Config $config,
    ): void {
        $path = '/path/to/file.txt';
        $this->adapter->write($path, file_get_contents(static::$tempFiles['10B']), $this->emptyConfig);

        $filename = static::$tempFiles['10K'];
        $file = is_callable($fileCallback) ? $fileCallback($filename) : $this->createResource($filename);

        $this->adapter->{$updateMethod}($path, $file, $config);
        $actual = $this->adapter->{$readMethod}($path);

        if (is_resource($file)) {
            rewind($file);
            $file = stream_get_contents($file);
        }
        if (is_resource($actual)) {
            $actual = stream_get_contents($actual);
        }
        static::assertSame($file, $actual);
    }

    public function testUpdateWhenExpiryFuture(): void
    {
        $path = '/path/to/file.txt';
        $expiry = new \DateTimeImmutable('+2 day');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content1 = sha1(uniqid('Test content 1'));
        $content2 = sha1(uniqid('Test content 2'));

        $this->adapter->write($path, $content1, $config);
        $this->adapter->write($path, $content2, $this->emptyConfig);

        $actual = $this->adapter->expiry($path)->expiry();

        static::assertSame($expiry->format('Y-m-d H:i:s'), $actual->format('Y-m-d H:i:s'));
    }

    public function testUpdateWhenExpiryPast(): void
    {
        $path = '/path/to/file.txt';
        $expiry = new \DateTimeImmutable('-2 day');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $content1 = sha1(uniqid('Test content 1'));
        $content2 = sha1(uniqid('Test content 2'));

        $this->writeExpiredFile($path, $content1);
        static::assertFalse($this->adapter->fileExists($path));

        $this->adapter->write($path, $content2, $this->emptyConfig);
        static::assertTrue($this->adapter->fileExists($path));
        static::assertSame($content2, $this->adapter->read($path));

        // No expiry on the new file
        $actual = $this->adapter->expiry($path)->expiry();
        static::assertNull($actual);

        // The expired file is replaced
        static::assertRowCount(1, 'flysystem_path');
        static::assertSame(0, $this->adapter->deleteExpired());
    }

    public function testUpdateWithExpiryAddToFuture(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $this->emptyConfig);

        $expiry = new \DateTimeImmutable('+2 day');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $this->adapter->write($path, $content, $config);

        $actual = $this->adapter->expiry($path)->expiry();

        static::assertSame($expiry->format('Y-m-d H:i:s'), $actual->format('Y-m-d H:i:s'));
    }

    public function testUpdateWithExpiryAddToPast(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $this->adapter->write($path, $content, $this->emptyConfig);

        $expiry = new \DateTimeImmutable('-2 day');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $this->adapter->write($path, $content, $config);

        static::assertFalse($this->adapter->fileExists($path));
    }

    public function testUpdateWithExpiryChangeToFuture(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $expiry1 = new \DateTimeImmutable('+2 day');
        $config1 = new Config([
            'expiry' => $expiry1,
        ]);

        $this->adapter->write($path, $content, $config1);

        $expiry2 = new \DateTimeImmutable('+5 day');
        $config2 = new Config([
            'expiry' => $expiry2,
        ]);

        $this->adapter->write($path, $content, $config2);
        $actual = $this->adapter->expiry($path)->expiry();

        static::assertSame($expiry2->format('Y-m-d H:i:s'), $actual->format('Y-m-d H:i:s'));
    }

    public function testUpdateWithExpiryChangeToPast(): void
    {
        $path = '/path/to/file.txt';
        $content = sha1(uniqid('Test content'));

        $expiry1 = new \DateTimeImmutable('+2 day');
        $config1 = new Config([
            'expiry' => $expiry1,
        ]);

        $this->adapter->write($path, $content, $config1);

        $expiry2 = new \DateTimeImmutable('-2 day');
        $config2 = new Config([
            'expiry' => $expiry2,
        ]);

        $this->adapter->write($path, $content, $config2);

        // Writing with an expiry in the past removes the existing file
        static::assertFalse($this->adapter->fileExists($path));
        static::assertRowCount(0, 'flysystem_path');
        static::assertRowCount(0, 'flysystem_chunk');
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

        $this->adapter->write($path, $content2, $this->emptyConfig);

        $actual = $this->adapter->extra($path)->extraMetadata();

        static::assertSame($additional, $actual);
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

        $this->adapter->write($path, $content, $config);

        $actual = $this->adapter->extra($path)->extraMetadata();

        static::assertSame($additional, $actual);
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

        $this->adapter->write($path, $content, $config2);

        $actual = $this->adapter->extra($path)->extraMetadata();

        static::assertSame($additional2, $actual);
    }

    public function testCopyingFile(): void
    {
        $path1 = '/first.txt';
        $path2 = '/second.txt';
        $this->adapter->write($path1, file_get_contents(static::$tempFiles['10B']), $this->emptyConfig);
        $this->adapter->copy($path1, $path2, $this->emptyConfig);

        $actual1 = $this->adapter->read($path1);
        $actual2 = $this->adapter->read($path2);

        static::assertSame($actual1, $actual2);
    }

    public function testCompressionIsSetOnThePath(): void
    {
        $filename = static::$tempFiles['10B'];
        $file = $this->createResource($filename);
        $path = '/path/to/file.txt';
        $this->adapter->writeStream($path, $file, new Config([
            'enable_compression' => true,
        ]));

        $sql = 'SELECT is_compressed FROM flysystem_path WHERE path = ?';
        $stmt = static::getTestDbAdapter()->prepare($sql);
        $stmt->execute([$path]);
        $actual = $stmt->fetchColumn();

        static::assertSame(1, $actual);
    }

    public function testCopyingPathMakesAccurateCopy(): void
    {
        $origPath = '/path/to/file.txt';
        $content = file_get_contents(static::$tempFiles['10B']);
        $this->adapter->write($origPath, $content, $this->emptyConfig);

        $copyPath = '/path/to/copy.txt';
        $this->adapter->copy($origPath, $copyPath, $this->emptyConfig);

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
        $this->adapter->copy($origPath, $copyPath, $this->emptyConfig);

        $select = 'SELECT chunk_no, content FROM flysystem_chunk JOIN flysystem_path USING (path_id) WHERE path = "%s"';
        $origDataSet = static::getTestDbAdapter()->query(sprintf($select, $origPath))->fetchAll();
        $copyDataSet = static::getTestDbAdapter()->query(sprintf($select, $copyPath))->fetchAll();

        static::assertSame($origDataSet, $copyDataSet);
    }

    public function testCopyOverExpiredPath(): void
    {
        $this->writeExpiredFile('/destination.txt', 'old content');
        $this->adapter->write('/source.txt', 'new content', $this->emptyConfig);

        $this->adapter->copy('/source.txt', '/destination.txt', $this->emptyConfig);

        static::assertSame('new content', $this->adapter->read('/destination.txt'));
        static::assertRowCount(2, 'flysystem_path');
        static::assertRowCount(2, 'flysystem_chunk');
    }

    public function testRenameFile(): void
    {
        $path1 = '/first.txt';
        $path2 = '/second.txt';

        $testContent = file_get_contents(static::$tempFiles['10B']);

        $this->adapter->write($path1, $testContent, $this->emptyConfig);

        // Check successfully written, first
        $actualOld = $this->adapter->read($path1);
        static::assertSame($testContent, $actualOld);

        // Perform rename; check original path is now missing and new path is present
        $this->adapter->move($path1, $path2, $this->emptyConfig);

        try {
            $this->adapter->read($path1);
            static::fail('Read should throw exception for old path');
        } catch (UnableToReadFile) {
            // Correctly threw the exception for the old path
        }
        $actual = $this->adapter->read($path2);

        static::assertSame($testContent, $actual);
    }

    public function testMoveExpiredFile(): void
    {
        $this->writeExpiredFile('/source.txt', 'content');

        try {
            $this->adapter->move('/source.txt', '/destination.txt', $this->emptyConfig);
            static::fail('Moving an expired file should throw an exception');
        } catch (UnableToMoveFile) {
            // Correctly threw the exception
        }

        // The expired file has not been moved
        static::assertFalse($this->adapter->fileExists('/destination.txt'));
        static::assertSame(1, $this->adapter->deleteExpired());
        static::assertRowCount(0, 'flysystem_path');
    }

    public function testMoveNonExistentFile(): void
    {
        $this->expectException(UnableToMoveFile::class);
        $this->adapter->move('/does/not/exist.txt', '/destination.txt', $this->emptyConfig);
    }

    public function testMoveToSamePath(): void
    {
        $this->adapter->write('/source.txt', 'content', $this->emptyConfig);

        $this->adapter->move('/source.txt', '/source.txt', $this->emptyConfig);

        static::assertSame('content', $this->adapter->read('/source.txt'));
    }

    public function testMoveNonExistentFileToSamePath(): void
    {
        $this->expectException(UnableToMoveFile::class);
        $this->adapter->move('/does/not/exist.txt', '/does/not/exist.txt', $this->emptyConfig);
    }

    public function testMoveOverExpiredPath(): void
    {
        $this->writeExpiredFile('/destination.txt', 'old content');
        $this->adapter->write('/source.txt', 'new content', $this->emptyConfig);

        $this->adapter->move('/source.txt', '/destination.txt', $this->emptyConfig);

        static::assertSame('new content', $this->adapter->read('/destination.txt'));
        static::assertRowCount(1, 'flysystem_path');
        static::assertRowCount(1, 'flysystem_chunk');
    }

    #[DataProvider('pathsDataProvider')]
    public function testAddingPaths(array $paths, int $expectedRows): void
    {
        foreach ($paths as $path) {
            if ($path['type'] === 'dir') {
                $this->adapter->createDirectory($path['name'], $this->emptyConfig);
            } else {
                $this->adapter->write($path['name'], '', $this->emptyConfig);
            }
        }
        static::assertRowCount($expectedRows, 'flysystem_path');
    }

    #[DataProvider('pathsDataProvider')]
    public function testListContentsMeetsExpectedOutput(array $paths, int $expectedRows): void
    {
        foreach ($paths as $path) {
            if ($path['type'] === 'dir') {
                if ($path['name'] === '/test') {
                    // The test path will not be included in the listing output
                    $expectedRows--;
                }
                $this->adapter->createDirectory($path['name'], $this->emptyConfig);
            } else {
                $this->adapter->write($path['name'], '', $this->emptyConfig);
            }
        }

        // extraneous data to throw off the listings
        $this->adapter->createDirectory('/not/this', $this->emptyConfig);
        $this->adapter->createDirectory('/not/that', $this->emptyConfig);
        $this->adapter->write('/not/this/too.txt', '', $this->emptyConfig);

        $listing = $this->adapter->listContents('/test', true);

        static::assertCount($expectedRows, iterator_to_array($listing));
    }

    public static function pathsDataProvider(): array
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

    public function testListContentsExcludesExpired(): void
    {
        $configFuture = new Config([
            'expiry' => new \DateTimeImmutable('+2 day'),
        ]);

        $this->writeExpiredFile('/path/expiry-past.txt', 'Test content');
        $this->adapter->write('/path/expiry-future.txt', 'Test content', $configFuture);
        $this->adapter->write('/path/expiry-none.txt', 'Test content', $this->emptyConfig);

        $expected = [
            '/path/expiry-future.txt',
            '/path/expiry-none.txt',
        ];

        $actual = array_map(
            fn(StorageAttributes $item): string => $item->path(),
            iterator_to_array($this->adapter->listContents('/path', true), false),
        );
        sort($actual);
        static::assertSame($expected, $actual);
    }

    public function testListContentsExcludesSimilarPaths(): void
    {
        $this->adapter->write('/path/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/path/sub/file.txt', 'content', $this->emptyConfig);
        // Sorts between '/path/' and '/path0'
        $this->adapter->write('/path/~file.txt', 'content', $this->emptyConfig);
        // Sorts before '/path/'
        $this->adapter->write('/path-other/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/path.txt', 'content', $this->emptyConfig);
        // Sorts after '/path0'
        $this->adapter->write('/path0/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/pathx/file.txt', 'content', $this->emptyConfig);
        // Not treated as wildcards
        $this->adapter->write('/p_th/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/p%th/file.txt', 'content', $this->emptyConfig);

        static::assertSame([
            '/path/file.txt' => 'file',
            '/path/sub' => 'dir',
            '/path/sub/file.txt' => 'file',
            '/path/~file.txt' => 'file',
        ], $this->listPathTypes('/path', true));

        static::assertSame([
            '/p_th/file.txt' => 'file',
        ], $this->listPathTypes('/p_th'));
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

        $this->adapter->createDirectory($path, $config);

        // Directory metadata is only available via listContents() on the parent
        $listing = $this->adapter->listContents('/new');
        $listing = iterator_to_array($listing);
        /** @var DirectoryAttributes $actual */
        $actual = $listing[0];

        static::assertSame($additional, $actual->extraMetadata());
    }

    public function testListContentsShallowIncludesImpliedDirectories(): void
    {
        $this->adapter->write('/list/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/list/implied/deeper/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/list/implied/other.txt', 'content', $this->emptyConfig);
        $this->adapter->createDirectory('/list/real', $this->emptyConfig);
        $this->adapter->write('/list/real/file.txt', 'content', $this->emptyConfig);

        static::assertSame([
            '/list/file.txt' => 'file',
            '/list/implied' => 'dir',
            '/list/real' => 'dir',
        ], $this->listPathTypes('/list'));
    }

    public function testListContentsDeepIncludesImpliedDirectories(): void
    {
        $this->adapter->write('/list/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/list/implied/deeper/file.txt', 'content', $this->emptyConfig);
        $this->adapter->createDirectory('/list/real', $this->emptyConfig);
        $this->adapter->write('/list/real/file.txt', 'content', $this->emptyConfig);

        static::assertSame([
            '/list/file.txt' => 'file',
            '/list/implied' => 'dir',
            '/list/implied/deeper' => 'dir',
            '/list/implied/deeper/file.txt' => 'file',
            '/list/real' => 'dir',
            '/list/real/file.txt' => 'file',
        ], $this->listPathTypes('/list', true));
    }

    public function testListContentsRoot(): void
    {
        $this->adapter->write('/root.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/list/file.txt', 'content', $this->emptyConfig);

        // A leading separator is not listed as a directory
        static::assertSame([
            '/list' => 'dir',
            '/root.txt' => 'file',
        ], $this->listPathTypes(''));
    }

    public function testListContentsUsesDirectoryEntryMetadata(): void
    {
        $meta = [
            'owner' => 'example',
        ];
        // The directory entry is created after its child, to check the listing does not depend on the order
        $this->adapter->write('/list/real/file.txt', 'content', $this->emptyConfig);
        $this->adapter->createDirectory('/list/real', new Config([
            'meta' => $meta,
        ]));

        $listing = iterator_to_array($this->adapter->listContents('/list'), false);
        static::assertCount(1, $listing);
        static::assertInstanceOf(DirectoryAttributes::class, $listing[0]);
        static::assertSame($meta, $listing[0]->extraMetadata());
    }

    public function testListContentsEscapesWildcards(): void
    {
        $this->adapter->write('/a_b/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/axb/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/a%b/file.txt', 'content', $this->emptyConfig);

        static::assertSame([
            '/a_b/file.txt' => 'file',
        ], $this->listPathTypes('/a_b'));
        static::assertSame([
            '/a%b/file.txt' => 'file',
        ], $this->listPathTypes('/a%b'));
    }

    public function testCreateDirectoryOverExpiredPath(): void
    {
        $this->writeExpiredFile('/path', 'old content');

        $this->adapter->createDirectory('/path', $this->emptyConfig);

        static::assertSame([
            '/path' => 'dir',
        ], $this->listPathTypes(''));
        static::assertRowCount(1, 'flysystem_path');
        static::assertRowCount(0, 'flysystem_chunk');
    }

    public function testCreateDirectoryIsIdempotent(): void
    {
        $this->adapter->createDirectory('/test', $this->emptyConfig);
        $this->adapter->createDirectory('/test', $this->emptyConfig);

        static::assertRowCount(1, 'flysystem_path');
    }

    public function testCreateDirectoryWhereFileExists(): void
    {
        $this->adapter->write('/test', 'content', $this->emptyConfig);

        $this->expectException(UnableToCreateDirectory::class);
        $this->adapter->createDirectory('/test', $this->emptyConfig);
    }

    public function testDeletingImpliedDirectory(): void
    {
        $this->adapter->write('/test/file.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/test/deeper/file.txt', 'content', $this->emptyConfig);
        $this->writeExpiredFile('/test/expired.txt', 'content');
        $this->adapter->write('/test-sibling.txt', 'content', $this->emptyConfig);
        $this->adapter->write('/tests/file.txt', 'content', $this->emptyConfig);

        $this->adapter->deleteDirectory('/test');

        // Only the siblings remain, and the expired file has also been deleted
        static::assertSame([
            '/test-sibling.txt' => 'file',
            '/tests' => 'dir',
        ], $this->listPathTypes(''));
        static::assertRowCount(2, 'flysystem_path');
        static::assertRowCount(2, 'flysystem_chunk');
    }

    public function testDeletingDirectoryDoesNotDeleteFileAtPath(): void
    {
        $this->adapter->write('/test', 'content', $this->emptyConfig);

        $this->adapter->deleteDirectory('/test');

        static::assertTrue($this->adapter->fileExists('/test'));
    }

    public function testDeletingRootDirectoryIsNotAllowed(): void
    {
        $this->expectException(UnableToDeleteDirectory::class);
        $this->adapter->deleteDirectory('');
    }

    public function testDeletingDirectoryClearsAllFiles(): void
    {
        $this->adapter->createDirectory('/test', $this->emptyConfig);
        $this->adapter->write('/test/file.txt', '', $this->emptyConfig);

        static::assertRowCount(2, 'flysystem_path');
        $this->adapter->deleteDirectory('/test');
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
        $this->expectException(UnableToReadFile::class);
        $this->adapter->read('/path/does/not/exist.txt');
    }

    public function testReadingStreamForNonExistentPath(): void
    {
        $this->expectException(UnableToReadFile::class);
        $this->adapter->readStream('/path/does/not/exist.txt');
    }

    public function testHasForNonExistentPath(): void
    {
        static::assertFalse($this->adapter->fileExists('/path/does/not/exist.txt'));
    }

    public function testHasForExistingPath(): void
    {
        $path = '/this/path/does/exist.txt';
        $this->adapter->write($path, 'some text', $this->emptyConfig);
        static::assertTrue($this->adapter->fileExists($path));
    }

    public function testFileExistsWithExpiryPast(): void
    {
        $path = '/path/to/file.txt';
        $this->writeExpiredFile($path, 'Test content');

        static::assertFalse($this->adapter->fileExists($path));

        $this->expectException(UnableToReadFile::class);
        $this->adapter->read($path);
    }

    public function testCopyingNonExistentPath(): void
    {
        $this->expectException(UnableToCopyFile::class);
        $this->adapter->copy('/this/does/not/exist.txt', '/my/new/path.txt', $this->emptyConfig);
    }

    public function testSettingVisibility(): void
    {
        $path = '/test.txt';
        $config = new Config([
            'visibility' => Visibility::PUBLIC,
        ]);
        $this->adapter->write($path, 'Some Content', $config);

        $this->adapter->setVisibility($path, Visibility::PRIVATE);

        $actual = $this->adapter->visibility($path);
        static::assertSame(Visibility::PRIVATE, $actual->visibility());
    }

    public function testCoreMetadataGetters(): void
    {
        $path = '/path/to/file.txt';
        $contents = file_get_contents(static::$tempFiles['10K']);
        $timestamp = time();

        $this->adapter->write($path, $contents, $this->emptyConfig);

        static::assertSame(strlen($contents), $this->adapter->fileSize($path)->fileSize());

        static::assertSame('text/plain', $this->adapter->mimeType($path)->mimeType());

        static::assertSame($timestamp, $this->adapter->lastModified($path)->lastModified());

        static::assertSame(Visibility::PUBLIC, $this->adapter->visibility($path)->visibility());
    }

    public function testDeleteExpired(): void
    {
        $content = sha1(uniqid('Test content'));

        // File with no expiry
        $pathNone = '/path/with/expiry-none.txt';
        $this->adapter->write($pathNone, $content, $this->emptyConfig);

        // File with past expiry
        $pathPast = '/path/with/expiry-past.txt';
        $this->writeExpiredFile($pathPast, $content);

        // File with future expiry
        $pathFuture = '/path/with/expiry-future.txt';
        $expiryFuture = new \DateTimeImmutable('+2 day');
        $configFuture = new Config([
            'expiry' => $expiryFuture,
        ]);
        $this->adapter->write($pathFuture, $content, $configFuture);

        // The expired file is excluded from the listing, but still exists in the DB
        $listBefore = $this->adapter->listContents('/path/with');
        static::assertCount(2, iterator_to_array($listBefore));
        static::assertRowCount(3, 'flysystem_path');
        // Each file is smaller than the chunk size, so has a single chunk
        static::assertRowCount(3, 'flysystem_chunk');

        static::assertSame(1, $this->adapter->deleteExpired());

        $listAfter = $this->adapter->listContents('/path/with');
        static::assertCount(2, iterator_to_array($listAfter));
        static::assertRowCount(2, 'flysystem_path');
        static::assertRowCount(2, 'flysystem_chunk', 'Chunks for the expired file should be deleted');

        // Check it's the expired one that has gone
        $this->expectException(UnableToReadFile::class);
        $this->adapter->read($pathPast);
    }

    public function testCopyRetainsExpiryAndAdditional(): void
    {
        $expiry = new \DateTimeImmutable('+2 day');
        $meta = [
            'owner' => 'example',
        ];
        $this->adapter->write('/source.txt', 'content', new Config([
            'expiry' => $expiry,
            'meta' => $meta,
        ]));

        $this->adapter->copy('/source.txt', '/destination.txt', $this->emptyConfig);

        $actual = $this->adapter->expiry('/destination.txt');
        static::assertSame($expiry->format('Y-m-d H:i:s'), $actual->expiry()->format('Y-m-d H:i:s'));
        static::assertSame($meta, $actual->extraMetadata());
        static::assertSame('content', $this->adapter->read('/destination.txt'));
    }

    public function testCopyReplacesExistingFile(): void
    {
        $this->adapter->write('/source.txt', 'new content', $this->emptyConfig);
        $this->adapter->write('/destination.txt', 'old content', $this->emptyConfig);

        $this->adapter->copy('/source.txt', '/destination.txt', $this->emptyConfig);

        static::assertSame('new content', $this->adapter->read('/destination.txt'));
        static::assertRowCount(2, 'flysystem_path');
        static::assertRowCount(2, 'flysystem_chunk');
    }

    public function testCopyWhereDirectoryExists(): void
    {
        $this->adapter->write('/source.txt', 'content', $this->emptyConfig);
        $this->adapter->createDirectory('/destination', $this->emptyConfig);

        $this->expectException(UnableToCopyFile::class);
        $this->adapter->copy('/source.txt', '/destination', $this->emptyConfig);
    }

    public function testSetVisibilityWhenPathDoesNotExist(): void
    {
        $this->expectException(UnableToSetVisibility::class);
        $this->adapter->setVisibility('/does/not/exist.txt', Visibility::PRIVATE);
    }

    public function testMimeTypeUnknown(): void
    {
        // A stream with an unknown extension cannot be detected
        $this->adapter->writeStream('/file.unknown', $this->createResource(static::$tempFiles['10B']), $this->emptyConfig);

        $this->expectException(UnableToRetrieveMetadata::class);
        $this->adapter->mimeType('/file.unknown');
    }

    /**
     * @return array<string, string> Path => type
     */
    private function listPathTypes(string $path, bool $deep = false): array
    {
        $actual = [];
        foreach ($this->adapter->listContents($path, $deep) as $item) {
            static::assertArrayNotHasKey($item->path(), $actual, 'Duplicate listing for ' . $item->path());
            $actual[$item->path()] = $item->type();
        }
        ksort($actual);
        return $actual;
    }

    /**
     * The adapter does not store a file which is written with an expiry in the past,
     * so write with a future expiry and then move it to the past directly in the DB.
     */
    private function writeExpiredFile(string $path, string $content): void
    {
        $this->adapter->write($path, $content, new Config([
            'expiry' => new \DateTimeImmutable('+2 day'),
        ]));

        $stmt = static::getTestDbAdapter()->prepare('UPDATE flysystem_path SET expiry = :expiry WHERE path = :path');
        $stmt->execute([
            'expiry' => (new \DateTimeImmutable('-2 day'))->format('Y-m-d H:i:s'),
            'path' => $path,
        ]);
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
