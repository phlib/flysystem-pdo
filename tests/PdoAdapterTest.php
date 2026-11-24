<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckFileExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\Visibility;
use Phlib\Flysystem\Pdo\FileAttributes;
use Phlib\Flysystem\Pdo\PdoAdapter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests using a stub DB connection.
 *
 * Each prepared statement is given the rows of the first registered response whose SQL fragment it contains, and
 * every execution is recorded. Tests assert the outcome, and the SQL conditions and parameters which matter for the
 * behaviour being tested (e.g. excluding expired files), rather than every query and the order they're run in.
 * How the data is actually stored is verified by the integration tests.
 */
class PdoAdapterTest extends TestCase
{
    /**
     * SQL fragments to identify each type of query, for registering responses and finding executions
     */
    private const SQL_LOOKUP = 'SELECT * FROM flysystem_path WHERE path = :path';

    private const SQL_RAW_LOOKUP = 'AS is_expired';

    private const SQL_EXISTS = 'SELECT 1 FROM flysystem_path';

    private const SQL_LIST = 'ORDER BY path';

    private const SQL_READ_CHUNKS = 'SELECT content FROM flysystem_chunk';

    private const SQL_FIND_EXPIRED = 'SELECT path_id FROM flysystem_path WHERE expiry <= :now';

    private const SQL_INSERT_PATH = 'INSERT INTO flysystem_path';

    private const SQL_INSERT_CHUNK = 'INSERT INTO flysystem_chunk';

    private const SQL_UPDATE_PATH = 'UPDATE flysystem_path';

    private const SQL_MOVE = 'SET path = :destination';

    private const SQL_DELETE_PATH = 'DELETE FROM flysystem_path';

    private const SQL_DELETE_CHUNKS = 'DELETE FROM flysystem_chunk';

    private const NOT_EXPIRED_CONDITION = '(expiry IS NULL OR expiry > :now)';

    private const PATH = '/path/to/file.txt';

    private const INSERT_ID = 1000;

    private PdoAdapter $adapter;

    private \PDO&MockObject $pdo;

    /**
     * @var list<array{match: string, rows: list<array>, fail: bool, rowCount: int}>
     */
    private array $responses = [];

    /**
     * @var list<array{sql: string, params: array}>
     */
    private array $executed = [];

    private int $lastInsertId = self::INSERT_ID;

    private ?string $driverName = null;

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo = $this->createMock(\PDO::class);
        $this->pdo->method('getAttribute')
            ->willReturnCallback(fn(int $attribute): mixed => match ($attribute) {
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DRIVER_NAME => $this->driverName,
                default => null,
            });
        $this->pdo->method('prepare')
            ->willReturnCallback($this->prepare(...));
        $this->pdo->method('lastInsertId')
            ->willReturnCallback(fn(): string => (string) ++$this->lastInsertId);

        $this->adapter = new PdoAdapter($this->pdo);
    }

    public function testImplementsAdapterInterface(): void
    {
        static::assertInstanceOf(FilesystemAdapter::class, $this->adapter);
    }

    public static function dataRequiresExceptionErrorMode(): array
    {
        return [
            'silent' => [\PDO::ERRMODE_SILENT],
            'warning' => [\PDO::ERRMODE_WARNING],
        ];
    }

    #[DataProvider('dataRequiresExceptionErrorMode')]
    public function testRequiresExceptionErrorMode(int $errorMode): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('getAttribute')
            ->with(\PDO::ATTR_ERRMODE)
            ->willReturn($errorMode);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('ERRMODE_EXCEPTION');

        new PdoAdapter($pdo);
    }

    public function testMysqlBufferingDisabledByDefault(): void
    {
        $pdo = $this->createExceptionModePdo();
        $pdo->expects(static::once())
            ->method('setAttribute')
            ->with(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

        new PdoAdapter($pdo);
    }

    public function testMysqlBufferingDisabledByDefaultWithOtherConfig(): void
    {
        $pdo = $this->createExceptionModePdo();
        $pdo->expects(static::once())
            ->method('setAttribute')
            ->with(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

        new PdoAdapter($pdo, new Config([
            'table_prefix' => 'myprefix',
        ]));
    }

    public function testMysqlBufferingNotDisabled(): void
    {
        $pdo = $this->createExceptionModePdo();
        $pdo->expects(static::never())
            ->method('setAttribute');

        new PdoAdapter($pdo, new Config([
            'disable_mysql_buffering' => false,
        ]));
    }

    public static function dataTablePrefix(): array
    {
        return [
            'default' => [[], 'flysystem'],
            'configured' => [[
                'table_prefix' => 'myprefix',
            ], 'myprefix'],
            'blank' => [[
                'table_prefix' => ' ',
            ], 'flysystem'],
        ];
    }

    #[DataProvider('dataTablePrefix')]
    public function testTablePrefix(array $config, string $expectedPrefix): void
    {
        $adapter = new PdoAdapter($this->pdo, new Config($config));

        $adapter->fileExists(self::PATH);
        $adapter->deleteDirectory('/path');

        static::assertNotEmpty($this->executed);
        foreach ($this->executed as $execution) {
            static::assertMatchesRegularExpression("/ {$expectedPrefix}_(path|chunk)\\b/", $execution['sql']);
        }
    }

    /**
     * Every operation which looks up a path must exclude expired files in the query, so with no row returned the
     * operation should behave as though the path does not exist
     */
    public static function dataLookupExcludesExpired(): array
    {
        $config = new Config();
        return [
            'fileExists' => [
                fn(PdoAdapter $adapter) => $adapter->fileExists(self::PATH),
                null,
            ],
            'read' => [
                fn(PdoAdapter $adapter) => $adapter->read(self::PATH),
                UnableToReadFile::class,
            ],
            'readStream' => [
                fn(PdoAdapter $adapter) => $adapter->readStream(self::PATH),
                UnableToReadFile::class,
            ],
            'fileSize' => [
                fn(PdoAdapter $adapter) => $adapter->fileSize(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'mimeType' => [
                fn(PdoAdapter $adapter) => $adapter->mimeType(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'lastModified' => [
                fn(PdoAdapter $adapter) => $adapter->lastModified(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'visibility' => [
                fn(PdoAdapter $adapter) => $adapter->visibility(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'expiry' => [
                fn(PdoAdapter $adapter) => $adapter->expiry(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'extra' => [
                fn(PdoAdapter $adapter) => $adapter->extra(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'delete' => [
                fn(PdoAdapter $adapter) => $adapter->delete(self::PATH),
                null,
            ],
            'copy' => [
                fn(PdoAdapter $adapter) => $adapter->copy(self::PATH, '/destination.txt', $config),
                UnableToCopyFile::class,
            ],
        ];
    }

    #[DataProvider('dataLookupExcludesExpired')]
    public function testLookupExcludesExpired(\Closure $operation, ?string $expectedException): void
    {
        try {
            $operation($this->adapter);
            if ($expectedException !== null) {
                static::fail("Expected {$expectedException} to be thrown");
            }
        } catch (\Exception $e) {
            if ($expectedException === null || !$e instanceof $expectedException) {
                throw $e;
            }
        }

        // The first query looks up the path, excluding expired files
        static::assertNotEmpty($this->executed);
        $lookup = $this->executed[0];
        static::assertStringContainsString('WHERE path = :path', $lookup['sql']);
        static::assertSame(self::PATH, $lookup['params']['path']);
        static::assertExcludesExpired($lookup);

        // Nothing is changed
        $this->assertNotExecuted(self::SQL_INSERT_PATH);
        $this->assertNotExecuted(self::SQL_UPDATE_PATH);
        $this->assertNotExecuted(self::SQL_DELETE_PATH);
    }

    public function testFileExists(): void
    {
        $this->respond(self::SQL_EXISTS, [[
            '1' => 1,
        ]]);

        static::assertTrue($this->adapter->fileExists(self::PATH));

        $exists = $this->executedOnce(self::SQL_EXISTS);
        static::assertExcludesExpired($exists);
        static::assertParams([
            'path' => self::PATH,
            'type' => 'file',
        ], $exists['params'], ['now']);
    }

    /**
     * A DB error in any operation is thrown as the Flysystem exception for that operation
     */
    public static function dataDbFailure(): array
    {
        $config = new Config();
        return [
            'fileExists' => [
                fn(PdoAdapter $adapter) => $adapter->fileExists(self::PATH),
                UnableToCheckFileExistence::class,
            ],
            'write' => [
                fn(PdoAdapter $adapter) => $adapter->write(self::PATH, 'Content', $config),
                UnableToWriteFile::class,
            ],
            'writeStream' => [
                fn(PdoAdapter $adapter) => $adapter->writeStream(self::PATH, fopen('data://text/plain,Content', 'rb'), $config),
                UnableToWriteFile::class,
            ],
            'read' => [
                fn(PdoAdapter $adapter) => $adapter->read(self::PATH),
                UnableToReadFile::class,
            ],
            'readStream' => [
                fn(PdoAdapter $adapter) => $adapter->readStream(self::PATH),
                UnableToReadFile::class,
            ],
            'move' => [
                fn(PdoAdapter $adapter) => $adapter->move(self::PATH, '/destination.txt', $config),
                UnableToMoveFile::class,
            ],
            'copy' => [
                fn(PdoAdapter $adapter) => $adapter->copy(self::PATH, '/destination.txt', $config),
                UnableToCopyFile::class,
            ],
            'delete' => [
                fn(PdoAdapter $adapter) => $adapter->delete(self::PATH),
                UnableToDeleteFile::class,
            ],
            'deleteDirectory' => [
                fn(PdoAdapter $adapter) => $adapter->deleteDirectory('/path/to'),
                UnableToDeleteDirectory::class,
            ],
            'createDirectory' => [
                fn(PdoAdapter $adapter) => $adapter->createDirectory('/path/to', $config),
                UnableToCreateDirectory::class,
            ],
            'setVisibility' => [
                fn(PdoAdapter $adapter) => $adapter->setVisibility(self::PATH, Visibility::PRIVATE),
                UnableToSetVisibility::class,
            ],
            'fileSize' => [
                fn(PdoAdapter $adapter) => $adapter->fileSize(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'mimeType' => [
                fn(PdoAdapter $adapter) => $adapter->mimeType(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'lastModified' => [
                fn(PdoAdapter $adapter) => $adapter->lastModified(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'visibility' => [
                fn(PdoAdapter $adapter) => $adapter->visibility(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'expiry' => [
                fn(PdoAdapter $adapter) => $adapter->expiry(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
            'extra' => [
                fn(PdoAdapter $adapter) => $adapter->extra(self::PATH),
                UnableToRetrieveMetadata::class,
            ],
        ];
    }

    #[DataProvider('dataDbFailure')]
    public function testDbFailure(\Closure $operation, string $expectedException): void
    {
        // Every statement fails
        $this->respond('', fail: true);

        $this->assertDbFailure($expectedException, fn() => $operation($this->adapter));
    }

    public static function dataRead(): array
    {
        $content = str_repeat('Test content ', 100);
        return [
            'uncompressed' => [false, [substr($content, 0, 600), substr($content, 600)], $content],
            'compressed' => [true, str_split(gzdeflate($content), 20), $content],
        ];
    }

    #[DataProvider('dataRead')]
    public function testRead(bool $isCompressed, array $chunks, string $expected): void
    {
        $this->givenFile([
            'is_compressed' => (int) $isCompressed,
        ]);
        $this->respond(self::SQL_READ_CHUNKS, array_map(fn(string $chunk): array => [
            'content' => $chunk,
        ], $chunks));

        static::assertSame($expected, $this->adapter->read(self::PATH));

        $stream = $this->adapter->readStream(self::PATH);
        static::assertSame($expected, stream_get_contents($stream));
        fclose($stream);

        // Chunks are read in order for the file
        $readChunks = $this->executed(self::SQL_READ_CHUNKS);
        foreach ($readChunks as $execution) {
            static::assertStringContainsString('ORDER BY chunk_no', $execution['sql']);
            static::assertSame([
                'path_id' => 123,
            ], $execution['params']);
        }
    }

    public function testWriteNewFile(): void
    {
        $content = 'Test Content';

        $this->adapter->write(self::PATH, $content, new Config());

        $insert = $this->executedOnce(self::SQL_INSERT_PATH);
        static::assertParams([
            'type' => 'file',
            'path' => self::PATH,
            'visibility' => Visibility::PUBLIC,
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'is_compressed' => 1,
        ], $insert['params']);

        // Compressed by default
        static::assertSame($content, gzinflate($this->writtenContent(self::INSERT_ID + 1)));
        $this->assertNotExecuted(self::SQL_UPDATE_PATH);
    }

    public function testWriteStreamNewFile(): void
    {
        $content = 'Test Content';
        $stream = fopen('php://temp', 'w+b');
        fwrite($stream, $content);
        rewind($stream);

        $this->adapter->writeStream(self::PATH, $stream, new Config());
        fclose($stream);

        $insert = $this->executedOnce(self::SQL_INSERT_PATH);
        static::assertSame(strlen($content), $insert['params']['size']);
        static::assertSame($content, gzinflate($this->writtenContent(self::INSERT_ID + 1)));
    }

    public function testWriteWithoutCompression(): void
    {
        $content = 'Test Content';

        $this->adapter->write(self::PATH, $content, new Config([
            'enable_compression' => false,
        ]));

        static::assertSame(0, $this->executedOnce(self::SQL_INSERT_PATH)['params']['is_compressed']);
        static::assertSame($content, $this->writtenContent(self::INSERT_ID + 1));
    }

    public function testWriteWithConfig(): void
    {
        $expiry = new \DateTimeImmutable('+2 days');
        $meta = [
            'owner' => 'example',
        ];

        $this->adapter->write(self::PATH, 'Test Content', new Config([
            'visibility' => Visibility::PRIVATE,
            'expiry' => $expiry,
            'meta' => $meta,
        ]));

        $params = $this->executedOnce(self::SQL_INSERT_PATH)['params'];
        static::assertSame(Visibility::PRIVATE, $params['visibility']);
        static::assertSame($expiry->format('Y-m-d H:i:s'), $params['expiry']);
        static::assertSame(json_encode($meta), $params['meta']);
    }

    public static function dataWriteDetectsMimeType(): array
    {
        $gif = base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs=', true); // 1x1 transparent gif
        return [
            'by extension' => ['/path/to/file.json', '', 'application/json'],
            'by content' => ['/path/to/file', $gif, 'image/gif'],
            'unknown' => ['/path/to/file.unknown', '', 'text/plain'],
        ];
    }

    #[DataProvider('dataWriteDetectsMimeType')]
    public function testWriteDetectsMimeType(string $path, string $content, ?string $expected): void
    {
        $this->adapter->write($path, $content, new Config());

        static::assertSame($expected, $this->executedOnce(self::SQL_INSERT_PATH)['params']['mimetype']);
    }

    public function testWriteLooksUpPathIncludingExpired(): void
    {
        $this->adapter->write(self::PATH, 'Test Content', new Config());

        // The lookup must find an expired file, as its path has to be reused, and the expiry is checked in the query
        $lookup = $this->executedOnce(self::SQL_RAW_LOOKUP);
        static::assertStringContainsString('NOT ' . self::NOT_EXPIRED_CONDITION . ' AS is_expired', $lookup['sql']);
        static::assertStringNotContainsString('AND ' . self::NOT_EXPIRED_CONDITION, $lookup['sql']);
        static::assertSame(self::PATH, $lookup['params']['path']);
        static::assertNowParam($lookup['params']['now']);
    }

    public function testWriteUpdatesExistingFile(): void
    {
        $content = 'New Content';
        $this->respond(self::SQL_RAW_LOOKUP, [[
            'path_id' => 123,
            'type' => 'file',
            'is_compressed' => 0,
            'is_expired' => 0,
        ]]);

        // Compression is enabled by config, but the stored setting is used for an existing file
        $this->adapter->write(self::PATH, $content, new Config([
            'enable_compression' => true,
        ]));

        $this->assertNotExecuted(self::SQL_INSERT_PATH);
        $update = $this->executedOnce(self::SQL_UPDATE_PATH);
        static::assertStringContainsString('WHERE path_id = :path_id', $update['sql']);
        static::assertParams([
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'path_id' => 123,
        ], $update['params']);

        // The previous content is replaced
        static::assertSame([
            'path_id' => 123,
        ], $this->executedOnce(self::SQL_DELETE_CHUNKS)['params']);
        $this->assertExecutedBefore(self::SQL_DELETE_CHUNKS, self::SQL_INSERT_CHUNK);
        static::assertSame($content, $this->writtenContent(123));
    }

    public function testWriteUpdatesExistingFileWithConfig(): void
    {
        $expiry = new \DateTimeImmutable('+2 days');
        $meta = [
            'owner' => 'example',
        ];
        $this->givenRawFile();

        $this->adapter->write(self::PATH, 'New Content', new Config([
            'expiry' => $expiry,
            'meta' => $meta,
        ]));

        $params = $this->executedOnce(self::SQL_UPDATE_PATH)['params'];
        static::assertSame($expiry->format('Y-m-d H:i:s'), $params['expiry']);
        static::assertSame(json_encode($meta), $params['meta']);
    }

    public function testWriteReplacesExpiredFile(): void
    {
        $this->givenRawFile([
            'is_expired' => 1,
        ]);

        $this->adapter->write(self::PATH, 'New Content', new Config());

        // The expired file is deleted, then the new file inserted, rather than updating the expired file
        $this->assertNotExecuted(self::SQL_UPDATE_PATH);
        $this->assertDeleted(123);
        $this->assertExecutedBefore(self::SQL_DELETE_PATH, self::SQL_INSERT_PATH);
        static::assertSame(self::PATH, $this->executedOnce(self::SQL_INSERT_PATH)['params']['path']);
    }

    public function testWriteToDirectory(): void
    {
        $this->givenRawFile([
            'type' => 'dir',
        ]);

        try {
            $this->adapter->write(self::PATH, 'Content', new Config());
            static::fail('Expected exception to be thrown');
        } catch (UnableToWriteFile) {
        }

        $this->assertNotExecuted(self::SQL_INSERT_PATH);
        $this->assertNotExecuted(self::SQL_UPDATE_PATH);
    }

    public function testWriteWithExpiryInPast(): void
    {
        // An existing file is deleted, as writing it again would make it immediately expire
        $this->givenFile();

        $this->adapter->write(self::PATH, 'Content', new Config([
            'expiry' => new \DateTimeImmutable('-2 days'),
        ]));

        $this->assertNotExecuted(self::SQL_INSERT_PATH);
        $this->assertNotExecuted(self::SQL_UPDATE_PATH);
        $this->assertNotExecuted(self::SQL_INSERT_CHUNK);
        $this->assertDeleted(123);
    }

    public function testWriteWithDbFailureRemovesTempFile(): void
    {
        $tempDir = sys_get_temp_dir() . '/flysystempdo-test-' . bin2hex(random_bytes(4));
        mkdir($tempDir);
        $adapter = new PdoAdapter($this->pdo, new Config([
            'temp_dir' => $tempDir,
        ]));
        $this->respond(self::SQL_INSERT_PATH, fail: true);

        try {
            $adapter->write(self::PATH, 'Content', new Config());
            static::fail('Expected exception was not thrown');
        } catch (UnableToWriteFile) {
            static::assertSame([], array_diff(scandir($tempDir), ['.', '..']));
        } finally {
            array_map(unlink(...), glob($tempDir . '/*'));
            rmdir($tempDir);
        }
    }

    public function testUpdateWithDbFailure(): void
    {
        $this->givenRawFile();
        $this->respond(self::SQL_UPDATE_PATH, fail: true);

        $this->assertDbFailure(
            UnableToWriteFile::class,
            fn() => $this->adapter->write(self::PATH, 'Content', new Config()),
        );
    }

    public function testMove(): void
    {
        $this->respond(self::SQL_MOVE, rowCount: 1);

        $this->adapter->move(self::PATH, '/destination.txt', new Config());

        $move = $this->executedOnce(self::SQL_MOVE);
        static::assertExcludesExpired($move);
        static::assertParams([
            'destination' => '/destination.txt',
            'source' => self::PATH,
            'type' => 'file',
        ], $move['params'], ['now']);
    }

    public function testMoveNonExistentOrExpiredFile(): void
    {
        // No row is updated, as the file does not exist or has expired
        $this->respond(self::SQL_MOVE, rowCount: 0);

        $this->expectException(UnableToMoveFile::class);
        $this->adapter->move(self::PATH, '/destination.txt', new Config());
    }

    public function testMoveReplacesExpiredDestination(): void
    {
        $this->givenRawFile([
            'is_expired' => 1,
        ]);
        $this->respond(self::SQL_MOVE, rowCount: 1);

        $this->adapter->move('/source.txt', self::PATH, new Config());

        static::assertSame(self::PATH, $this->executedOnce(self::SQL_RAW_LOOKUP)['params']['path']);
        $this->assertDeleted(123);
        $this->assertExecutedBefore(self::SQL_DELETE_PATH, self::SQL_MOVE);
    }

    public function testMoveDoesNotReplaceLiveDestination(): void
    {
        $this->givenRawFile();
        $this->respond(self::SQL_MOVE, rowCount: 1);

        $this->adapter->move('/source.txt', self::PATH, new Config());

        $this->assertNotExecuted(self::SQL_DELETE_PATH);
    }

    public function testMoveToSamePath(): void
    {
        $this->respond(self::SQL_EXISTS, [[
            '1' => 1,
        ]]);

        $this->adapter->move(self::PATH, self::PATH, new Config());

        static::assertExcludesExpired($this->executedOnce(self::SQL_EXISTS));
        $this->assertNotExecuted(self::SQL_MOVE);
    }

    public function testMoveNonExistentFileToSamePath(): void
    {
        $this->expectException(UnableToMoveFile::class);
        $this->adapter->move(self::PATH, self::PATH, new Config());
    }

    public function testCopy(): void
    {
        $content = 'Test Content';
        $this->givenFile([
            'visibility' => Visibility::PRIVATE,
            'mimetype' => 'text/csv',
            'size' => strlen($content),
            'is_compressed' => 1,
        ]);
        $this->respond(self::SQL_READ_CHUNKS, [[
            'content' => gzdeflate($content),
        ]]);

        $this->adapter->copy(self::PATH, '/destination.txt', new Config());

        static::assertParams([
            'type' => 'file',
            'path' => '/destination.txt',
            'visibility' => Visibility::PRIVATE,
            'mimetype' => 'text/csv',
            'size' => strlen($content),
            'is_compressed' => 1,
        ], $this->executedOnce(self::SQL_INSERT_PATH)['params']);
        static::assertSame($content, gzinflate($this->writtenContent(self::INSERT_ID + 1)));
        $this->assertNotExecuted(self::SQL_DELETE_PATH);
    }

    public static function dataCopyReplacesDestination(): array
    {
        return [
            'expired file' => ['file', 1],
            'expired directory' => ['dir', 1],
        ];
    }

    #[DataProvider('dataCopyReplacesDestination')]
    public function testCopyReplacesDestination(string $type, int $isExpired): void
    {
        $this->givenFile();
        $this->givenRawFile([
            'path_id' => 456,
            'type' => $type,
            'is_expired' => $isExpired,
        ]);

        $this->adapter->copy(self::PATH, '/destination.txt', new Config());

        static::assertSame('/destination.txt', $this->executedOnce(self::SQL_RAW_LOOKUP)['params']['path']);
        $this->assertDeleted(456);
        $this->assertExecutedBefore(self::SQL_DELETE_PATH, self::SQL_INSERT_PATH);
    }

    public function testDelete(): void
    {
        $this->givenFile();

        $this->adapter->delete(self::PATH);

        $this->assertDeleted(123);
    }

    public function testDeleteDirectoryPath(): void
    {
        $this->givenFile([
            'type' => 'dir',
        ]);

        $this->adapter->delete(self::PATH);

        $this->assertNotExecuted(self::SQL_DELETE_PATH);
        $this->assertNotExecuted(self::SQL_DELETE_CHUNKS);
    }

    public static function dataDeleteDirectory(): array
    {
        return [
            'path' => ['/path/to'],
            'trailing separator' => ['/path/to/'],
        ];
    }

    #[DataProvider('dataDeleteDirectory')]
    public function testDeleteDirectory(string $path): void
    {
        // Registered before the directory entry, as the lookup has the same start
        $this->respond('WHERE path = :path OR', [
            $this->row('/path/to', [
                'path_id' => 123,
                'type' => 'dir',
            ]),
            $this->row('/path/to/file.txt', [
                'path_id' => 456,
            ]),
        ]);
        $this->givenFile([
            'path' => '/path/to',
            'type' => 'dir',
        ]);

        $this->adapter->deleteDirectory($path);

        // The directory and all descendants are found, including any which are expired
        $select = $this->executedOnce('WHERE path = :path OR');
        static::assertStringContainsString('(path >= :descendant_from AND path < :descendant_to)', $select['sql']);
        static::assertStringNotContainsString('expiry', $select['sql']);
        static::assertParams([
            'path' => '/path/to',
            'descendant_from' => '/path/to/',
            'descendant_to' => '/path/to0',
        ], $select['params']);

        // Each is deleted, along with the chunks for files
        $this->assertDeleted(456);
        static::assertCount(1, array_filter(
            $this->executed(self::SQL_DELETE_PATH),
            fn(array $execution): bool => $execution['params']['path_id'] === 123,
        ));
    }

    public function testCreateDirectory(): void
    {
        $meta = [
            'owner' => 'example',
        ];

        $this->adapter->createDirectory('/path/to', new Config([
            'meta' => $meta,
        ]));

        $insert = $this->executedOnce(self::SQL_INSERT_PATH);
        static::assertSame('dir', $insert['params']['type']);
        static::assertSame('/path/to', $insert['params']['path']);
        static::assertSame(json_encode($meta), $insert['params']['meta']);
        $this->assertNotExecuted(self::SQL_INSERT_CHUNK);
    }

    public function testCreateDirectoryWhichExists(): void
    {
        $this->givenFile([
            'type' => 'dir',
        ]);

        $this->adapter->createDirectory(self::PATH, new Config());

        static::assertExcludesExpired($this->executedOnce(self::SQL_LOOKUP));
        $this->assertNotExecuted(self::SQL_INSERT_PATH);
    }

    public function testCreateDirectoryWhereFileExists(): void
    {
        $this->givenFile();

        try {
            $this->adapter->createDirectory(self::PATH, new Config());
            static::fail('Expected exception to be thrown');
        } catch (UnableToCreateDirectory) {
        }

        $this->assertNotExecuted(self::SQL_INSERT_PATH);
    }

    public function testCreateDirectoryReplacesExpiredFile(): void
    {
        $this->givenRawFile([
            'is_expired' => 1,
        ]);

        $this->adapter->createDirectory(self::PATH, new Config());

        $this->assertDeleted(123);
        $this->assertExecutedBefore(self::SQL_DELETE_PATH, self::SQL_INSERT_PATH);
    }

    public function testSetVisibility(): void
    {
        $this->adapter->setVisibility(self::PATH, Visibility::PRIVATE);

        static::assertParams([
            'visibility' => Visibility::PRIVATE,
            'path' => self::PATH,
        ], $this->executedOnce(self::SQL_UPDATE_PATH)['params']);
    }

    public function testMetadata(): void
    {
        $expiry = '2030-01-02 03:04:05';
        $meta = [
            'owner' => 'example',
        ];
        $this->givenFile([
            'mimetype' => 'text/csv',
            'visibility' => Visibility::PRIVATE,
            'size' => 1234,
            'update_ts' => '2026-01-02 03:04:05',
            'expiry' => $expiry,
            'meta' => json_encode($meta),
        ]);

        static::assertSame(1234, $this->adapter->fileSize(self::PATH)->fileSize());
        static::assertSame('text/csv', $this->adapter->mimeType(self::PATH)->mimeType());
        static::assertSame(Visibility::PRIVATE, $this->adapter->visibility(self::PATH)->visibility());
        static::assertSame(strtotime('2026-01-02 03:04:05'), $this->adapter->lastModified(self::PATH)->lastModified());

        $attributes = $this->adapter->expiry(self::PATH);
        static::assertEquals(new \DateTimeImmutable($expiry), $attributes->expiry());
        static::assertSame($meta, $this->adapter->extra(self::PATH)->extraMetadata());
    }

    public function testLastModifiedSqliteIsUtc(): void
    {
        $this->driverName = 'sqlite';
        $this->givenFile([
            'update_ts' => '2026-01-02 03:04:05',
        ]);

        static::assertSame(
            strtotime('2026-01-02 03:04:05 +00:00'),
            $this->adapter->lastModified(self::PATH)->lastModified(),
        );
    }

    public function testMetadataForDirectory(): void
    {
        $this->givenFile([
            'type' => 'dir',
        ]);

        $this->expectException(UnableToRetrieveMetadata::class);
        $this->adapter->fileSize(self::PATH);
    }

    public function testListContentsExcludesExpired(): void
    {
        iterator_to_array($this->adapter->listContents('/path/'));

        $list = $this->executedOnce(self::SQL_LIST);
        static::assertExcludesExpired($list);
        static::assertStringContainsString('(path >= :descendant_from AND path < :descendant_to)', $list['sql']);
        static::assertParams([
            'descendant_from' => '/path/',
            'descendant_to' => '/path0',
        ], $list['params'], ['now']);
    }

    public function testListContentsRootExcludesExpired(): void
    {
        iterator_to_array($this->adapter->listContents(''));

        $list = $this->executedOnce(self::SQL_LIST);
        static::assertExcludesExpired($list);
        static::assertStringNotContainsString('descendant', $list['sql']);
        static::assertParams([], $list['params'], ['now']);
    }

    public function testListContentsShallow(): void
    {
        $this->respond(self::SQL_LIST, [
            $this->row('/list/file.txt'),
            // A directory implied by its children
            $this->row('/list/implied/deeper/file.txt'),
            $this->row('/list/implied/other.txt'),
            // A directory with its own entry, after its children
            $this->row('/list/real/file.txt'),
            $this->row('/list/real', [
                'type' => 'dir',
                'visibility' => Visibility::PRIVATE,
            ]),
        ]);

        $listing = $this->listing('/list');

        static::assertSame(['/list/file.txt', '/list/implied', '/list/real'], array_keys($listing));
        static::assertInstanceOf(FileAttributes::class, $listing['/list/file.txt']);
        static::assertInstanceOf(DirectoryAttributes::class, $listing['/list/implied']);
        // The directory's own entry is used, rather than an implied directory
        static::assertSame(Visibility::PRIVATE, $listing['/list/real']->visibility());
    }

    public function testListContentsDeep(): void
    {
        $this->respond(self::SQL_LIST, [
            $this->row('/list/file.txt'),
            $this->row('/list/implied/deeper/file.txt'),
            $this->row('/list/real', [
                'type' => 'dir',
            ]),
            // Duplicate directory entry
            $this->row('/list/real', [
                'type' => 'dir',
            ]),
            $this->row('/list/real/file.txt'),
        ]);

        $listing = $this->listing('/list', true);

        static::assertSame([
            '/list/file.txt',
            '/list/implied',
            '/list/implied/deeper',
            '/list/implied/deeper/file.txt',
            '/list/real',
            '/list/real/file.txt',
        ], array_keys($listing));
    }

    public function testListContentsRoot(): void
    {
        $this->respond(self::SQL_LIST, [
            $this->row('/root.txt'),
            $this->row('/list/file.txt'),
        ]);

        // A leading separator is not listed as a directory
        static::assertSame(['/root.txt', '/list'], array_keys($this->listing('')));
    }

    public function testListContentsWithDbFailure(): void
    {
        $this->respond(self::SQL_LIST, fail: true);

        // Flysystem v2 has no exception for listing; v3 adds UnableToListContents
        $this->expectException(\PDOException::class);
        $this->listing('/list');
    }

    public function testDeleteExpired(): void
    {
        $this->respond(self::SQL_FIND_EXPIRED, [
            [
                'path_id' => 123,
            ],
            [
                'path_id' => 456,
            ],
        ]);

        static::assertSame(2, $this->adapter->deleteExpired());

        static::assertNowParam($this->executedOnce(self::SQL_FIND_EXPIRED)['params']['now']);
        $this->assertDeleted(123);
        $this->assertDeleted(456);
    }

    public function testDeleteExpiredSpecific(): void
    {
        $now = new \DateTimeImmutable('+2 days');

        static::assertSame(0, $this->adapter->deleteExpired($now));

        static::assertSame([
            'now' => $now->format('Y-m-d H:i:s'),
        ], $this->executedOnce(self::SQL_FIND_EXPIRED)['params']);
        $this->assertNotExecuted(self::SQL_DELETE_PATH);
    }

    private function createExceptionModePdo(): \PDO&MockObject
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('getAttribute')
            ->with(\PDO::ATTR_ERRMODE)
            ->willReturn(\PDO::ERRMODE_EXCEPTION);
        return $pdo;
    }

    /**
     * The operation fails with the expected Flysystem exception, wrapping the DB exception
     */
    private function assertDbFailure(string $expectedException, \Closure $operation): void
    {
        try {
            $operation();
            static::fail("Expected {$expectedException} to be thrown");
        } catch (\Exception $e) {
            static::assertInstanceOf($expectedException, $e);
            static::assertInstanceOf(\PDOException::class, $e->getPrevious());
        }
    }

    /**
     * Register the rows returned by statements whose SQL contains the fragment. The first match is used.
     */
    private function respond(string $match, array $rows = [], bool $fail = false, int $rowCount = 0): void
    {
        $this->responses[] = [
            'match' => $match,
            'rows' => $rows,
            'fail' => $fail,
            'rowCount' => $rowCount,
        ];
    }

    /**
     * An existing file, which is not expired, found by the path lookup
     */
    private function givenFile(array $row = []): void
    {
        $this->respond(self::SQL_LOOKUP, [$this->row(self::PATH, $row)]);
    }

    /**
     * An existing file found by the lookup which includes expired files
     */
    private function givenRawFile(array $row = []): void
    {
        $this->respond(self::SQL_RAW_LOOKUP, [$this->row(self::PATH, $row + [
            'is_expired' => 0,
        ])]);
    }

    private function row(string $path, array $row = []): array
    {
        return $row + [
            'path_id' => 123,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => Visibility::PUBLIC,
            'size' => 0,
            'is_compressed' => 0,
            'expiry' => null,
            'meta' => null,
            'update_ts' => '2026-01-02 03:04:05',
        ];
    }

    private function prepare(string $sql): \PDOStatement
    {
        $response = [
            'rows' => [],
            'fail' => false,
            'rowCount' => 0,
        ];
        foreach ($this->responses as $candidate) {
            if (str_contains($sql, $candidate['match'])) {
                $response = $candidate;
                break;
            }
        }
        $rows = $response['rows'];

        $stmt = static::createStub(\PDOStatement::class);
        $stmt->method('execute')
            ->willReturnCallback(function (?array $params = null) use ($sql, $response): bool {
                $this->executed[] = [
                    'sql' => $sql,
                    'params' => $params ?? [],
                ];
                if ($response['fail']) {
                    throw new \PDOException('DB failure');
                }
                return true;
            });
        $stmt->method('fetch')
            ->willReturnCallback(function () use (&$rows): array|false {
                return array_shift($rows) ?? false;
            });
        $stmt->method('fetchAll')
            ->willReturnCallback(function () use (&$rows): array {
                $all = $rows;
                $rows = [];
                return $all;
            });
        $stmt->method('fetchColumn')
            ->willReturnCallback(function () use (&$rows): mixed {
                $row = array_shift($rows);
                return $row === null ? false : reset($row);
            });
        $stmt->method('rowCount')
            ->willReturn($response['rowCount']);
        $stmt->method('closeCursor')
            ->willReturn(true);

        return $stmt;
    }

    /**
     * @return array<int, array{sql: string, params: array}> Keyed by the order of execution
     */
    private function executed(string $match): array
    {
        return array_filter(
            $this->executed,
            fn(array $execution): bool => str_contains($execution['sql'], $match),
        );
    }

    /**
     * @return array{sql: string, params: array}
     */
    private function executedOnce(string $match): array
    {
        $executed = $this->executed($match);
        static::assertCount(1, $executed, "Expected one execution matching '{$match}'");
        return reset($executed);
    }

    /**
     * The content of the chunks inserted for the path ID
     */
    private function writtenContent(int $pathId): string
    {
        $content = '';
        foreach ($this->executed(self::SQL_INSERT_CHUNK) as $execution) {
            if ($execution['params']['path_id'] === $pathId) {
                $content .= $execution['params']['content'];
            }
        }
        return $content;
    }

    /**
     * @return array<string, StorageAttributes> Keyed by path
     */
    private function listing(string $path, bool $deep = false): array
    {
        $listing = [];
        foreach ($this->adapter->listContents($path, $deep) as $item) {
            static::assertArrayNotHasKey($item->path(), $listing, 'Duplicate listing for ' . $item->path());
            $listing[$item->path()] = $item;
        }
        return $listing;
    }

    private function assertNotExecuted(string $match): void
    {
        static::assertEmpty($this->executed($match), "Expected no execution matching '{$match}'");
    }

    private function assertExecutedBefore(string $first, string $second): void
    {
        $firstIndex = array_key_first($this->executed($first));
        $secondIndex = array_key_first($this->executed($second));
        static::assertNotNull($firstIndex, "Expected an execution matching '{$first}'");
        static::assertNotNull($secondIndex, "Expected an execution matching '{$second}'");
        static::assertLessThan($secondIndex, $firstIndex, "Expected '{$first}' before '{$second}'");
    }

    /**
     * The path entry and its chunks are deleted
     */
    private function assertDeleted(int $pathId): void
    {
        foreach ([self::SQL_DELETE_PATH, self::SQL_DELETE_CHUNKS] as $delete) {
            $deleted = array_filter(
                $this->executed($delete),
                fn(array $execution): bool => ($execution['params']['path_id'] ?? null) === $pathId,
            );
            static::assertCount(1, $deleted, "Expected '{$delete}' for path ID {$pathId}");
        }
    }

    /**
     * @param array{sql: string, params: array} $execution
     */
    private static function assertExcludesExpired(array $execution): void
    {
        static::assertStringContainsString(self::NOT_EXPIRED_CONDITION, $execution['sql']);
        static::assertArrayHasKey('now', $execution['params']);
        static::assertNowParam($execution['params']['now']);
    }

    private static function assertNowParam(string $actual): void
    {
        // Allow for the clock ticking over between the call and the assertion
        static::assertMatchesRegularExpression('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $actual);
        static::assertEqualsWithDelta(time(), strtotime($actual), 1);
    }

    /**
     * Compare params regardless of their order, optionally ignoring some which are checked separately
     */
    private static function assertParams(array $expected, array $actual, array $ignore = []): void
    {
        $actual = array_diff_key($actual, array_flip($ignore));
        ksort($expected);
        ksort($actual);
        static::assertSame($expected, $actual);
    }
}
