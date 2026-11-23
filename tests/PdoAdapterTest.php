<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use Phlib\Flysystem\Pdo\PdoAdapter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class PdoAdapterTest extends TestCase
{
    private PdoAdapter $adapter;

    private \PDO&MockObject $pdo;

    private Config $emptyConfig;

    protected function setUp(): void
    {
        parent::setUp();
        $this->emptyConfig = new Config();
        $this->pdo = $this->createMock(\PDO::class);
        $this->pdo->method('getAttribute')
            ->willReturnCallback(fn(int $attribute) => match ($attribute) {
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                default => null,
            });
        $this->adapter = new PdoAdapter($this->pdo);
    }

    public function testImplementsAdapterInterface(): void
    {
        static::assertInstanceOf(AdapterInterface::class, $this->adapter);
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

    public static function dataRequiresExceptionErrorMode(): array
    {
        return [
            'silent' => [\PDO::ERRMODE_SILENT],
            'warning' => [\PDO::ERRMODE_WARNING],
        ];
    }

    #[DataProvider('dataDbFailureThrowsException')]
    public function testDbFailureThrowsException(string $method, array $args): void
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')
            ->willThrowException(new \PDOException('DB failure'));
        $this->pdo->method('prepare')
            ->willReturn($stmt);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('DB failure');

        $this->adapter->{$method}(...$args);
    }

    public static function dataDbFailureThrowsException(): array
    {
        $config = new Config();
        return [
            'write' => ['write', ['/file.txt', '', $config]],
            'writeStream' => ['writeStream', ['/file.txt', null, $config]],
            'update' => ['update', ['/file.txt', 'Test Content', $config]],
            'updateStream' => ['updateStream', ['/file.txt', 'Test Content', $config]],
            'rename' => ['rename', ['/the-old-name.asp', '/the-new-name.php']],
            'copy' => ['copy', ['/file.txt', '/copy.txt']],
            'delete' => ['delete', ['/file.txt']],
            'deleteDir' => ['deleteDir', ['/some-directory']],
            'createDir' => ['createDir', ['/path', $config]],
            'setVisibility' => ['setVisibility', ['/file.txt', AdapterInterface::VISIBILITY_PRIVATE]],
            'has' => ['has', ['/file.txt']],
            'read' => ['read', ['/file.txt']],
            'readStream' => ['readStream', ['/file.txt']],
            'listContents' => ['listContents', ['/path']],
            'getMetadata' => ['getMetadata', ['/file.txt']],
            'deleteExpired' => ['deleteExpired', []],
        ];
    }

    public function testWriteWithDbFailureRemovesTempFile(): void
    {
        $tempDir = sys_get_temp_dir() . '/flysystempdo-test-' . bin2hex(random_bytes(4));
        mkdir($tempDir);
        $adapter = new PdoAdapter($this->pdo, new Config([
            'temp_dir' => $tempDir,
        ]));

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')
            ->willThrowException(new \PDOException('DB failure'));
        $this->pdo->method('prepare')
            ->willReturn($stmt);

        try {
            $adapter->write('/file.txt', 'Test Content', $this->emptyConfig);
            static::fail('Expected exception was not thrown');
        } catch (\PDOException) {
            static::assertSame([], array_diff(scandir($tempDir), ['.', '..']));
        } finally {
            array_map(unlink(...), glob($tempDir . '/*'));
            rmdir($tempDir);
        }
    }

    public function testMysqlBufferingDisabledByDefault(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('getAttribute')
            ->with(\PDO::ATTR_ERRMODE)
            ->willReturn(\PDO::ERRMODE_EXCEPTION);
        $pdo->expects(static::once())
            ->method('setAttribute')
            ->with(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

        new PdoAdapter($pdo);
    }

    public function testMysqlBufferingDisabledByDefaultWithOtherConfig(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('getAttribute')
            ->with(\PDO::ATTR_ERRMODE)
            ->willReturn(\PDO::ERRMODE_EXCEPTION);
        $pdo->expects(static::once())
            ->method('setAttribute')
            ->with(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

        new PdoAdapter($pdo, new Config([
            'table_prefix' => 'myprefix',
        ]));
    }

    public function testMysqlBufferingNotDisabled(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('getAttribute')
            ->with(\PDO::ATTR_ERRMODE)
            ->willReturn(\PDO::ERRMODE_EXCEPTION);
        $pdo->expects(static::never())
            ->method('setAttribute');

        new PdoAdapter($pdo, new Config([
            'disable_mysql_buffering' => false,
        ]));
    }

    public function testTablePrefixDefault(): void
    {
        $default = 'flysystem';
        $stmt = $this->createMock(\PDOStatement::class);
        $this->pdo->expects(static::atLeastOnce())
            ->method('prepare')
            ->with(static::stringContains($default))
            ->willReturn($stmt);

        $this->adapter->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testTablePrefixConfigurationIsHonored(): void
    {
        $prefix = 'myprefix';
        $config = new Config([
            'table_prefix' => $prefix,
        ]);
        $stmt = $this->createMock(\PDOStatement::class);
        $this->pdo->expects(static::atLeastOnce())
            ->method('prepare')
            ->with(static::stringContains($prefix))
            ->willReturn($stmt);

        (new PdoAdapter($this->pdo, $config))->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testTablePrefixConfigurationWithBlankValue(): void
    {
        $default = 'flysystem';
        $prefix = '';
        $config = new Config([
            'table_prefix' => $prefix,
        ]);
        $stmt = $this->createMock(\PDOStatement::class);
        $this->pdo->expects(static::atLeastOnce())
            ->method('prepare')
            ->with(static::stringContains($default))
            ->willReturn($stmt);

        (new PdoAdapter($this->pdo, $config))->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testWrite(): void
    {
        $this->setupBasicDbResponse();

        $pathId = rand(1, 1000);

        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) $pathId);

        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $meta = $this->adapter
            ->write($path, $content, $this->emptyConfig);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'timestamp' => time(),
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'visibility' => 'public',
        ];
        static::assertSame($expected, $meta);
    }

    public function testWriteWithExpiry(): void
    {
        $this->setupBasicDbResponse();

        $pathId = rand(1, 1000);

        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) $pathId);

        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $expiry = new \DateTimeImmutable('+2 day');
        $expiryString = $expiry->format('Y-m-d H:i:s');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $actual = $this->adapter
            ->write($path, $content, $config);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'timestamp' => time(),
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'visibility' => 'public',
            'expiry' => $expiryString,
        ];

        // Change DT object to a string, for comparison
        $actual['expiry'] = $actual['expiry']->format('Y-m-d H:i:s');
        static::assertSame($expected, $actual);
    }

    public function testWriteWithExpiryPast(): void
    {
        // An already-expired file is not stored
        $this->pdo->expects(static::never())
            ->method('prepare');

        $config = new Config([
            'expiry' => new \DateTimeImmutable('-2 day'),
        ]);

        static::assertFalse($this->adapter->write('/path/to/file.txt', 'Test Content', $config));
    }

    public function testWriteStreamWithExpiryPast(): void
    {
        // An already-expired file is not stored
        $this->pdo->expects(static::never())
            ->method('prepare');

        $config = new Config([
            'expiry' => new \DateTimeImmutable('-2 day'),
        ]);

        $actual = $this->adapter->writeStream('/path/to/file.txt', $this->createTempResource('Test Content'), $config);
        static::assertFalse($actual);
    }

    public function testWriteWithAdditionalFields(): void
    {
        $this->setupBasicDbResponse();

        $pathId = rand(1, 1000);

        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) $pathId);

        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $additional = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config = new Config([
            'meta' => $additional,
        ]);

        $actual = $this->adapter
            ->write($path, $content, $config);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'timestamp' => time(),
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'visibility' => 'public',
            'meta' => $additional,
        ];
        static::assertSame($expected, $actual);
    }

    public function testWriteDetectsMimetypeByExtension(): void
    {
        $this->setupBasicDbResponse();
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) rand(1, 1000));

        $meta = $this->adapter
            ->write(
                '/example.json',
                '',
                $this->emptyConfig,
            );

        static::assertSame('application/json', $meta['mimetype']);
    }

    public function testWriteDetectsMimetypeByContent(): void
    {
        $this->setupBasicDbResponse();
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) rand(1, 1000));

        $meta = $this->adapter
            ->write(
                '/missing-extension',
                base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs=', true), // 1x1 trans gif
                $this->emptyConfig,
            );

        static::assertSame('image/gif', $meta['mimetype']);
    }

    public function testWriteToStream(): void
    {
        $this->setupBasicDbResponse();

        $pathId = rand(1, 1000);
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) $pathId);

        $path = '/some/path/to/file.txt';
        $content = 'Test File';
        $handle = $this->createTempResource($content);
        $meta = $this->adapter
            ->writeStream($path, $handle, $this->emptyConfig);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'timestamp' => time(),
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'visibility' => 'public',
        ];
        static::assertSame($expected, $meta);
    }

    public function testWriteToStreamDetectsMimetypeByExtension(): void
    {
        $this->setupBasicDbResponse();
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) rand(1, 1000));

        $handle = $this->createTempResource();
        $meta = $this->adapter
            ->writeStream(
                '/example.json',
                $handle,
                $this->emptyConfig,
            );

        static::assertSame('application/json', $meta['mimetype']);
    }

    /**
     * This test documents the intended behaviour. The adapter attempts at no point, when using streams,
     * to load the file into memory.
     */
    public function testWriteToStreamFailsToDetectMimetypeByContent(): void
    {
        $this->setupBasicDbResponse();
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) rand(1, 1000));

        $content = base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs=', true); // 1x1 transparent gif
        $handle = $this->createTempResource($content);
        $meta = $this->adapter
            ->writeStream(
                '/missing-extension',
                $handle,
                $this->emptyConfig,
            );

        static::assertNotEquals('image/gif', $meta['mimetype']);
    }

    public function testUpdate(): void
    {
        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $data = [
            'path_id' => 123,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);

        $meta = $this->adapter
            ->update($path, $content, $this->emptyConfig);

        static::assertSame(strlen($content), $meta['size']);
    }

    public function testUpdateWithExpiry(): void
    {
        $pathId = rand(1, 1000);

        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $expiry = new \DateTimeImmutable('+2 day');
        $expiryString = $expiry->format('Y-m-d H:i:s');
        $config = new Config([
            'expiry' => $expiry,
        ]);

        $data = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            // no expiry set
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);

        $actual = $this->adapter
            ->update($path, $content, $config);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'timestamp' => time(),
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'visibility' => 'public',
            'expiry' => $expiryString,
        ];

        // Change DT object to a string, for comparison
        $actual['expiry'] = $actual['expiry']->format('Y-m-d H:i:s');
        static::assertSame($expected, $actual);
    }

    public function testUpdateWithAdditionalFields(): void
    {
        $pathId = rand(1, 1000);

        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $additional = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $config = new Config([
            'meta' => $additional,
        ]);

        $data = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            // no meta set
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);

        $actual = $this->adapter
            ->update($path, $content, $config);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => $path,
            'timestamp' => time(),
            'mimetype' => 'text/plain',
            'size' => strlen($content),
            'visibility' => 'public',
            'meta' => $additional,
        ];
        static::assertSame($expected, $actual);
    }

    public function testUpdateDetectsMimetypeByExtension(): void
    {
        $path = '/example.json';
        $data = [
            'path_id' => 123,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);
        $meta = $this->adapter
            ->update($path, '', $this->emptyConfig);

        static::assertSame('application/json', $meta['mimetype']);
    }

    public function testUpdateDetectsMimetypeByContent(): void
    {
        $path = '/missing-extension';
        $data = [
            'path_id' => 123,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);
        $meta = $this->adapter
            ->update(
                $path,
                base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs=', true), // 1x1 trans gif
                $this->emptyConfig,
            );

        static::assertSame('image/gif', $meta['mimetype']);
    }

    public function testUpdateToStream(): void
    {
        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $data = [
            'path_id' => 123,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);

        $meta = $this->adapter
            ->updateStream($path, $content, $this->emptyConfig);

        static::assertSame(strlen($content), $meta['size']);
    }

    public function testUpdateToStreamDetectsMimetypeByExtension(): void
    {
        $path = '/example.json';
        $data = [
            'path_id' => 123,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);
        $handle = $this->createTempResource();
        $meta = $this->adapter
            ->updateStream($path, $handle, $this->emptyConfig);

        static::assertSame('application/json', $meta['mimetype']);
    }

    public function testUpdateToStreamFailsToDetectsMimetypeByContent(): void
    {
        $path = '/missing-extension';
        $data = [
            'path_id' => 123,
            'type' => 'file',
            'path' => $path,
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 214454,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);
        $content = base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs=', true); // 1x1 transparent gif
        $handle = $this->createTempResource($content);
        $meta = $this->adapter
            ->updateStream($path, $handle, $this->emptyConfig);

        static::assertNotEquals('image/gif', $meta['mimetype']);
    }

    public function testRenameFile(): void
    {
        $path = '/the-old-name.asp';
        $newName = '/the-new-name.php';
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'path' => $path,
            'type' => 'file',
            'visibility' => 'public',
            'size' => 2341,
            'update_ts' => date('Y-m-d H:i:s'),
            'mimetype' => 'text/plain',
        ]);

        $actual = $this->adapter->rename($path, $newName);
        static::assertTrue($actual);
    }

    public function testRenameDirectory(): void
    {
        $path = '/the-old-name';
        $newName = '/the-new-name';
        $this->setupDbMultiCall([
            [
                'method' => 'fetch',
                'response' => [
                    'path_id' => 123,
                    'path' => $path,
                    'type' => 'dir',
                    'update_ts' => date('Y-m-d H:i:s'),
                ],
            ], [
                'method' => 'fetchAll',
                'response' => [
                    [
                        'path_id' => 1234,
                        'path' => $path . '/somefile.txt',
                        'type' => 'file',
                        'visibility' => 'public',
                        'size' => 2341,
                        'update_ts' => date('Y-m-d H:i:s'),
                        'mimetype' => 'text/plain',
                    ],
                ],
            ],
        ]);

        $actual = $this->adapter->rename($path, $newName);
        static::assertTrue($actual);
    }

    public function testCopy(): void
    {
        $path = '/the-old-name.txt';
        $newpath = '/the-new-name.txt';
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'path' => $path,
            'type' => 'file',
            'mimetype' => 'text/plain',
            'visibility' => 'public',
            'size' => 2341,
            'is_compressed' => 1,
            'update_ts' => date('Y-m-d H:i:s'),
        ]);
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn((string) rand(1, 1000));

        $actual = $this->adapter->copy($path, $newpath);
        static::assertTrue($actual);
    }

    public function testCopyFailsToFindPath(): void
    {
        $path = '/the-old-name.txt';
        $newpath = '/the-new-name.txt';
        $stmt = $this->setupBasicDbResponse();
        $stmt->method('fetch')
            ->willReturn(false);

        $actual = $this->adapter->copy($path, $newpath);
        static::assertFalse($actual);
    }

    public function testDelete(): void
    {
        $this->setupDbFetchResponse([
            'type' => 'file',
            'path_id' => 123,
        ]);
        static::assertTrue($this->adapter->delete('/some-file.txt'));
    }

    public function testDeleteWithNonFilePath(): void
    {
        $this->setupDbFetchResponse([
            'type' => 'dir',
            'path_id' => 123,
        ]);
        static::assertFalse($this->adapter->delete('/some-file.txt'));
    }

    public function testDeleteEmptyDirectory(): void
    {
        $this->setupDbMultiCall([
            [
                'method' => 'fetch',
                'response' => [
                    'type' => 'dir',
                    'path_id' => 123,
                ],
            ], [
                'method' => 'fetchAll',
                'response' => [],
            ],
        ]);
        static::assertTrue($this->adapter->deleteDir('/some-directory'));
    }

    public function testDeleteDirectoryWithChildren(): void
    {
        $this->setupDbMultiCall([
            [
                'method' => 'fetch',
                'response' => [
                    'type' => 'dir',
                    'path_id' => 123,
                    'path' => '/path/to',
                    'update_ts' => date('Y-m-d H:i:s'),
                ],
            ], [
                'method' => 'fetchAll',
                'response' => [
                    [
                        'type' => 'file',
                        'path_id' => 321,
                        'path' => '/path/to/file.txt',
                        'mimetype' => 'text/plain',
                        'visibility' => 'public',
                        'size' => 1234,
                        'update_ts' => date('Y-m-d H:i:s'),
                    ],
                ],
            ],
        ]);
        static::assertTrue($this->adapter->deleteDir('/some-directory'));
    }

    public function testDeleteDirWithNonDirectoryPath(): void
    {
        $this->setupDbFetchResponse([
            'type' => 'file',
            'path_id' => 123,
        ]);
        static::assertFalse($this->adapter->deleteDir('/some-file.txt'));
    }

    public function testCreateDir(): void
    {
        $pathId = 12345;
        $this->setupBasicDbResponse();
        $this->pdo->method('lastInsertId')
            ->willReturn((string) $pathId);

        $meta = $this->adapter->createDir('/path', $this->emptyConfig);
        static::assertSame($pathId, $meta['path_id']);
    }

    public function testCreateDirWithAdditionalFields(): void
    {
        $pathId = 12345;
        $this->setupBasicDbResponse();
        $this->pdo->method('lastInsertId')
            ->willReturn((string) $pathId);

        $additional = [
            uniqid('key1') => sha1(uniqid('value1')),
            uniqid('key2') => sha1(uniqid('value2')),
        ];
        $actual = $this->adapter->createDir('/path', new Config([
            'meta' => $additional,
        ]));

        static::assertArrayHasKey('meta', $actual);
        static::assertSame($additional, $actual['meta']);
    }

    public function testSetVisibility(): void
    {
        $path = '/path/to/file.txt';
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'path' => $path,
            'type' => 'file',
            'mimetype' => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size' => 1234,
            'update_ts' => date('Y-m-d H:i:s'),
        ]);
        $meta = $this->adapter
            ->setVisibility('/path/to/file.txt', AdapterInterface::VISIBILITY_PRIVATE);
        static::assertNotFalse($meta);
    }

    public function testHas(): void
    {
        $stmt = $this->setupBasicDbResponse();
        $stmt->method('fetchColumn')
            ->willReturn(1);
        static::assertTrue($this->adapter->has('/this/path'));
    }

    public function testHasExcludesExpired(): void
    {
        $path = '/this/path';

        $stmt = $this->createMock(\PDOStatement::class);
        $this->pdo->expects(static::once())
            ->method('prepare')
            ->with(static::stringContains('WHERE path = :path AND (expiry IS NULL OR expiry > :now)'))
            ->willReturn($stmt);

        $stmt->expects(static::once())
            ->method('execute')
            ->with(static::callback(function (array $params) use ($path): bool {
                static::assertSame($path, $params['path']);
                static::assertNowParam($params['now']);
                return true;
            }))
            ->willReturn(true);

        // An expired row is excluded by the DB query
        $stmt->expects(static::once())
            ->method('fetchColumn')
            ->willReturn(false);

        static::assertFalse($this->adapter->has($path));
    }

    public function testRead(): void
    {
        $pathId = rand(1, 1000);
        $size = rand(1000, 99999);
        $path = '/path/to/file.txt';
        $updateTs = date('Y-m-d H:i:s');

        $this->setupDbFetchResponse([
            'path_id' => $pathId,
            'path' => $path,
            'type' => 'file',
            'mimetype' => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size' => $size,
            'is_compressed' => false,
            'update_ts' => $updateTs,
        ]);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => '/path/to/file.txt',
            'timestamp' => strtotime($updateTs),
            'mimetype' => 'text/plain',
            'size' => $size,
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'contents' => '',
        ];

        $actual = $this->adapter->read($path);
        static::assertSame($expected, $actual);
    }

    public function testReadWithExpiryFuture(): void
    {
        $pathId = rand(1, 1000);
        $size = rand(1000, 99999);
        $path = '/path/to/file.txt';
        $updateTs = date('Y-m-d H:i:s');
        $expiry = new \DateTimeImmutable('+2 day');
        $expiryString = $expiry->format('Y-m-d H:i:s');

        $this->setupDbFetchResponse([
            'path_id' => $pathId,
            'path' => $path,
            'type' => 'file',
            'mimetype' => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size' => $size,
            'is_compressed' => false,
            'expiry' => $expiryString,
            'update_ts' => $updateTs,
        ]);

        $expected = [
            'path_id' => $pathId,
            'type' => 'file',
            'path' => '/path/to/file.txt',
            'timestamp' => strtotime($updateTs),
            'mimetype' => 'text/plain',
            'size' => $size,
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'expiry' => $expiryString,
            'contents' => '',
        ];

        $actual = $this->adapter->read($path);
        // Change DT object to a string, for comparison
        $actual['expiry'] = $actual['expiry']->format('Y-m-d H:i:s');
        static::assertSame($expected, $actual);
    }

    public function testReadExcludesExpired(): void
    {
        $path = '/path/to/file.txt';

        $stmt = $this->createMock(\PDOStatement::class);
        $this->pdo->expects(static::once())
            ->method('prepare')
            ->with(static::stringContains('WHERE path = :path AND (expiry IS NULL OR expiry > :now)'))
            ->willReturn($stmt);

        $stmt->expects(static::once())
            ->method('execute')
            ->with(static::callback(function (array $params) use ($path): bool {
                static::assertSame($path, $params['path']);
                static::assertNowParam($params['now']);
                return true;
            }))
            ->willReturn(true);

        // An expired row is excluded by the DB query
        $stmt->expects(static::once())
            ->method('fetch')
            ->willReturn(false);

        static::assertFalse($this->adapter->read($path));
    }

    public function testReadReturnsContents(): void
    {
        $path = '/path/to/file.txt';
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'path' => $path,
            'type' => 'file',
            'mimetype' => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size' => 1234,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ]);
        $meta = $this->adapter->read($path);
        static::assertArrayHasKey('contents', $meta);
    }

    public function testReadStreamReturnsStream(): void
    {
        $path = '/path/to/file.txt';
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'path' => $path,
            'type' => 'file',
            'mimetype' => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size' => 1234,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ]);
        $meta = $this->adapter->readStream($path);
        static::assertTrue(is_resource($meta['stream']));
    }

    public function testListContents(): void
    {
        $this->setupDbFetchResponse(
            [
                [
                    'path_id' => 123,
                    'path' => '/path/file.txt',
                    'type' => 'file',
                    'mimetype' => 'text/plain',
                    'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
                    'size' => 1234,
                    'is_compressed' => false,
                    'update_ts' => date('Y-m-d H:i:s'),
                ],
            ],
            'fetchAll',
        );
        $listing = $this->adapter->listContents('/path');
        static::assertCount(1, $listing);
    }

    public function testListContentsExcludesExpired(): void
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $this->pdo->expects(static::once())
            ->method('prepare')
            ->with(static::stringContains(
                'WHERE (expiry IS NULL OR expiry > :now) AND (path LIKE :prefix OR path = :path)',
            ))
            ->willReturn($stmt);

        $stmt->expects(static::once())
            ->method('execute')
            ->with(static::callback(function (array $params): bool {
                static::assertSame('/path/%', $params['prefix']);
                static::assertSame('/path', $params['path']);
                static::assertNowParam($params['now']);
                return true;
            }))
            ->willReturn(true);

        $stmt->expects(static::once())
            ->method('fetchAll')
            ->willReturn([]);

        static::assertEmpty($this->adapter->listContents('/path'));
    }

    public function testListContentsRootExcludesExpired(): void
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $this->pdo->expects(static::once())
            ->method('prepare')
            ->with(static::logicalAnd(
                static::stringEndsWith('WHERE (expiry IS NULL OR expiry > :now)'),
                static::logicalNot(static::stringContains(':prefix')),
            ))
            ->willReturn($stmt);

        $stmt->expects(static::once())
            ->method('execute')
            ->with(static::callback(function (array $params): bool {
                static::assertSame(['now'], array_keys($params));
                static::assertNowParam($params['now']);
                return true;
            }))
            ->willReturn(true);

        $stmt->expects(static::once())
            ->method('fetchAll')
            ->willReturn([]);

        static::assertEmpty($this->adapter->listContents());
    }

    public function testListContentsWithEmtpyResults(): void
    {
        $this->setupDbFetchResponse([], 'fetchAll');
        $listing = $this->adapter->listContents('/path');
        static::assertEmpty($listing);
    }

    public function testGetMetadataHasCorrectKeysForFile(): void
    {
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'type' => 'file',
            'path' => '/path/file.txt',
            'mimetype' => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size' => 1234,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
        ]);

        $meta = $this->adapter->getMetadata('/path/file.txt');
        $expectedKeys = ['path_id', 'type', 'path', 'mimetype', 'visibility', 'size', 'timestamp'];
        $unexpectedKeys = array_diff_key($meta, array_flip($expectedKeys));
        static::assertEmpty($unexpectedKeys);
    }

    public function testGetMetadataHasCorrectKeysForFileWithAdditionalFields(): void
    {
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'type' => 'file',
            'path' => '/path/file.txt',
            'mimetype' => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size' => 1234,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s'),
            'expiry' => date('Y-m-d H:i:s', strtotime('+2 days')),
            'meta' => json_encode([
                'owner' => 'exampleFoo',
            ]),
        ]);

        $meta = $this->adapter->getMetadata('/path/file.txt');
        $expectedKeys = ['path_id', 'type', 'path', 'mimetype', 'visibility', 'size', 'timestamp', 'expiry', 'meta'];
        $unexpectedKeys = array_diff_key($meta, array_flip($expectedKeys));

        static::assertEmpty($unexpectedKeys);
    }

    public function testGetMetadataNormalizesDataForDirectory(): void
    {
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'type' => 'dir',
            'path' => '/path/file.txt',
            'mimetype' => null,
            'visibility' => null,
            'size' => null,
            'is_compressed' => 0,
            'update_ts' => date('Y-m-d H:i:s'),
        ]);

        $meta = $this->adapter->getMetadata('/path/file.txt');
        $expectedKeys = ['path_id', 'type', 'path', 'timestamp'];
        $unexpectedKeys = array_diff_key($meta, array_flip($expectedKeys));
        static::assertEmpty($unexpectedKeys);
    }

    public function testGetMetadataNormalizesDataForDirectoryWithAdditionalFields(): void
    {
        $this->setupDbFetchResponse([
            'path_id' => 123,
            'type' => 'dir',
            'path' => '/path/file.txt',
            'mimetype' => null,
            'visibility' => null,
            'size' => null,
            'is_compressed' => 0,
            'update_ts' => date('Y-m-d H:i:s'),
            'expiry' => date('Y-m-d H:i:s', strtotime('+2 days')),
            'meta' => json_encode([
                'owner' => 'exampleFoo',
            ]),
        ]);

        $meta = $this->adapter->getMetadata('/path/file.txt');
        $expectedKeys = ['path_id', 'type', 'path', 'timestamp', 'expiry', 'meta'];
        $unexpectedKeys = array_diff_key($meta, array_flip($expectedKeys));
        static::assertEmpty($unexpectedKeys);
    }

    /**
     * @param mixed $expectValue
     */
    #[DataProvider('individualAttributeGetMethodsDataProvider')]
    public function testIndividualAttributeGetMethods(array $rowData, string $attribute, $expectValue): void
    {
        $method = 'get' . ucfirst($attribute);
        $this->setupDbFetchResponse($rowData);
        static::assertSame($expectValue, $this->adapter->{$method}($rowData['path'])[$attribute]);
    }

    public static function individualAttributeGetMethodsDataProvider(): array
    {
        $size = 1234;
        $mimetype = 'text/plain';
        $timestamp = time();
        $visibility = AdapterInterface::VISIBILITY_PUBLIC;
        $rowData = [
            'path_id' => 123,
            'type' => 'file',
            'path' => '/path/file.txt',
            'mimetype' => $mimetype,
            'visibility' => $visibility,
            'size' => $size,
            'is_compressed' => false,
            'update_ts' => date('Y-m-d H:i:s', $timestamp),
        ];
        return [
            [$rowData, 'size', $size],
            [$rowData, 'mimetype', $mimetype],
            [$rowData, 'timestamp', $timestamp],
            [$rowData, 'visibility', $visibility],
        ];
    }

    public function testDeleteExpiredDefault(): void
    {
        $pathId = rand(1, 1000);

        $rows = [[
            'path_id' => $pathId,
        ]];

        $expiredStmt = $this->createMock(\PDOStatement::class);
        $deleteStmt = $this->createMock(\PDOStatement::class);
        $deleteChunksStmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects(static::exactly(3))
            ->method('prepare')
            ->with()
            ->willReturnCallback(function (string $actualSql) use ($expiredStmt, $deleteStmt, $deleteChunksStmt): \PDOStatement {
                static $callCount = 0;
                $callCount++;

                switch ($callCount) {
                    case 1:
                        static::assertStringContainsString('WHERE expiry <= :now', $actualSql);
                        return $expiredStmt;
                    case 2:
                        static::assertStringContainsString('DELETE FROM flysystem_path ', $actualSql);
                        static::assertStringContainsString('WHERE path_id = :path_id', $actualSql);
                        return $deleteStmt;
                    case 3:
                        static::assertStringContainsString('DELETE FROM flysystem_chunk ', $actualSql);
                        static::assertStringContainsString('WHERE path_id = :path_id', $actualSql);
                        return $deleteChunksStmt;
                    default:
                        static::fail('Method called more times than expected');
                }
            });

        $expiredStmt->expects(static::once())
            ->method('execute')
            ->with([
                'now' => date('Y-m-d H:i:s'),
            ])
            ->willReturn(true);

        $expiredStmt->expects(static::once())
            ->method('fetchAll')
            ->willReturn($rows);

        $deleteStmt->expects(static::once())
            ->method('execute')
            ->with([
                'path_id' => $pathId,
            ])
            ->willReturn(true);

        $deleteChunksStmt->expects(static::once())
            ->method('execute')
            ->with([
                'path_id' => $pathId,
            ])
            ->willReturn(true);

        static::assertSame(1, $this->adapter->deleteExpired());
    }

    public function testDeleteExpiredSpecific(): void
    {
        $expiry = new \DateTimeImmutable('+2 day');
        $expiryString = $expiry->format('Y-m-d H:i:s');

        $selectSql = static::stringContains('WHERE expiry <= :now');
        $expiredStmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects(static::once())
            ->method('prepare')
            ->with($selectSql)
            ->willReturn($expiredStmt);

        $expiredStmt->expects(static::once())
            ->method('execute')
            ->with([
                'now' => $expiryString,
            ])
            ->willReturn(true);

        $expiredStmt->expects(static::once())
            ->method('fetchAll')
            ->willReturn([]);

        static::assertSame(0, $this->adapter->deleteExpired($expiry));
    }

    private static function assertNowParam(string $actual): void
    {
        // Allow for the clock ticking over between the call and the assertion
        static::assertMatchesRegularExpression('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $actual);
        static::assertEqualsWithDelta(time(), strtotime($actual), 1);
    }

    /**
     * @return resource
     */
    protected function createTempResource($content = '')
    {
        $handle = fopen('php://temp', 'w+b');
        fwrite($handle, $content);
        rewind($handle);
        return $handle;
    }

    /**
     * @param mixed $response
     * @return \PDOStatement|MockObject
     */
    protected function setupBasicDbResponse($response = true): \PDOStatement
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')
            ->willReturn($response);

        $this->pdo->method('prepare')
            ->willReturn($stmt);

        return $stmt;
    }

    /**
     * @return \PDOStatement|MockObject
     */
    protected function setupDbFetchResponse(array $response, $method = 'fetch'): \PDOStatement
    {
        return $this->setupDbMultiCall([
            [
                'method' => $method,
                'response' => $response,
            ],
        ]);
    }

    /**
     * @return \PDOStatement|MockObject
     */
    public function setupDbMultiCall(array $calls): \PDOStatement
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')
            ->willReturn(true);
        foreach ($calls as $call) {
            $stmt->method($call['method'])
                ->willReturn($call['response']);
        }

        $this->pdo->method('prepare')
            ->willReturn($stmt);

        return $stmt;
    }
}
