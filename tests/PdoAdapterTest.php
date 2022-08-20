<?php

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use Phlib\Flysystem\Pdo\PdoAdapter;

class PdoAdapterTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var PdoAdapter
     */
    protected $adapter;

    /**
     * @var \PDO|\PHPUnit_Framework_MockObject_MockObject
     */
    protected $pdo;

    /**
     * @var Config|\PHPUnit_Framework_MockObject_MockObject
     */
    protected $emptyConfig;

    public function setUp(): void
    {
        parent::setUp();
        $this->emptyConfig = new Config();
        $this->pdo = $this->getMock(PdoMock::class);
        $this->adapter = new PdoAdapter($this->pdo);
    }

    public function tearDown(): void
    {
        $this->adapter = null;
        $this->pdo = null;
        $this->emptyConfig = null;
        parent::tearDown();
    }

    public function testImplementsAdapterInterface(): void
    {
        $this->assertInstanceOf(AdapterInterface::class, $this->adapter);
    }

    public function testTablePrefixDefault(): void
    {
        $default = 'flysystem';
        $stmt = $this->getMock(\PDOStatement::class);
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains($default))
            ->will($this->returnValue($stmt));

        $this->adapter->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testTablePrefixConfigurationIsHonored(): void
    {
        $prefix = 'myprefix';
        $config = new Config([
            'table_prefix' => $prefix,
        ]);
        $stmt = $this->getMock(\PDOStatement::class);
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains($prefix))
            ->will($this->returnValue($stmt));

        (new PdoAdapter($this->pdo, $config))->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testTablePrefixConfigurationWithBlankValue(): void
    {
        $default = 'flysystem';
        $prefix = '';
        $config = new Config([
            'table_prefix' => $prefix,
        ]);
        $stmt = $this->getMock(\PDOStatement::class);
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains($default))
            ->will($this->returnValue($stmt));

        (new PdoAdapter($this->pdo, $config))->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testWrite(): void
    {
        $this->setupBasicDbResponse();

        $pathId = rand(1, 1000);

        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn($pathId);

        $path = '/some/path/to/file.txt';
        $content = 'Test Content';
        $meta = $this->adapter
            ->write($path, $content, $this->emptyConfig);

        $expected = [
            'type' => 'file',
            'path' => $path,
            'visibility' => 'public',
            'size' => strlen($content),
            'timestamp' => time(), // this is going to bite me in the arse!
            'path_id' => $pathId,
            'mimetype' => 'text/plain',
        ];
        $this->assertEquals($expected, $meta);
    }

    public function testWriteWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->write('/file.txt', '', $this->emptyConfig);
        $this->assertFalse($meta);
    }

    public function testWriteDetectsMimetypeByExtension(): void
    {
        $this->setupBasicDbResponse();
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn(rand(1, 1000));

        $meta = $this->adapter
            ->write(
                '/example.json',
                '',
                $this->emptyConfig
            );

        $this->assertEquals('application/json', $meta['mimetype']);
    }

    public function testWriteDetectsMimetypeByContent(): void
    {
        $this->setupBasicDbResponse();
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn(rand(1, 1000));

        $meta = $this->adapter
            ->write(
                '/missing-extension',
                base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs='), // 1x1 trans gif
                $this->emptyConfig
            );

        $this->assertEquals('image/gif', $meta['mimetype']);
    }

    public function testWriteToStream(): void
    {
        $this->setupBasicDbResponse();

        $pathId = rand(1, 1000);
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn($pathId);

        $path = '/some/path/to/file.txt';
        $content = 'Test File';
        $handle = $this->createTempResource($content);
        $meta = $this->adapter
            ->writeStream($path, $handle, $this->emptyConfig);

        $expected = [
            'type' => 'file',
            'path' => $path,
            'visibility' => 'public',
            'size' => strlen($content),
            'timestamp' => time(), // this is going to bite me in the arse!
            'path_id' => $pathId,
            'mimetype' => 'text/plain',
        ];
        $this->assertEquals($expected, $meta);
    }

    public function testWriteToStreamWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->writeStream('/file.txt', null, $this->emptyConfig);
        $this->assertFalse($meta);
    }

    public function testWriteToStreamDetectsMimetypeByExtension(): void
    {
        $this->setupBasicDbResponse();
        $this->pdo->expects(static::once())
            ->method('lastInsertId')
            ->willReturn(rand(1, 1000));

        $handle = $this->createTempResource();
        $meta = $this->adapter
            ->writeStream(
                '/example.json',
                $handle,
                $this->emptyConfig
            );

        $this->assertEquals('application/json', $meta['mimetype']);
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
            ->willReturn(rand(1, 1000));

        $content = base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs='); // 1x1 transparent gif
        $handle = $this->createTempResource($content);
        $meta = $this->adapter
            ->writeStream(
                '/missing-extension',
                $handle,
                $this->emptyConfig
            );

        $this->assertNotEquals('image/gif', $meta['mimetype']);
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

        $this->assertEquals(strlen($content), $meta['size']);
    }

    public function testUpdateWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->update('/some/path/to/file.txt', 'Test Content', $this->emptyConfig);

        $this->assertFalse($meta);
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

        $this->assertEquals('application/json', $meta['mimetype']);
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
                base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs='), // 1x1 trans gif
                $this->emptyConfig
            );

        $this->assertEquals('image/gif', $meta['mimetype']);
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

        $this->assertEquals(strlen($content), $meta['size']);
    }

    public function testUpdateToStreamWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->updateStream('/some/path/to/file.txt', 'Test Content', $this->emptyConfig);

        $this->assertFalse($meta);
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

        $this->assertEquals('application/json', $meta['mimetype']);
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
        $content = base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs='); // 1x1 transparent gif
        $handle = $this->createTempResource($content);
        $meta = $this->adapter
            ->updateStream($path, $handle, $this->emptyConfig);

        $this->assertNotEquals('image/gif', $meta['mimetype']);
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
        $this->assertTrue($actual);
    }

    public function testRenameWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $actual = $this->adapter->rename('/the-old-name.asp', '/the-new-name.php');
        $this->assertFalse($actual);
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
        $this->assertTrue($actual);
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
            ->willReturn(rand(1, 1000));

        $actual = $this->adapter->copy($path, $newpath);
        $this->assertTrue($actual);
    }

    public function testCopyFailsToFindPath(): void
    {
        $path = '/the-old-name.txt';
        $newpath = '/the-new-name.txt';
        $this->setupBasicDbResponse(false);

        $actual = $this->adapter->copy($path, $newpath);
        $this->assertFalse($actual);
    }

    public function testDelete(): void
    {
        $this->setupDbFetchResponse([
            'type' => 'file',
            'path_id' => 123,
        ]);
        $this->assertTrue($this->adapter->delete('/some-file.txt'));
    }

    public function testDeleteWithNonFilePath(): void
    {
        $this->setupDbFetchResponse([
            'type' => 'dir',
            'path_id' => 123,
        ]);
        $this->assertFalse($this->adapter->delete('/some-file.txt'));
    }

    public function testDeleteWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->delete('/some-file.txt'));
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
        $this->assertTrue($this->adapter->deleteDir('/some-directory'));
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
        $this->assertTrue($this->adapter->deleteDir('/some-directory'));
    }

    public function testDeleteDirWithNonDirectoryPath(): void
    {
        $this->setupDbFetchResponse([
            'type' => 'file',
            'path_id' => 123,
        ]);
        $this->assertFalse($this->adapter->deleteDir('/some-file.txt'));
    }

    public function testDeleteDirWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->deleteDir('/some-directory'));
    }

    public function testCreateDir(): void
    {
        $pathId = 12345;
        $this->setupBasicDbResponse();
        $this->pdo->expects($this->any())
            ->method('lastInsertId')
            ->will($this->returnValue($pathId));

        $meta = $this->adapter->createDir('/path', $this->emptyConfig);
        $this->assertEquals($pathId, $meta['path_id']);
    }

    public function testCreateDirWithAdditionalFields(): void
    {
        $pathId = 12345;
        $this->setupBasicDbResponse();
        $this->pdo->expects($this->any())
            ->method('lastInsertId')
            ->will($this->returnValue($pathId));

        $owner = 'exampleFoo';
        $meta = $this->adapter->createDir('/path', new Config([
            'meta' => [
                'owner' => $owner,
            ],
        ]));
        $this->assertArrayHasKey('meta', $meta);
        $this->assertArrayHasKey('owner', $meta['meta']);
        $this->assertEquals($owner, $meta['meta']['owner']);
    }

    public function testCreateDirWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter->createDir('/path', $this->emptyConfig);
        $this->assertFalse($meta);
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
        $this->assertNotFalse($meta);
    }

    public function testSetVisibilityWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->setVisibility('/path/to/file.txt', AdapterInterface::VISIBILITY_PRIVATE);
        $this->assertFalse($meta);
    }

    public function testHas(): void
    {
        $stmt = $this->setupBasicDbResponse();
        $stmt->expects($this->any())
            ->method('fetchColumn')
            ->will($this->returnValue(1));
        $this->assertTrue($this->adapter->has('/this/path'));
    }

    public function testHasWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->has('/this/path'));
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
        $this->assertArrayHasKey('contents', $meta);
    }

    public function testReadWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter->read('/path/to/file.txt');
        $this->assertFalse($meta);
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
        $this->assertTrue(is_resource($meta['stream']));
    }

    public function testReadStreamWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter->readStream('/path/to/file.txt');
        $this->assertFalse($meta);
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
            'fetchAll'
        );
        $listing = $this->adapter->listContents('/path');
        $this->assertCount(1, $listing);
    }

    public function testListContentsWithEmtpyResults(): void
    {
        $this->setupDbFetchResponse([], 'fetchAll');
        $listing = $this->adapter->listContents('/path');
        $this->assertEmpty($listing);
    }

    public function testListContentsWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $listing = $this->adapter->listContents('/path');
        $this->assertEmpty($listing);
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
        $this->assertEmpty($unexpectedKeys);
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

        $this->assertEmpty($unexpectedKeys);
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
        $this->assertEmpty($unexpectedKeys);
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
        $this->assertEmpty($unexpectedKeys);
    }

    public function testGetMetadataWithDbFailure(): void
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->getMetadata('/path'));
    }

    /**
     * @param mixed $expectValue
     * @dataProvider individualAttributeGetMethodsDataProvider
     */
    public function testIndividualAttributeGetMethods(array $rowData, string $attribute, $expectValue): void
    {
        $method = 'get' . ucfirst($attribute);
        $this->setupDbFetchResponse($rowData);
        $this->assertEquals($expectValue, $this->adapter->{$method}($rowData['path'])[$attribute]);
    }

    public function individualAttributeGetMethodsDataProvider(): array
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
     * @return \PDOStatement|\PHPUnit_Framework_MockObject_MockObject
     */
    protected function setupBasicDbResponse($response = true): \PDOStatement
    {
        $stmt = $this->getMock(\PDOStatement::class);
        $stmt->expects($this->any())
            ->method('execute')
            ->will($this->returnValue($response));

        $this->pdo->expects($this->any())
            ->method('prepare')
            ->will($this->returnValue($stmt));

        return $stmt;
    }

    /**
     * @return \PDOStatement|\PHPUnit_Framework_MockObject_MockObject
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
     * @return \PDOStatement|\PHPUnit_Framework_MockObject_MockObject
     */
    public function setupDbMultiCall(array $calls): \PDOStatement
    {
        $stmt = $this->getMock(\PDOStatement::class);
        $stmt->expects($this->any())
            ->method('execute')
            ->will($this->returnValue(true));
        foreach ($calls as $call) {
            $stmt->expects($this->any())
                ->method($call['method'])
                ->will($this->returnValue($call['response']));
        }

        $this->pdo->expects($this->any())
            ->method('prepare')
            ->will($this->returnValue($stmt));

        return $stmt;
    }
}
