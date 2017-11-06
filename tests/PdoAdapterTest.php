<?php

namespace Phlib\Flysystem\Pdo\Tests;

use Phlib\Flysystem\Pdo\PdoAdapter;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;

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

    public function setUp()
    {
        parent::setUp();
        $this->emptyConfig = new Config();
        $this->pdo         = $this->getMock('\Phlib\Flysystem\Pdo\Tests\PdoMock');
        $this->adapter     = new PdoAdapter($this->pdo);
    }

    public function tearDown()
    {
        $this->adapter     = null;
        $this->pdo         = null;
        $this->emptyConfig = null;
        parent::tearDown();
    }

    public function testImplementsAdapterInterface()
    {
        $this->assertInstanceOf('\League\Flysystem\AdapterInterface', $this->adapter);
    }

    public function testTablePrefixDefault()
    {
        $default = 'flysystem';
        $stmt    = $this->getMock('\PDOStatement');
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains($default))
            ->will($this->returnValue($stmt));

        $this->adapter->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testTablePrefixConfigurationIsHonored()
    {
        $prefix = 'myprefix';
        $config = new Config(['table_prefix' => $prefix]);
        $stmt   = $this->getMock('\PDOStatement');
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains($prefix))
            ->will($this->returnValue($stmt));

        (new PdoAdapter($this->pdo, $config))->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testTablePrefixConfigurationWithBlankValue()
    {
        $default = 'flysystem';
        $prefix  = '';
        $config  = new Config(['table_prefix' => $prefix]);
        $stmt    = $this->getMock('\PDOStatement');
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains($default))
            ->will($this->returnValue($stmt));

        (new PdoAdapter($this->pdo, $config))->write('somefile.txt', '', $this->emptyConfig);
    }

    public function testWrite()
    {
        $this->setupBasicDbResponse();

        $path    = '/some/path/to/file.txt';
        $content = 'Test Content';
        $meta    = $this->adapter
            ->write($path, $content, $this->emptyConfig);

        $expected = [
            'type'       => 'file',
            'path'       => $path,
            'visibility' => 'public',
            'size'       => strlen($content),
            'timestamp'  => time(), // this is going to bite me in the arse!
            'path_id'    => null,
            'mimetype'   => 'text/plain'
        ];
        $this->assertEquals($expected, $meta);
    }

    public function testWriteWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->write('/file.txt', null, $this->emptyConfig);
        $this->assertFalse($meta);
    }

    public function testWriteDetectsMimetypeByExtension()
    {
        $this->setupBasicDbResponse();
        $meta = $this->adapter
            ->write(
                '/example.json',
                '',
                $this->emptyConfig
            );

        $this->assertEquals('application/json', $meta['mimetype']);
    }

    public function testWriteDetectsMimetypeByContent()
    {
        $this->setupBasicDbResponse();
        $meta = $this->adapter
            ->write(
                '/missing-extension',
                base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs='), // 1x1 trans gif
                $this->emptyConfig
            );

        $this->assertEquals('image/gif', $meta['mimetype']);
    }

    public function testWriteToStream()
    {
        $this->setupBasicDbResponse();

        $path    = '/some/path/to/file.txt';
        $content = 'Test File';
        $handle  = $this->createTempResource($content);
        $meta    = $this->adapter
            ->writeStream($path, $handle, $this->emptyConfig);

        $expected = [
            'type'       => 'file',
            'path'       => $path,
            'visibility' => 'public',
            'size'       => strlen($content),
            'timestamp'  => time(), // this is going to bite me in the arse!
            'path_id'    => null,
            'mimetype'   => 'text/plain'
        ];
        $this->assertEquals($expected, $meta);
    }

    public function testWriteToStreamWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->writeStream('/file.txt', null, $this->emptyConfig);
        $this->assertFalse($meta);
    }

    public function testWriteToStreamDetectsMimetypeByExtension()
    {
        $this->setupBasicDbResponse();
        $handle = $this->createTempResource();
        $meta   = $this->adapter
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
    public function testWriteToStreamFailsToDetectMimetypeByContent()
    {
        $this->setupBasicDbResponse();
        $content = base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs='); // 1x1 transparent gif
        $handle  = $this->createTempResource($content);
        $meta    = $this->adapter
            ->writeStream(
                '/missing-extension',
                $handle,
                $this->emptyConfig
            );

        $this->assertNotEquals('image/gif', $meta['mimetype']);
    }

    public function testUpdate()
    {
        $path    = '/some/path/to/file.txt';
        $content = 'Test Content';
        $data    = [
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => $path,
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 214454,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);

        $meta = $this->adapter
            ->update($path, $content, $this->emptyConfig);

        $this->assertEquals(strlen($content), $meta['size']);
    }

    public function testUpdateWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->update('/some/path/to/file.txt', 'Test Content', $this->emptyConfig);

        $this->assertFalse($meta);
    }

    public function testUpdateDetectsMimetypeByExtension()
    {
        $path = '/example.json';
        $data = [
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => $path,
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 214454,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);
        $meta = $this->adapter
            ->update($path, '', $this->emptyConfig);

        $this->assertEquals('application/json', $meta['mimetype']);
    }

    public function testUpdateDetectsMimetypeByContent()
    {
        $path = '/missing-extension';
        $data = [
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => $path,
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 214454,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s'),
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

    public function testUpdateToStream()
    {
        $path    = '/some/path/to/file.txt';
        $content = 'Test Content';
        $data    = [
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => $path,
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 214454,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);

        $meta = $this->adapter
            ->updateStream($path, $content, $this->emptyConfig);

        $this->assertEquals(strlen($content), $meta['size']);
    }

    public function testUpdateToStreamWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->updateStream('/some/path/to/file.txt', 'Test Content', $this->emptyConfig);

        $this->assertFalse($meta);
    }

    public function testUpdateToStreamDetectsMimetypeByExtension()
    {
        $path = '/example.json';
        $data = [
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => $path,
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 214454,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);
        $handle = $this->createTempResource();
        $meta   = $this->adapter
            ->updateStream($path, $handle, $this->emptyConfig);

        $this->assertEquals('application/json', $meta['mimetype']);
    }

    public function testUpdateToStreamFailsToDetectsMimetypeByContent()
    {
        $path = '/missing-extension';
        $data = [
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => $path,
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 214454,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s'),
        ];

        $this->setupDbFetchResponse($data);
        $content = base64_decode('R0lGODdhAQABAPAAAP///wAAACwAAAAAAQABAEACAkQBADs='); // 1x1 transparent gif
        $handle  = $this->createTempResource($content);
        $meta    = $this->adapter
            ->updateStream($path, $handle, $this->emptyConfig);

        $this->assertNotEquals('image/gif', $meta['mimetype']);
    }

    public function testRenameFile()
    {
        $path    = '/the-old-name.asp';
        $newName = '/the-new-name.php';
        $this->setupDbFetchResponse([
            'path_id'    => 123,
            'path'       => $path,
            'type'       => 'file',
            'visibility' => 'public',
            'size'       => 2341,
            'update_ts'  => date('Y-m-d H:i:s'),
            'mimetype'   => 'text/plain'
        ]);

        $meta = $this->adapter->rename($path, $newName);
        $this->assertEquals($newName, $meta['path']);
    }

    public function testRenameWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter->rename('/the-old-name.asp', '/the-new-name.php');
        $this->assertFalse($meta);
    }

    public function testRenameDirectory()
    {
        $path    = '/the-old-name';
        $newName = '/the-new-name';
        $this->setupDbMultiCall([
            [
                'method' => 'fetch',
                'response' => [
                    'path_id'    => 123,
                    'path'       => $path,
                    'type'       => 'dir',
                    'update_ts'  => date('Y-m-d H:i:s')
                ]
            ], [
                'method' => 'fetchAll',
                'response' => [
                    [
                        'path_id'    => 1234,
                        'path'       => $path . '/somefile.txt',
                        'type'       => 'file',
                        'visibility' => 'public',
                        'size'       => 2341,
                        'update_ts'  => date('Y-m-d H:i:s'),
                        'mimetype'   => 'text/plain'
                    ]
                ]
            ]
        ]);

        $meta = $this->adapter->rename($path, $newName);
        $this->assertEquals($newName, $meta['path']);
    }

    public function testCopy()
    {
        $path    = '/the-old-name.txt';
        $newpath = '/the-new-name.txt';
        $this->setupDbFetchResponse([
            'path_id'       => 123,
            'path'          => $path,
            'type'          => 'file',
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 2341,
            'is_compressed' => 1,
            'update_ts'     => date('Y-m-d H:i:s')
        ]);

        $meta = $this->adapter->copy($path, $newpath);
        $this->assertEquals($meta['path'], $newpath);
    }

    public function testCopyFailsToFindPath()
    {
        $path    = '/the-old-name.txt';
        $newpath = '/the-new-name.txt';
        $this->setupBasicDbResponse(false);

        $meta = $this->adapter->copy($path, $newpath);
        $this->assertFalse($meta);
    }

    public function testCopyDoesntCopyModifiedTime()
    {
        $time    = strtotime('yesterday');
        $path    = '/the-old-name.txt';
        $newpath = '/the-new-name.txt';
        $this->setupDbFetchResponse([
            'path_id'       => 123,
            'path'          => $path,
            'type'          => 'file',
            'mimetype'      => 'text/plain',
            'visibility'    => 'public',
            'size'          => 2341,
            'is_compressed' => 1,
            'update_ts'     => date('Y-m-d H:i:s', $time)
        ]);

        $meta = $this->adapter->copy($path, $newpath);
        $this->assertGreaterThan($time, $meta['timestamp']);
    }

    public function testDelete()
    {
        $this->setupDbFetchResponse(['type' => 'file', 'path_id' => 123]);
        $this->assertTrue($this->adapter->delete('/some-file.txt'));
    }

    public function testDeleteWithNonFilePath()
    {
        $this->setupDbFetchResponse(['type' => 'dir', 'path_id' => 123]);
        $this->assertFalse($this->adapter->delete('/some-file.txt'));
    }

    public function testDeleteWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->delete('/some-file.txt'));
    }

    public function testDeleteEmptyDirectory()
    {
        $this->setupDbMultiCall([
            [
                'method'   => 'fetch',
                'response' => ['type' => 'dir', 'path_id' => 123]
            ], [
                'method'   => 'fetchAll',
                'response' => []
            ]
        ]);
        $this->assertTrue($this->adapter->deleteDir('/some-directory'));
    }

    public function testDeleteDirectoryWithChildren()
    {
        $this->setupDbMultiCall([
            [
                'method'   => 'fetch',
                'response' => [
                    'type'      => 'dir',
                    'path_id'   => 123,
                    'path'      => '/path/to',
                    'update_ts' => date('Y-m-d H:i:s')
                ]
            ], [
                'method'   => 'fetchAll',
                'response' => [
                    [
                        'type'       => 'file',
                        'path_id'    => 321,
                        'path'       => '/path/to/file.txt',
                        'mimetype'   => 'text/plain',
                        'visibility' => 'public',
                        'size'       => 1234,
                        'update_ts'  => date('Y-m-d H:i:s')
                    ]
                ]
            ]
        ]);
        $this->assertTrue($this->adapter->deleteDir('/some-directory'));
    }

    public function testDeleteDirWithNonDirectoryPath()
    {
        $this->setupDbFetchResponse(['type' => 'file', 'path_id' => 123]);
        $this->assertFalse($this->adapter->deleteDir('/some-file.txt'));
    }

    public function testDeleteDirWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->deleteDir('/some-directory'));
    }

    public function testCreateDir()
    {
        $pathId = 12345;
        $this->setupBasicDbResponse();
        $this->pdo->expects($this->any())
            ->method('lastInsertId')
            ->will($this->returnValue($pathId));

        $meta = $this->adapter->createDir('/path', $this->emptyConfig);
        $this->assertEquals($pathId, $meta['path_id']);
    }

    public function testCreateDirWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter->createDir('/path', $this->emptyConfig);
        $this->assertFalse($meta);
    }

    public function testSetVisibility()
    {
        $path = '/path/to/file.txt';
        $this->setupDbFetchResponse([
            'path_id'    => 123,
            'path'       => $path,
            'type'       => 'file',
            'mimetype'   => 'text/plain',
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            'size'       => 1234,
            'update_ts'  => date('Y-m-d H:i:s')
        ]);
        $meta = $this->adapter
            ->setVisibility('/path/to/file.txt', AdapterInterface::VISIBILITY_PRIVATE);
        $this->assertNotFalse($meta);
    }

    public function testSetVisibilityWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter
            ->setVisibility('/path/to/file.txt', AdapterInterface::VISIBILITY_PRIVATE);
        $this->assertFalse($meta);
    }

    public function testHas()
    {
        $stmt = $this->setupBasicDbResponse();
        $stmt->expects($this->any())
            ->method('fetchColumn')
            ->will($this->returnValue(1));
        $this->assertTrue($this->adapter->has('/this/path'));
    }

    public function testHasWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->has('/this/path'));
    }

    public function testReadReturnsContents()
    {
        $path = '/path/to/file.txt';
        $this->setupDbFetchResponse([
            'path_id'       => 123,
            'path'          => $path,
            'type'          => 'file',
            'mimetype'      => 'text/plain',
            'visibility'    => AdapterInterface::VISIBILITY_PRIVATE,
            'size'          => 1234,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s')
        ]);
        $meta = $this->adapter->read($path);
        $this->assertArrayHasKey('contents', $meta);
    }

    public function testReadWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter->read('/path/to/file.txt');
        $this->assertFalse($meta);
    }

    public function testReadStreamReturnsStream()
    {
        $path = '/path/to/file.txt';
        $this->setupDbFetchResponse([
            'path_id'       => 123,
            'path'          => $path,
            'type'          => 'file',
            'mimetype'      => 'text/plain',
            'visibility'    => AdapterInterface::VISIBILITY_PRIVATE,
            'size'          => 1234,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s')
        ]);
        $meta = $this->adapter->readStream($path);
        $this->assertTrue(is_resource($meta['stream']));
    }

    public function testReadStreamWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $meta = $this->adapter->readStream('/path/to/file.txt');
        $this->assertFalse($meta);
    }

    public function testListContents()
    {
        $this->setupDbFetchResponse(
            [
                [
                    'path_id'       => 123,
                    'path'          => '/path/file.txt',
                    'type'          => 'file',
                    'mimetype'      => 'text/plain',
                    'visibility'    => AdapterInterface::VISIBILITY_PRIVATE,
                    'size'          => 1234,
                    'is_compressed' => false,
                    'update_ts'     => date('Y-m-d H:i:s')
                ]
            ],
            'fetchAll'
        );
        $listing = $this->adapter->listContents('/path');
        $this->assertCount(1, $listing);
    }

    public function testListContentsWithEmtpyResults()
    {
        $this->setupDbFetchResponse([], 'fetchAll');
        $listing = $this->adapter->listContents('/path');
        $this->assertEmpty($listing);
    }

    public function testListContentsWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $listing = $this->adapter->listContents('/path');
        $this->assertEmpty($listing);
    }

    public function testGetMetadataHasCorrectKeysForFile()
    {
        $this->setupDbFetchResponse([
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => '/path/file.txt',
            'mimetype'      => 'text/plain',
            'visibility'    => AdapterInterface::VISIBILITY_PRIVATE,
            'size'          => 1234,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s')
        ]);

        $meta           = $this->adapter->getMetadata('/path/file.txt');
        $expectedKeys   = ['path_id', 'type', 'path', 'mimetype', 'visibility', 'size', 'timestamp'];
        $unexpectedKeys = array_diff_key(array_flip($expectedKeys), $meta);
        $this->assertEmpty($unexpectedKeys);
    }

    public function testGetMetadataNormalizesDataForDirectory()
    {
        $this->setupDbFetchResponse([
            'path_id'       => 123,
            'type'          => 'dir',
            'path'          => '/path/file.txt',
            'mimetype'      => null,
            'visibility'    => null,
            'size'          => null,
            'is_compressed' => 0,
            'update_ts'     => date('Y-m-d H:i:s')
        ]);

        $meta           = $this->adapter->getMetadata('/path/file.txt');
        $expectedKeys   = ['path_id', 'type', 'path', 'timestamp'];
        $unexpectedKeys = array_diff_key(array_flip($expectedKeys), $meta);
        $this->assertEmpty($unexpectedKeys);
    }

    public function testGetMetadataWithDbFailure()
    {
        $this->setupBasicDbResponse(false);
        $this->assertFalse($this->adapter->getMetadata('/path'));
    }

    /**
     * @param array $rowData
     * @param string $attribute
     * @param mixed $expectValue
     * @dataProvider individualAttributeGetMethodsDataProvider
     */
    public function testIndividualAttributeGetMethods(array $rowData, $attribute, $expectValue)
    {
        $method = 'get' . ucfirst($attribute);
        $this->setupDbFetchResponse($rowData);
        $this->assertEquals($expectValue, $this->adapter->$method($rowData['path'])[$attribute]);
    }

    public function individualAttributeGetMethodsDataProvider()
    {
        $size       = 1234;
        $mimetype   = 'text/plain';
        $timestamp  = time();
        $visibility = AdapterInterface::VISIBILITY_PUBLIC;
        $rowData    = [
            'path_id'       => 123,
            'type'          => 'file',
            'path'          => '/path/file.txt',
            'mimetype'      => $mimetype,
            'visibility'    => $visibility,
            'size'          => $size,
            'is_compressed' => false,
            'update_ts'     => date('Y-m-d H:i:s', $timestamp)
        ];
        return [
            [$rowData, 'size', $size],
            [$rowData, 'mimetype', $mimetype],
            [$rowData, 'timestamp', $timestamp],
            [$rowData, 'visibility', $visibility]
        ];
    }

    protected function createTempResource($content = '')
    {
        $handle = fopen('php://temp', 'w+b');
        fwrite($handle, $content);
        rewind($handle);
        return $handle;
    }

    protected function setupBasicDbResponse($response = true)
    {
        $stmt = $this->getMock('\PDOStatement');
        $stmt->expects($this->any())
            ->method('execute')
            ->will($this->returnValue($response));

        $this->pdo->expects($this->any())
            ->method('prepare')
            ->will($this->returnValue($stmt));

        return $stmt;
    }

    protected function setupDbFetchResponse($response, $method = 'fetch')
    {
        return $this->setupDbMultiCall([
            [
                'method'   => $method,
                'response' => $response
            ]
        ]);
    }

    public function setupDbMultiCall($calls)
    {
        $stmt = $this->getMock('\PDOStatement');
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
