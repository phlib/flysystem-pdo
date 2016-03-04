<?php

namespace Phlib\Flysystem\Pdo\Tests;

use Phlib\Flysystem\Pdo\PdoAdapter;
use League\Flysystem\Config;
use PHPUnit_Extensions_Database_DataSet_IDataSet;
use PHPUnit_Extensions_Database_DB_IDatabaseConnection;
use PHPUnit_Extensions_Database_DataSet_ArrayDataSet as ArrayDataSet;

/**
 * @runTestsInSeparateProcesses
 * @group integration
 */
class IntegrationTest extends \PHPUnit_Extensions_Database_TestCase
{
    /**
     * @var \PDO
     */
    protected static $pdo;

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
    protected $tempFiles = [];

    /**
     * @var array
     */
    protected $tempHandles = [];

    /**
     * @var array
     */
    protected $charRange = [
        0,1,2,3,4,5,6,7,8,9,
        'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',
        '.',' '
    ];

    /**
     * @var string|false
     */
    protected $previousMemoryLimit = false;

    public static function setUpBeforeClass()
    {
        parent::setUpBeforeClass();
        if (isset($GLOBALS['PDO_DSN']) && isset($GLOBALS['PDO_USER']) && isset($GLOBALS['PDO_PASS']) && isset($GLOBALS['PDO_DBNAME'])) {
            static::$pdo = new \PDO($GLOBALS['PDO_DSN'], $GLOBALS['PDO_USER'], $GLOBALS['PDO_PASS']);
        }
    }

    public function setUp()
    {
        if (!static::$pdo instanceof \PDO) {
            $this->markTestSkipped();
            return;
        }

        parent::setUp();

        $this->adapter = new PdoAdapter(static::$pdo);
        $this->emptyConfig = new Config();
    }

    public function tearDown()
    {
        foreach ($this->tempHandles as $tempHandle) {
            if (is_resource($tempHandle)) {
                fclose($tempHandle);
            }
        }
        foreach ($this->tempFiles as $tempFile) {
            if (is_file($tempFile)) {
                unlink($tempFile);
            }
        }

        $this->emptyConfig = null;
        $this->adapter = null;
        parent::tearDown();
    }

    /**
     * @return PHPUnit_Extensions_Database_DB_IDatabaseConnection
     */
    public function getConnection()
    {
        return $this->createDefaultDBConnection(static::$pdo, $GLOBALS['PDO_DBNAME']);
    }

    /**
     * mysqldump -hdhost --xml -t -uroot -p dbname flysystem_chunk flysystem_path > tests/_files/mysql-integration.xml
     * @return PHPUnit_Extensions_Database_DataSet_IDataSet
     * @throws \Exception
     */
    protected function getDataSet()
    {
        $dsn    = $GLOBALS['PDO_DSN'];
        $driver = substr($dsn, 0, strpos($dsn, ':'));
        switch ($driver) {
            case 'mysql':
                $dataSetFile = dirname(__FILE__) . '/_files/mysql-integration.xml';
                return $this->createMySQLXMLDataSet($dataSetFile);
            default:
                throw new \Exception("Missing dataset for '{$driver}'");
        }
    }

    public function testInsertEmptyFile()
    {
        $filename = $this->createTempFilename();
        $handle   = $this->createFile($filename, 0);
        $this->adapter->writeStream('/path/to/file.txt', $handle, $this->emptyConfig);
        $this->assertEquals(0, $this->getConnection()->getRowCount('flysystem_chunk'));
    }

    /**
     * @param callable $fileCallback
     * @param string $writeMethod
     * @param string $readMethod
     * @param Config $config
     * @dataProvider writtenAndReadAreTheSameFileDataProvider
     */
    public function testWrittenAndReadAreTheSameFile($fileCallback, $writeMethod, $readMethod, $config)
    {
        $filename = $this->createTempFilename();
        $file     = call_user_func($fileCallback, $filename, 10);

        $path = '/path/to/file.txt';
        $this->adapter->$writeMethod($path, $file, $config);
        $meta = $this->adapter->$readMethod($path);

        if (is_resource($file)) {
            rewind($file);
            $file = stream_get_contents($file);
        }
        if (isset($meta['stream'])) {
            $meta['contents'] = stream_get_contents($meta['stream']);
        }

        $this->assertEquals($file, $meta['contents']);
    }

    public function writtenAndReadAreTheSameFileDataProvider()
    {
        $compressionConfig  = new Config(['enable_compression' => true]);
        $uncompressedConfig = new Config(['enable_compression' => false]);
        return [
            [[$this, 'createFile'], 'writeStream', 'readStream', $compressionConfig],
            [[$this, 'createFile'], 'writeStream', 'read', $compressionConfig],
            [[$this, 'createFileContent'], 'write', 'readStream', $compressionConfig],
            [[$this, 'createFileContent'], 'write', 'read', $compressionConfig],
            [[$this, 'createFile'], 'writeStream', 'readStream', $uncompressedConfig],
            [[$this, 'createFile'], 'writeStream', 'read', $uncompressedConfig],
            [[$this, 'createFileContent'], 'write', 'readStream', $uncompressedConfig],
            [[$this, 'createFileContent'], 'write', 'read', $uncompressedConfig],
        ];
    }

    public function testCompressionIsUsed()
    {
        $filename = $this->createTempFilename();
        $file     = $this->createFile($filename, 10);
        $path     = '/path/to/file.txt';
        $meta     = $this->adapter->writeStream($path, $file, new Config(['enable_compression' => true]));

        $rows     = [['is_compressed' => 1]];
        $sql      = "SELECT is_compressed FROM flysystem_path WHERE path_id = {$meta['path_id']}";
        $expected = (new ArrayDataSet(['flysystem_path' => $rows]))->getTable('flysystem_path');
        $actual   = $this->getConnection()->createQueryTable('flysystem_path', $sql);

        $this->assertTablesEqual($expected, $actual);
    }

    public function testMemoryUsageOnWritingToStream()
    {
        $this->setupMemoryLimit(-1); // unlimited

        $fileSize = 15 * 1024 * 1024; // 15M
        $filename = $this->createTempFilename();
        $file     = $this->createFile($filename, $fileSize);

        $initial = memory_get_peak_usage(true);
        $path    = '/path/to/file.txt';
        $this->adapter->writeStream($path, $file, $this->emptyConfig);
        $final   = memory_get_peak_usage(true);

        $variation  = 2 * 1024 * 1024; // 2MB variation seems fair game
        $difference = $final - $initial;

        $this->assertLessThanOrEqual($variation, $difference);

        $this->tearDownMemoryLimit();
    }

    public function testMemoryUsageOnReadingFromStream()
    {
        $this->setupMemoryLimit(-1); // unlimited

        $fileSize = 15 * 1024 * 1024; // 15M
        $filename = $this->createTempFilename();
        $file     = $this->createFile($filename, $fileSize);

        $path    = '/path/to/file.txt';
        $this->adapter->writeStream($path, $file, $this->emptyConfig);

        $initial = memory_get_peak_usage(true);
        $this->adapter->readStream($path);
        $final   = memory_get_peak_usage(true);

        $variation  = 2 * 1024 * 1024; // 2MB variation seems fair game
        $difference = $final - $initial;

        $this->assertLessThanOrEqual($variation, $difference);

        $this->tearDownMemoryLimit();
    }

    /**
     * @param array $paths
     * @param int $expectedRows
     * @dataProvider pathsDataProvider
     */
    public function testAddingPaths(array $paths, $expectedRows)
    {
        foreach ($paths as $path) {
            if ($path['type'] == 'dir') {
                $this->adapter->createDir($path['name'], $this->emptyConfig);
            } else {
                $this->adapter->write($path['name'], '', $this->emptyConfig);
            }
        }
        $this->assertEquals($expectedRows, $this->getConnection()->getRowCount('flysystem_path'));
    }

    /**
     * @param array $paths
     * @param int $expectedRows
     * @dataProvider pathsDataProvider
     */
    public function testListContentsMeetsExpectedOutput(array $paths, $expectedRows)
    {
        foreach ($paths as $path) {
            if ($path['type'] == 'dir') {
                $this->adapter->createDir($path['name'], $this->emptyConfig);
            } else {
                $this->adapter->write($path['name'], '', $this->emptyConfig);
            }
        }

        // extraneous data to throw off the listings
        $this->adapter->createDir('/not/this', $this->emptyConfig);
        $this->adapter->createDir('/not/that', $this->emptyConfig);
        $this->adapter->write('/not/this/too.txt', '', $this->emptyConfig);

        $this->assertCount($expectedRows, $this->adapter->listContents('/test', true));
    }

    public function pathsDataProvider()
    {
        $dir1  = ['type' => 'dir', 'name' => '/test'];
        $dir2  = ['type' => 'dir', 'name' => '/test/sub1'];
        $dir3  = ['type' => 'dir', 'name' => '/test/sub2'];
        $file1 = ['type' => 'file', 'name' => '/test/file1.txt'];
        $file2 = ['type' => 'file', 'name' => '/test/file2.txt'];
        $file3 = ['type' => 'file', 'name' => '/test/file3.txt'];

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
            [[$dir1, $dir2, $dir3, $file1, $file2, $file3], 6]
        ];
    }

    public function testDeletingDirectoryClearsAllFiles()
    {
        $this->adapter->createDir('/test', $this->emptyConfig);
        $this->adapter->write('/test/file.txt', '', $this->emptyConfig);

        $this->assertEquals(2, $this->getConnection()->getRowCount('flysystem_path'));
        $this->adapter->deleteDir('/test');
        $this->assertEquals(0, $this->getConnection()->getRowCount('flysystem_path'));
    }

    public function testDeletingFileClearsAllChunks()
    {
        $filename = $this->createTempFilename();
        $file = $this->createFileContent($filename, 2 * 1024 * 1024);
        $this->adapter->write('/test.txt', $file, $this->emptyConfig);

        $this->assertEquals(2, $this->getConnection()->getRowCount('flysystem_chunk'));
        $this->adapter->delete('test.txt');
        $this->assertEquals(0, $this->getConnection()->getRowCount('flysystem_chunk'));
    }

    protected function createTempFilename()
    {
        return sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid('flysystempdo-test-', true);
    }

    protected function createFile($filename, $sizeKb)
    {
        $this->tempFiles[] = $filename;

        if ($sizeKb == 0) {
            touch($filename);
            $handle = fopen($filename, 'wb+');
            $this->tempHandles[] = $handle;
            return $handle;
        }

        $chunkSize    = 1024;
        $charCount    = $sizeKb;
        $randomString = $this->randomString($chunkSize);
        $handle       = fopen($filename, 'wb+');
        for ($i = 0; $i <= $charCount; $i += $chunkSize) {
            fwrite($handle, $randomString, $chunkSize);
        }
        rewind($handle);
        $this->tempHandles[] = $handle;

        return $handle;
    }

    protected function createFileContent($filename, $sizeKb)
    {
        return $this->randomString($sizeKb);
    }

    protected function randomString($length)
    {
        $string = '';
        for ($i = 0; $i < $length; $i++) {
            $string .= $this->charRange[array_rand($this->charRange)];
        }
        return $string;
    }

    /**
     * @param string|int $quantity See PHPs setting memory limit
     */
    protected function setupMemoryLimit($quantity)
    {
        $this->previousMemoryLimit = false;
        $current = ini_get('memory_limit');
        if ($current != $quantity) {
            $this->previousMemoryLimit = ini_set('memory_limit', $quantity);
        }
    }

    protected function tearDownMemoryLimit()
    {
        if ($this->previousMemoryLimit !== false) {
            ini_set('memory_limit', $this->previousMemoryLimit);
            $this->previousMemoryLimit = false;
        }
    }
}
