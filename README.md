# phlib/flysystem-pdo

[![Build Status](https://img.shields.io/travis/phlib/flysystem-pdo/master.svg)](https://travis-ci.org/phlib/flysystem-pdo)
[![Latest Stable Version](https://img.shields.io/packagist/v/phlib/flysystem-pdo.svg)](https://packagist.org/packages/phlib/flysystem-pdo)
[![Total Downloads](https://img.shields.io/packagist/dt/phlib/flysystem-pdo.svg)](https://packagist.org/packages/phlib/flysystem-pdo)

This is a [PDO](http://php.net/manual/en/class.pdo.php) Adapter for the Leagues [Flysystem](http://php.net/manual/en/class.pdo.php).

* Uses multiple tables.
* Stores files in chunks.
* Option to compress the file when stored.

This implementation is optimised for use with large files when using the streams. It avoids loading the complete file
into memory, preferring to store files during operation on the local file system.

## Usage
```php
use Phlib\Flysystem\Pdo\PdoAdapter;
use League\Flysystem\Filesystem;

$pdo        = new \PDO('mysql:host=hostname;dbname=database_name', 'username', 'password');
$adapter    = new PdoAdapter($pdo);
$filesystem = new Filesystem($adapter);
```

## Configuration

|Name|Type|Default|Description|
|----|----|-------|-----------|
|table_prefix|*String*|`flysystem`|Prepends all tablenames.|
|enable_compression|*Boolean*|`true`|Compresses a file stored in DB.|
|chunk_size|*Integer*|`1,048,576`|Changes the size of file chunks stored. Defaults to 1MB.|
|temp_dir|*String*|`sys_get_temp_dir()`|Location to store temporary files when they're stored and retrieved.|
|enable_mysql_buffering|*Boolean*|`false`|Stops large file results being pulled into memory|
