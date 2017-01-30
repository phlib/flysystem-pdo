# phlib/flysystem-pdo

[![Build Status](https://img.shields.io/travis/phlib/flysystem-pdo/master.svg)](https://travis-ci.org/phlib/flysystem-pdo)
[![Codecov](https://img.shields.io/codecov/c/github/phlib/flysystem-pdo.svg)](https://codecov.io/gh/phlib/flysystem-pdo)
[![Latest Stable Version](https://img.shields.io/packagist/v/phlib/flysystem-pdo.svg)](https://packagist.org/packages/phlib/flysystem-pdo)
[![Total Downloads](https://img.shields.io/packagist/dt/phlib/flysystem-pdo.svg)](https://packagist.org/packages/phlib/flysystem-pdo)
![Licence](https://img.shields.io/github/license/phlib/flysystem-pdo.svg)

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

### Configuration on `write` and `writeStream`

```php
use League\Flysystem\Config;

$config = new Config([
    'enable_compression' => false,
    'visibility'         => AdapterInterface::VISIBILITY_PUBLIC
]); 
$adapter->writeStream('/path/to/file.zip', $handle, $config);
```

## Configuration

|Name|Type|Default|Description|
|----|----|-------|-----------|
|table_prefix|*String*|`flysystem`|Prepends all tablenames.|
|enable_compression|*Boolean*|`true`|Compresses a file stored in DB.|
|chunk_size|*Integer*|`1,048,576`|Changes the size of file chunks stored in bytes. Defaults to 1MB.|
|temp_dir|*String*|`sys_get_temp_dir()`|Location to store temporary files when they're stored and retrieved.|
|disable_mysql_buffering|*Boolean*|`true`|Stops large file results being pulled into memory|

### Example

```php
use League\Flysystem\Config;

$config = new Config([
    'table_prefix'            => 'flysystem',
    'enable_compression'      => true,
    'chunk_size'              => 1048576,
    'temp_dir'                => '/var/tmp',
    'disable_mysql_buffering' => true
]);
$adapter = new PdoAdapter($pdo, $config);
```

## Schema
Schemas can be found in the schema directory. Specific types can be changed based on requirements. All field names 
should remain the same. Notes about the DB specific definitions are below.

### MySQL Notes
* The `path` column is set to allow up to 255 characters.
* The `size` column has been set to a unsigned `INT` type to allow for convenient searching. This allows up to 4G files 
to be recorded. This can be changed to a `VARCHAR` if searching is not required.
* The size of chunks, allows for up to 16M per chunk.


## Memory Usage (and gotchas)

Any use of `read`, `write` or `update` with large files will cause problems with memory usage. The associated stream 
methods have been optimised to use as little memory as possible. The adapter first saves the file to the local 
filesystem before transferring it to the database.

### Buffering

On MySQL, the default behaviour is to buffer all query results. When reading a file back from the database this could 
cause memory problems. There is a configuration option which disables the buffering. This has the side effect that the 
pdo connection specified in the constructor is altered to set this attribute.

### Compression

Compression is especially useful when storing text based files. The compression option defaults to on. The side effect 
of this is that when reading files back some files may cause larger than expected memory usage. As an example, a very 
large file filled with a single letter 'a', can be compressed to a tiny size. When that file is read, the tiny chunk is 
expanded and will fill the memory.

When a file is stored, the setting for compression is stored with it. This can not be changed.

## Chunking

Chunking has been implemented to aid where systems have been set up for replication. Packet sizes are a consideration 
here.

## License

This package is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
