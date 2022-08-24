# phlib/flysystem-pdo

[![Code Checks](https://img.shields.io/github/actions/workflow/status/phlib/flysystem-pdo/code-checks.yml?logo=github)](https://github.com/phlib/flysystem-pdo/actions/workflows/code-checks.yml)
[![Codecov](https://img.shields.io/codecov/c/github/phlib/flysystem-pdo.svg?logo=codecov)](https://codecov.io/gh/phlib/flysystem-pdo)
[![Latest Stable Version](https://img.shields.io/packagist/v/phlib/flysystem-pdo.svg?logo=packagist)](https://packagist.org/packages/phlib/flysystem-pdo)
[![Total Downloads](https://img.shields.io/packagist/dt/phlib/flysystem-pdo.svg?logo=packagist)](https://packagist.org/packages/phlib/flysystem-pdo)
![Licence](https://img.shields.io/github/license/phlib/flysystem-pdo.svg)

This is a [PDO](https://www.php.net/manual/en/class.pdo.php) Adapter for the
League's [Flysystem](https://flysystem.thephpleague.com/).

* Uses multiple tables.
* Stores files in chunks.
* Option to compress the file when stored.

This implementation is optimised for use with large files when using the streams. It avoids loading the complete file
into memory, preferring to store files during operation on the local file system.

## Usage
```php
use League\Flysystem\Filesystem;
use Phlib\Flysystem\Pdo\PdoAdapter;

$pdo        = new \PDO('mysql:host=hostname;dbname=database_name', 'username', 'password');
$adapter    = new PdoAdapter($pdo);
$filesystem = new Filesystem($adapter);
```

The PDO connection must use the exception error mode, `\PDO::ERRMODE_EXCEPTION`, which is the default since PHP v8.0.
Database errors are thrown as `\PDOException`.

### Configuration on `write` and `writeStream`

```php
use League\Flysystem\Config;
use League\Flysystem\Visibility;

$config = new Config([
    'enable_compression' => false,
    'visibility'         => Visibility::PUBLIC,
]);
$adapter->writeStream('/path/to/file.zip', $handle, $config);
```

## Adapter Configuration

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
    'disable_mysql_buffering' => true,
]);
$adapter = new PdoAdapter($pdo, $config);
```

## File Configuration

The following optional file configurations supplement the standard behaviour.
The sample schemas include the columns, but they are optional and may be omitted.

|Name|Type|Description|
|----|----|-----------|
|expiry|`\DateTimeImmutable`|Specify an expiry time for the file|
|meta|array|Any additional information. Uses JSON encoding to store the information|

### Expiry
By specifying `expiry` as a configuration parameter when writing a file, the `PdoAdapter`
will store the value in a column called `expiry`. Once the expiry time has passed, the file is treated as though it
does not exist: `fileExists()` returns `false`, reading the file or its metadata throws the relevant Flysystem
exception, and writing to the same path creates a new file.

#### Example

```php
$config = new Config([
    'expiry' => new \DateTimeImmutable('+2 days'),
]);
$adapter->write($path, $content, $config);
```

The expiry is available from the adapter, or from the attributes returned by `listContents()`:

```php
/** @var \Phlib\Flysystem\Pdo\FileAttributes $attributes */
$attributes = $adapter->expiry($path);
$attributes->expiry(); // \DateTimeImmutable|null
```

Expired files remain in the database until they are removed. Use `deleteExpired()` to purge them, for example from a
scheduled task. It returns the number of files deleted.

```php
$deleted = $adapter->deleteExpired();
```

### Additional Metadata
It's possible to store additional metadata about a file or directory. This could include owner, permissions or groups
for example. The information is stored as a JSON encoded string. Once the item is retrieved from the Filesystem the
additional meta information is provided as an array via Flysystem's extra metadata.

#### Example
```php
$config = new Config([
    'meta' => ['owner' => 'John Smith', 'permissions' => 600],
]);
$adapter->write($path, $content, $config);
```

Those details are now part of the file attributes.

```php
$attributes = $adapter->extra($path);
$attributes->extraMetadata();
[
    'owner' => 'John Smith',
    'permissions' => 600,
]
```

## Schema
Schemas can be found in the schema directory. Specific types can be changed based on requirements. All field names
should remain the same. Notes about the DB specific definitions are below.

### Indexes
* `idx_path` is a unique index on the `path` column. Paths are case-sensitive.
* `idx_expiry` is an index on the `expiry` column, used when deleting expired files.

Implementations upgrading from v1 can update existing tables as below.
Any duplicate paths must be removed before the unique index can be added;
these can be found with `SELECT path, COUNT(*) FROM flysystem_path GROUP BY path HAVING COUNT(*) > 1`.

MySQL:
```sql
ALTER TABLE `flysystem_path`
  MODIFY `path` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  ADD UNIQUE KEY `idx_path` (`path`),
  ADD KEY `idx_expiry` (`expiry`);
```

SQLite:
```sql
CREATE UNIQUE INDEX idx_path ON flysystem_path (path);
CREATE INDEX idx_expiry ON flysystem_path (expiry);
```

SQLite index names are unique across the whole database, so when using more than one table prefix in the same
database, the index names must also be changed to include the prefix.

### MySQL Notes
* The `path` column is set to allow up to 255 characters.
* The `path` column uses the `utf8mb4_bin` collation, so that paths are case-sensitive, consistent with SQLite and
most filesystems. The unique index on 255 `utf8mb4` characters requires the `DYNAMIC` (default since MySQL 5.7) or
`COMPRESSED` row format.
* The `size` column has been set to a unsigned `INT` type to allow for convenient searching. This allows up to 4G files 
to be recorded. This can be changed to a `VARCHAR` if searching is not required.
* The size of chunks, allows for up to 16M per chunk.

## Memory Usage (and gotchas)

Any use of `read` or `write` with large files will cause problems with memory usage. The associated stream
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
