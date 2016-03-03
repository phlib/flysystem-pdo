# Flysystem PDO Adapter

This is a [PDO](http://php.net/manual/en/class.pdo.php) Adapter for the Leagues [Flysystem](http://php.net/manual/en/class.pdo.php).

* Uses multiple tables.
* Stores files in chunks.
* Option to compress the file.
* Supports visibility

## Table Schema

```sql
CREATE TABLE x
```

## Usage
```php
use Phlib\Flysystem\Pdo\PdoAdapter;
use League\Flysystem\Filesystem;

$pdo        = new \PDO('mysql:host=hostname;dbname=database_name', 'username', 'password');
$adapter    = new PdoAdapter($pdo);
$filesystem = new Filesystem($adapter);
```
