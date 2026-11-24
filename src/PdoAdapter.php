<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo;

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
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;

class PdoAdapter implements FilesystemAdapter
{
    protected Config $config;

    protected string $pathTable;

    protected string $chunkTable;

    private MimeTypeDetector $mimeTypeDetector;

    public function __construct(
        protected \PDO $db,
        Config $config = new Config(),
    ) {
        if ($this->db->getAttribute(\PDO::ATTR_ERRMODE) !== \PDO::ERRMODE_EXCEPTION) {
            throw new \InvalidArgumentException(
                'phlib/flysystem-pdo: PDO must use the exception error mode, \PDO::ERRMODE_EXCEPTION',
            );
        }

        $defaultPrefix = 'flysystem';
        $this->config = $config->withDefaults([
            'table_prefix' => $defaultPrefix,
            'enable_compression' => true,
            'chunk_size' => 1048576, // 1MB chunks, in bytes
            'temp_dir' => sys_get_temp_dir(),
            'disable_mysql_buffering' => true,
        ]);

        $tablePrefix = trim($this->config->get('table_prefix'));
        if ($tablePrefix === '') {
            $tablePrefix = $defaultPrefix;
        }
        $this->pathTable = "{$tablePrefix}_path";
        $this->chunkTable = "{$tablePrefix}_chunk";

        if ($this->config->get('disable_mysql_buffering')) {
            $this->db->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        }

        $this->mimeTypeDetector = new FinfoMimeTypeDetector();
    }

    public function write(string $path, string $contents, Config $config): void
    {
        try {
            $this->doWrite($path, $contents, $config);
        } catch (\PDOException $e) {
            throw UnableToWriteFile::atLocation($path, $e->getMessage(), $e);
        }
    }

    /**
     * @param resource $contents
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        try {
            $this->doWrite($path, $contents, $config);
        } catch (\PDOException $e) {
            throw UnableToWriteFile::atLocation($path, $e->getMessage(), $e);
        }
    }

    /**
     * @param string|resource $contents
     */
    protected function doWrite(
        string $path,
        $contents,
        Config $config,
    ): void {
        if ($this->isExpiryInPast($config)) {
            // The file would be immediately unavailable, so don't store it, and remove any existing file
            $this->delete($path);
            return;
        }

        $existing = $this->findRawPathData($path);
        if ($existing && $existing['is_expired']) {
            // The existing file has expired;
            // delete it and continue as a new file, to avoid a conflict with the unique index on the path
            $this->deletePath((int) $existing['path_id']);
            $this->deleteChunks((int) $existing['path_id']);
            $existing = false;
        }
        if ($existing && $existing['type'] !== StorageAttributes::TYPE_FILE) {
            throw UnableToWriteFile::atLocation($path, 'Path already exists as a directory');
        }

        $mimeType = $this->mimeTypeDetector->detectMimeType($path, $contents) ?? 'text/plain';
        $tempFilename = $this->getTempFilename();
        $tempResource = $this->getTempResource($tempFilename, $contents);
        $fileSize = filesize($tempFilename);

        if ($existing !== false) {
            try {
                $this->doUpdate(
                    $existing['path_id'],
                    $tempResource,
                    $config,
                    (bool) $existing['is_compressed'],
                    $mimeType,
                    $fileSize,
                );
            } finally {
                $this->cleanupTemp($tempResource, $tempFilename);
            }
            return;
        }

        $enableCompression = (bool) $config->get('enable_compression', $this->config->get('enable_compression'));
        $expiry = null;
        if ($config->get('expiry')) {
            $expiry = $config->get('expiry');
        }
        $meta = null;
        if ($config->get('meta')) {
            $meta = $config->get('meta');
        }

        try {
            $pathId = $this->insertPath(
                StorageAttributes::TYPE_FILE,
                $path,
                $config->get('visibility', Visibility::PUBLIC),
                $mimeType,
                $fileSize,
                $enableCompression,
                $expiry,
                $meta,
            );
            $this->insertChunks($pathId, $tempResource, $enableCompression);
        } finally {
            $this->cleanupTemp($tempResource, $tempFilename);
        }
    }

    /**
     * @param resource $contents
     */
    protected function doUpdate(
        int $pathId,
        $contents,
        Config $config,
        bool $isCompressed,
        string $mimeType,
        int $fileSize,
    ): void {
        $data = [
            'mimetype' => $mimeType,
            'size' => $fileSize,
        ];

        if ($config->get('expiry')) {
            $data['expiry'] = $config->get('expiry')->format('Y-m-d H:i:s');
        }
        if ($config->get('meta')) {
            $data['meta'] = json_encode($config->get('meta'));
        }

        $setValues = implode(', ', array_map(function ($field) {
            return "{$field} = :{$field}";
        }, array_keys($data)));

        $update = "UPDATE {$this->pathTable} SET {$setValues} WHERE path_id = :path_id";
        $stmt = $this->db->prepare($update);
        $params = array_merge($data, [
            'path_id' => $pathId,
        ]);
        $stmt->execute($params);

        $this->deleteChunks($pathId);
        $this->insertChunks($pathId, $contents, $isCompressed);
    }

    public function move(string $source, string $destination, Config $config): void
    {
        try {
            if ($source === $destination) {
                // Nothing would be updated, so the result cannot confirm the file exists
                if (!$this->fileExists($source)) {
                    throw UnableToMoveFile::fromLocationTo($source, $destination, UnableToReadFile::fromLocation($source));
                }
                return;
            }

            $this->deleteExpiredPath($destination);

            $update = "UPDATE {$this->pathTable} SET path = :destination"
                . " WHERE path = :source AND type = :type AND {$this->notExpiredCondition()}";
            $stmt = $this->db->prepare($update);
            $stmt->execute([
                'destination' => $destination,
                'source' => $source,
                'type' => StorageAttributes::TYPE_FILE,
                ...$this->getExpiredConditionBindParams(),
            ]);

            if ($stmt->rowCount() === 0) {
                // The file does not exist, or has expired
                throw UnableToMoveFile::fromLocationTo($source, $destination, UnableToReadFile::fromLocation($source));
            }
        } catch (\PDOException $e) {
            throw UnableToMoveFile::fromLocationTo($source, $destination, $e);
        }
    }

    public function copy(string $source, string $destination, Config $config): void
    {
        try {
            $data = $this->findPathData($source);
            if (!is_array($data)) {
                throw UnableToCopyFile::fromLocationTo($source, $destination);
            }

            $newData = $data;
            $newData['path'] = $destination;
            unset($newData['path_id']);
            unset($newData['update_ts']);

            $this->deleteExpiredPath($destination);
            $newData['path_id'] = $this->insertPath(
                $data['type'],
                $newData['path'],
                $data['visibility'],
                $data['mimetype'],
                (int) $data['size'],
                (bool) $data['is_compressed'],
                $data['expiry'] ?? null,
                $data['meta'] ?? null,
            );

            if ($newData['type'] === StorageAttributes::TYPE_FILE) {
                $resource = $this->getChunkResource($data['path_id'], (bool) $data['is_compressed']);
                $this->insertChunks($newData['path_id'], $resource, (bool) $data['is_compressed']);
                $this->cleanupTemp($resource, '');
            }
        } catch (\PDOException $e) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $e);
        }
    }

    public function delete(string $path): void
    {
        try {
            $data = $this->findPathData($path);
            if (!is_array($data) || $data['type'] !== StorageAttributes::TYPE_FILE) {
                return;
            }

            $this->deletePath($data['path_id']);
            $this->deleteChunks($data['path_id']);
        } catch (\PDOException $e) {
            throw UnableToDeleteFile::atLocation($path, $e->getMessage(), $e);
        }
    }

    public function deleteDirectory(string $path): void
    {
        try {
            $data = $this->findPathData($path);
            if (!is_array($data) || $data['type'] !== StorageAttributes::TYPE_DIRECTORY) {
                return;
            }

            $select = "SELECT * FROM {$this->pathTable}";

            $path = rtrim($path, '/');
            $select .= " WHERE path = :path OR {$this->descendantCondition()}";
            $params = [
                'path' => $path,
                ...$this->getDescendantConditionBindParams($path),
            ];

            $stmt = $this->db->prepare($select);
            $stmt->execute($params);

            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            foreach ($rows as $row) {
                $this->deletePath((int) $row['path_id']);
                if ($row['type'] === StorageAttributes::TYPE_FILE) {
                    $this->deleteChunks((int) $row['path_id']);
                }
            }
        } catch (\PDOException $e) {
            throw UnableToDeleteDirectory::atLocation($path, $e->getMessage(), $e);
        }
    }

    public function createDirectory(string $path, Config $config): void
    {
        try {
            $existing = $this->findPathData($path);
            if ($existing) {
                if ($existing['type'] !== StorageAttributes::TYPE_DIRECTORY) {
                    throw UnableToCreateDirectory::atLocation($path, 'Path already exists as a file');
                }
                // Creating a directory is idempotent
                return;
            }

            $this->deleteExpiredPath($path);
            $additional = null;
            if ($config->get('meta')) {
                $additional = $config->get('meta');
            }
            $this->insertPath(
                StorageAttributes::TYPE_DIRECTORY,
                $path,
                additional: $additional,
            );
        } catch (\PDOException $e) {
            throw UnableToCreateDirectory::dueToFailure($path, $e);
        }
    }

    public function setVisibility(string $path, string $visibility): void
    {
        try {
            $update = "UPDATE {$this->pathTable} SET visibility = :visibility WHERE path = :path";
            $data = [
                'visibility' => $visibility,
                'path' => $path,
            ];
            $stmt = $this->db->prepare($update);
            $stmt->execute($data);
        } catch (\PDOException $e) {
            throw UnableToSetVisibility::atLocation($path, $e->getMessage(), $e);
        }
    }

    public function fileExists($path): bool
    {
        try {
            $select = "SELECT 1 FROM {$this->pathTable} WHERE path = :path AND type = :type"
                . " AND {$this->notExpiredCondition()} LIMIT 1";
            $stmt = $this->db->prepare($select);
            $stmt->execute([
                'path' => $path,
                'type' => StorageAttributes::TYPE_FILE,
                ...$this->getExpiredConditionBindParams(),
            ]);

            return (bool) $stmt->fetchColumn();
        } catch (\PDOException $e) {
            throw UnableToCheckFileExistence::forLocation($path, $e);
        }
    }

    public function read(string $path): string
    {
        $resource = $this->readStream($path);
        $content = stream_get_contents($resource);
        fclose($resource);

        return $content;
    }

    /**
     * @return resource
     */
    public function readStream(string $path)
    {
        try {
            $data = $this->findPathData($path);
            if (!is_array($data)) {
                throw UnableToReadFile::fromLocation($path);
            }

            return $this->getChunkResource((int) $data['path_id'], (bool) $data['is_compressed']);
        } catch (\PDOException $e) {
            throw UnableToReadFile::fromLocation($path, $e->getMessage(), $e);
        }
    }

    /**
     * @return resource
     */
    private function getChunkResource(int $pathId, bool $isCompressed)
    {
        $resource = fopen('php://temp', 'w+b');
        $compressFilter = null;
        if ($isCompressed) {
            $compressFilter = stream_filter_append($resource, 'zlib.inflate', STREAM_FILTER_WRITE);
        }

        $this->extractChunks($pathId, $resource);

        if (is_resource($compressFilter)) {
            stream_filter_remove($compressFilter);
        }

        return $resource;
    }

    /**
     * @param resource $resource
     */
    protected function extractChunks(int $pathId, $resource): void
    {
        $select = "SELECT content FROM {$this->chunkTable} WHERE path_id = :path_id ORDER BY chunk_no ASC";
        $stmt = $this->db->prepare($select);
        $stmt->execute([
            'path_id' => $pathId,
        ]);
        while ($content = $stmt->fetchColumn()) {
            $contentLength = strlen($content);
            $pointer = 0;
            while ($pointer < $contentLength) {
                $pointer += fwrite($resource, substr($content, $pointer, 1024));
            }
            unset($content);
        }
        rewind($resource);
    }

    /**
     * Directories don't need to have their own entry, as they are implied by the paths of their descendants.
     * All descendants are fetched in a single query, and the direct children (or all, for $deep) are derived from
     * those paths, so that implied directories are listed.
     *
     * @return \Generator<StorageAttributes>
     */
    public function listContents(string $path = '', bool $deep = false): iterable
    {
        $path = rtrim($path, '/');
        $params = $this->getExpiredConditionBindParams();
        $select = "SELECT * FROM {$this->pathTable} WHERE {$this->notExpiredCondition()}";
        if ($path !== '') {
            $select .= " AND {$this->descendantCondition()}";
            $params += $this->getDescendantConditionBindParams($path);
        }
        $select .= ' ORDER BY path';

        $stmt = $this->db->prepare($select);
        $stmt->execute($params);

        // Fetch all rows before yielding: with MySQL buffering disabled, an open result set would prevent the caller
        // from running other queries (e.g. deleting each file) while iterating
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        // Directories which have their own entry, so are not yielded as implied directories
        $directoryEntries = [];
        foreach ($rows as $row) {
            if ($row['type'] === StorageAttributes::TYPE_DIRECTORY) {
                $directoryEntries[$row['path']] = true;
            }
        }

        // Ignore a separator at the start of the path, otherwise the root would be listed as a directory
        $offset = max($path === '' ? 0 : strlen($path) + 1, 1);
        $listedDirectories = [];
        foreach ($rows as $row) {
            // Each separator after the requested path marks an ancestor directory of this row
            $searchFrom = $offset;
            while (($separator = strpos($row['path'], '/', $searchFrom)) !== false) {
                $directory = substr($row['path'], 0, $separator);
                if (!isset($directoryEntries[$directory]) && !isset($listedDirectories[$directory])) {
                    $listedDirectories[$directory] = true;
                    yield new DirectoryAttributes($directory);
                }
                if (!$deep) {
                    // Not a direct child, and only the first level of directories is listed
                    continue 2;
                }
                $searchFrom = $separator + 1;
            }

            if ($row['type'] === StorageAttributes::TYPE_DIRECTORY) {
                // Avoid listing duplicate directory entries
                if (isset($listedDirectories[$row['path']])) {
                    continue;
                }
                $listedDirectories[$row['path']] = true;
            }

            yield $this->mapRowToAttributes($row);
        }
    }

    private function mapRowToAttributes(array $data): StorageAttributes
    {
        $timestamp = strtotime($data['update_ts']);
        if ($this->db->getAttribute(\PDO::ATTR_DRIVER_NAME) === 'sqlite') {
            $timestamp = strtotime($data['update_ts'] . ' +00:00');
        }

        $extraMetadata = null;
        if (isset($data['meta'])) {
            $extraMetadata = json_decode($data['meta'], true);
        }

        if ($data['type'] === StorageAttributes::TYPE_DIRECTORY) {
            return new DirectoryAttributes(
                $data['path'],
                $data['visibility'],
                $timestamp,
                $extraMetadata ?? [],
            );
        }

        $expiry = null;
        if (isset($data['expiry'])) {
            $expiry = new \DateTimeImmutable($data['expiry']);
        }

        return new FileAttributes(
            $data['path'],
            $data['size'],
            $data['visibility'],
            $timestamp,
            $data['mimetype'],
            $expiry,
            $extraMetadata ?? [],
        );
    }

    public function fileSize(string $path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'size');
    }

    public function mimeType(string $path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'mimetype');
    }

    public function lastModified(string $path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'timestamp');
    }

    public function visibility(string $path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'visibility');
    }

    public function expiry(string $path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'expiry');
    }

    public function extra(string $path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'meta');
    }

    protected function findPathData(string $path): array|false
    {
        $select = "SELECT * FROM {$this->pathTable} WHERE path = :path AND {$this->notExpiredCondition()} LIMIT 1";
        $stmt = $this->db->prepare($select);
        $stmt->execute([
            'path' => $path,
            ...$this->getExpiredConditionBindParams(),
        ]);

        return $stmt->fetch(\PDO::FETCH_ASSOC);
    }

    /**
     * Find the path including an expired file, which remains in the DB until it is purged by deleteExpired().
     * The `is_expired` value is set using the same condition as notExpiredCondition().
     */
    private function findRawPathData(string $path): array|false
    {
        $select = "SELECT *, NOT {$this->notExpiredCondition()} AS is_expired"
            . " FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $stmt = $this->db->prepare($select);
        $stmt->execute([
            'path' => $path,
            ...$this->getExpiredConditionBindParams(),
        ]);

        $data = $stmt->fetch(\PDO::FETCH_ASSOC);
        // Close the cursor, as further queries cannot run while an unbuffered MySQL result is still open
        $stmt->closeCursor();
        if ($data !== false) {
            $data['is_expired'] = (bool) $data['is_expired'];
        }

        return $data;
    }

    /**
     * SQL condition to exclude expired paths; requires the params from getExpiredConditionBindParams().
     */
    private function notExpiredCondition(): string
    {
        return '(expiry IS NULL OR expiry > :now)';
    }

    /**
     * The current time is provided by PHP rather than the DB, to avoid any timezone difference.
     */
    private function getExpiredConditionBindParams(): array
    {
        return [
            'now' => date('Y-m-d H:i:s'),
        ];
    }

    /**
     * A file with an expiry in the past would be immediately unavailable, so is not stored
     */
    private function isExpiryInPast(Config $config): bool
    {
        $expiry = $config->get('expiry');
        return $expiry !== null && $expiry <= new \DateTimeImmutable();
    }

    private function getFileMetadataValue(string $path, string $property): FileAttributes
    {
        try {
            $data = $this->findPathData($path);
        } catch (\PDOException $e) {
            throw UnableToRetrieveMetadata::create($path, $property, $e->getMessage(), $e);
        }
        if (!is_array($data) || $data['type'] !== StorageAttributes::TYPE_FILE) {
            throw UnableToRetrieveMetadata::create($path, $property);
        }

        return $this->mapRowToAttributes($data);
    }

    /**
     * @param string $type self::TYPE_FILE or self::TYPE_DIR
     * @param string|null $visibility 'public' or 'private'
     */
    protected function insertPath(
        string $type,
        string $path,
        ?string $visibility = null,
        ?string $mimeType = null,
        ?int $size = null,
        bool $enableCompression = true,
        ?\DateTimeImmutable $expiry = null,
        ?array $additional = null,
    ): int {
        $data = [
            'type' => $type,
            'path' => $path,
            'visibility' => $visibility,
            'mimetype' => $mimeType,
            'size' => $size,
            'is_compressed' => (int) $enableCompression,
        ];
        if ($expiry !== null) {
            $data['expiry'] = $expiry->format('Y-m-d H:i:s');
        }
        if ($additional !== null) {
            $data['meta'] = json_encode($additional);
        }

        $keys = array_keys($data);
        $fields = implode(', ', $keys);
        $values = implode(', ', array_map(function ($field): string {
            return ':' . $field;
        }, $keys));

        $insert = "INSERT INTO {$this->pathTable} ({$fields}) VALUES ({$values})";
        $stmt = $this->db->prepare($insert);
        $stmt->execute($data);

        return (int) $this->db->lastInsertId();
    }

    /**
     * @return int Number of expired files deleted
     */
    public function deleteExpired(\DateTimeImmutable $now = new \DateTimeImmutable()): int
    {
        $select = "SELECT path_id FROM {$this->pathTable} WHERE expiry <= :now";
        $stmt = $this->db->prepare($select);
        $stmt->execute([
            'now' => $now->format('Y-m-d H:i:s'),
        ]);

        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $this->deletePath((int) $row['path_id']);
            $this->deleteChunks((int) $row['path_id']);
        }

        return count($rows);
    }

    /**
     * SQL condition to match all paths below a directory; requires the params from getDescendantConditionBindParams().
     * A range is used rather than `LIKE`, as `LIKE` is case-insensitive in SQLite, and the range can use the path index.
     */
    private function descendantCondition(): string
    {
        return '(path >= :descendant_from AND path < :descendant_to)';
    }

    /**
     * With binary ordering of paths, everything starting with 'dir/' sorts from 'dir/' up to (but excluding) 'dir0',
     * as '0' is the character following '/'
     */
    private function getDescendantConditionBindParams(string $directory): array
    {
        $directory = rtrim($directory, '/');
        return [
            'descendant_from' => $directory . '/',
            'descendant_to' => $directory . '0',
        ];
    }

    /**
     * An expired file remains in the DB until it is purged by deleteExpired(), so it must be deleted before another
     * file can be stored at its path, to avoid a conflict with the unique index on the path
     */
    private function deleteExpiredPath(string $path): void
    {
        $data = $this->findRawPathData($path);
        if ($data && $data['is_expired']) {
            $this->deletePath((int) $data['path_id']);
            $this->deleteChunks((int) $data['path_id']);
        }
    }

    protected function deletePath(int $pathId): void
    {
        $delete = "DELETE FROM {$this->pathTable} WHERE path_id = :path_id";
        $stmt = $this->db->prepare($delete);
        $stmt->execute([
            'path_id' => $pathId,
        ]);
    }

    /**
     * @param resource $resource
     */
    protected function insertChunks(int $pathId, $resource, bool $enableCompression): void
    {
        rewind($resource);

        $compressFilter = null;
        if ($enableCompression) {
            $compressFilter = stream_filter_append($resource, 'zlib.deflate', STREAM_FILTER_READ);
        }

        $insert = "INSERT INTO {$this->chunkTable} (path_id, chunk_no, content) VALUES";
        $insert .= ' (:path_id, :chunk_no, :content)';

        $stmt = $this->db->prepare($insert);
        $chunk = 0;
        $chunkSize = $this->config->get('chunk_size');
        while (!feof($resource)) {
            $content = stream_get_contents($resource, $chunkSize);
            // when an empty stream is compressed it produces \000
            if ($content === '' || bin2hex($content) === '0300') {
                continue;
            }

            $stmt->execute([
                'path_id' => $pathId,
                'chunk_no' => $chunk++,
                'content' => $content,
            ]);
        }

        if (is_resource($compressFilter)) {
            stream_filter_remove($compressFilter);
        }
    }

    protected function deleteChunks(int $pathId): void
    {
        $delete = "DELETE FROM {$this->chunkTable} WHERE path_id = :path_id";
        $stmt = $this->db->prepare($delete);
        $stmt->execute([
            'path_id' => $pathId,
        ]);
    }

    protected function getTempFilename(): string
    {
        $tempDir = $this->config->get('temp_dir');
        return tempnam($tempDir, 'flysystempdo');
    }

    /**
     * @param string|resource $content
     * @return resource
     */
    protected function getTempResource(string $filename, $content)
    {
        $resource = fopen($filename, 'w+b');
        if (!is_resource($content)) {
            fwrite($resource, (string) $content);
        } else {
            while (!feof($content)) {
                fwrite($resource, stream_get_contents($content, 1024), 1024);
            }
        }
        rewind($resource);

        return $resource;
    }

    /**
     * @param resource $resource
     */
    protected function cleanupTemp($resource, string $filename): void
    {
        if (is_resource($resource)) {
            fclose($resource);
        }
        if (is_file($filename)) {
            unlink($filename);
        }
    }
}
