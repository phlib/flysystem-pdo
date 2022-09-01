<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use League\Flysystem\Util;

class PdoAdapter implements AdapterInterface
{
    private const TYPE_DIRECTORY = 'dir';

    private const TYPE_FILE = 'file';

    protected \PDO $db;

    protected Config $config;

    protected string $pathTable;

    protected string $chunkTable;

    public function __construct(\PDO $db, Config $config = null)
    {
        $this->db = $db;

        if ($config === null) {
            $config = new Config();
        }
        $defaultPrefix = 'flysystem';
        $config->setFallback(new Config([
            'table_prefix' => $defaultPrefix,
            'enable_compression' => true,
            'chunk_size' => 1048576, // 1MB chunks, in bytes
            'temp_dir' => sys_get_temp_dir(),
            'disable_mysql_buffering' => true,
        ]));
        $this->config = $config;

        $tablePrefix = trim($this->config->get('table_prefix'));
        if ($tablePrefix === '') {
            $tablePrefix = $defaultPrefix;
        }
        $this->pathTable = "{$tablePrefix}_path";
        $this->chunkTable = "{$tablePrefix}_chunk";

        if ($config->get('disable_mysql_buffering')) {
            $this->db->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        }
    }

    /**
     * @param string $path
     * @param string $contents
     * @return array|false false on failure file meta data on success
     */
    public function write($path, $contents, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $contents);

        return $this->doWrite($path, $filename, $contents, $resource, $config);
    }

    /**
     * @param string   $path
     * @param resource $resource
     * @return array|false false on failure file meta data on success
     */
    public function writeStream($path, $resource, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $resource);

        return $this->doWrite($path, $filename, '', $resource, $config);
    }

    /**
     * @param resource $resource
     * @return array|false
     */
    protected function doWrite(string $path, string $filename, string $contents, $resource, Config $config)
    {
        $enableCompression = (bool)$config->get('enable_compression', $this->config->get('enable_compression'));
        $data = [
            'path' => $path,
            'type' => self::TYPE_FILE,
            'mimetype' => Util::guessMimeType($path, $contents),
            'visibility' => $config->get('visibility', AdapterInterface::VISIBILITY_PUBLIC),
            'size' => filesize($filename),
            'is_compressed' => (int)$enableCompression,
        ];
        $expiry = null;
        if ($config->has('expiry')) {
            $expiry = $data['expiry'] = $config->get('expiry');
        }
        $meta = null;
        if ($config->has('meta')) {
            $meta = $config->get('meta');
            $data['meta'] = json_encode($meta);
        }

        $data['path_id'] = $this->insertPath(
            self::TYPE_FILE,
            $data['path'],
            $data['visibility'],
            $data['mimetype'],
            $data['size'],
            $enableCompression,
            $expiry,
            $meta
        );
        if ($data['path_id'] === false) {
            $this->cleanupTemp($resource, $filename);
            return false;
        }

        $this->insertChunks($data['path_id'], $resource, $enableCompression);
        $this->cleanupTemp($resource, $filename);

        $data['update_ts'] = date('Y-m-d H:i:s');
        return $this->normalizeMetadata($data);
    }

    /**
     * @param string $path
     * @param string $contents
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $contents);

        return $this->doUpdate($path, $filename, $contents, $resource, $config);
    }

    /**
     * @param string   $path
     * @param resource $resource
     * @return array|false false on failure file meta data on success
     */
    public function updateStream($path, $resource, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $resource);

        return $this->doUpdate($path, $filename, '', $resource, $config);
    }

    /**
     * @param resource $resource
     * @return array|false
     */
    protected function doUpdate(string $path, string $filename, string $contents, $resource, Config $config)
    {
        $data = $this->findPathData($path);
        if (!is_array($data) || $data['type'] !== self::TYPE_FILE) {
            return false;
        }

        $searchKeys = ['size', 'mimetype'];
        $data['size'] = filesize($filename);
        $data['mimetype'] = Util::guessMimeType($data['path'], $contents);
        if ($config->has('expiry')) {
            $data['expiry'] = $config->get('expiry');
            $searchKeys[] = 'expiry';
        }
        if ($config->has('meta')) {
            $data['meta'] = json_encode($config->get('meta'));
            $searchKeys[] = 'meta';
        }

        $values = array_intersect_key($data, array_flip($searchKeys));
        $setValues = implode(', ', array_map(function ($field) {
            return "{$field} = :{$field}";
        }, array_keys($values)));

        $update = "UPDATE {$this->pathTable} SET {$setValues} WHERE path_id = :path_id";
        $stmt = $this->db->prepare($update);
        $params = array_merge($values, [
            'path_id' => $data['path_id'],
        ]);
        if (!$stmt->execute($params)) {
            return false;
        }

        $this->deleteChunks($data['path_id']);
        $this->insertChunks($data['path_id'], $resource, (bool)$data['is_compressed']);
        $this->cleanupTemp($resource, $filename);

        $data['update_ts'] = date('Y-m-d H:i:s');
        return $this->normalizeMetadata($data);
    }

    /**
     * @param string $path
     * @param string $newpath
     */
    public function rename($path, $newpath): bool
    {
        $update = "UPDATE {$this->pathTable} SET path = :newpath WHERE path_id = :path_id";
        $stmt = $this->db->prepare($update);

        // rename the primary node first
        $data = $this->findPathData($path);
        if (is_array($data)) {
            $result = $stmt->execute([
                'newpath' => $newpath,
                'path_id' => $data['path_id'],
            ]);
            if (!$result) {
                return false;
            }
        }

        // rename all children when it's directory; it may be a directory if no record was found for exact match
        if ($data === false || $data['type'] === self::TYPE_DIRECTORY) {
            $pathLength = strlen($path);
            $listing = $this->listContents($path, true);
            if ($data === false && empty($listing)) {
                // No exact match, no children => it doesn't exist
                return false;
            }
            foreach ($listing as $item) {
                $newItemPath = $newpath . substr($item['path'], $pathLength);
                $stmt->execute([
                    'newpath' => $newItemPath,
                    'path_id' => $item['path_id'],
                ]);
            }
        }

        return true;
    }

    /**
     * @param string $path
     * @param string $newpath
     */
    public function copy($path, $newpath): bool
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $newData = $data;
        $newData['path'] = $newpath;
        unset($newData['path_id']);
        unset($newData['update_ts']);

        $newData['path_id'] = $this->insertPath(
            $data['type'],
            $newData['path'],
            $data['visibility'],
            $data['mimetype'],
            (int)$data['size'],
            (bool)$data['is_compressed'],
            $data['expiry'] ?? null,
            $data['meta'] ?? null
        );

        if ($newData['type'] === self::TYPE_FILE) {
            $resource = $this->getChunkResource($data['path_id'], (bool)$data['is_compressed']);
            $this->insertChunks($newData['path_id'], $resource, (bool)$data['is_compressed']);
            $this->cleanupTemp($resource, '');
        }

        return true;
    }

    /**
     * @param string $path
     */
    public function delete($path): bool
    {
        $data = $this->findPathData($path);
        if (!is_array($data) || $data['type'] !== self::TYPE_FILE) {
            return false;
        }

        if (!$this->deletePath($data['path_id'])) {
            return false;
        }
        $this->deleteChunks($data['path_id']);

        return true;
    }

    /**
     * @param string $dirname
     */
    public function deleteDir($dirname): bool
    {
        $data = $this->findPathData($dirname);
        if (!is_array($data) || $data['type'] !== self::TYPE_DIRECTORY) {
            return false;
        }

        $listing = $this->listContents($dirname, true);

        foreach ($listing as $item) {
            $this->deletePath($item['path_id']);
            if ($item['type'] === self::TYPE_FILE) {
                $this->deleteChunks($item['path_id']);
            }
        }

        return $this->deletePath($data['path_id']);
    }

    /**
     * @param string $dirname directory name
     * @return array|false
     */
    public function createDir($dirname, Config $config)
    {
        $additional = null;
        if ($config->has('meta')) {
            $additional = $config->get('meta');
        }
        $pathId = $this->insertPath(
            self::TYPE_DIRECTORY,
            $dirname,
            null,
            null,
            null,
            true,
            null,
            $additional,
        );
        if ($pathId === false) {
            return false;
        }

        $data = [
            'type' => self::TYPE_DIRECTORY,
            'path' => $dirname,
            'path_id' => $pathId,
            'update_ts' => date('Y-m-d H:i:s'),
        ];
        if ($additional !== null) {
            $data['meta'] = json_encode($additional);
        }
        return $this->normalizeMetadata($data);
    }

    /**
     * @param string $path
     * @param string $visibility
     * @return array|false file meta data
     */
    public function setVisibility($path, $visibility)
    {
        $update = "UPDATE {$this->pathTable} SET visibility = :visibility WHERE path = :path";
        $data = [
            'visibility' => $visibility,
            'path' => $path,
        ];
        $stmt = $this->db->prepare($update);
        if (!$stmt->execute($data)) {
            return false;
        }

        return $this->getMetadata($path);
    }

    /**
     * @param string $path
     */
    public function has($path): bool
    {
        $select = "SELECT 1 FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $stmt = $this->db->prepare($select);
        $result = $stmt->execute([
            'path' => $path,
        ]);
        if (!$result) {
            return false;
        }

        return (bool)$stmt->fetchColumn();
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function read($path)
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $metadata = $this->normalizeMetadata($data);

        if ($data['type'] === self::TYPE_FILE) {
            $resource = $this->getChunkResource($data['path_id'], (bool)$data['is_compressed']);
            $metadata['contents'] = stream_get_contents($resource);
            fclose($resource);
        }

        return $metadata;
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function readStream($path)
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $metadata = $this->normalizeMetadata($data);
        $metadata['stream'] = $this->getChunkResource($metadata['path_id'], (bool)$data['is_compressed']);
        return $metadata;
    }

    /**
     * @return resource
     */
    protected function getChunkResource(int $pathId, bool $isCompressed)
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
     * @param string $directory
     * @param bool   $recursive
     */
    public function listContents($directory = '', $recursive = false): array
    {
        $params = [];
        $select = "SELECT * FROM {$this->pathTable}";

        if (!empty($directory)) {
            $select .= ' WHERE path LIKE :prefix OR path = :path';
            $params = [
                'prefix' => $directory . '/%',
                'path' => $directory,
            ];
        }

        $stmt = $this->db->prepare($select);
        if (!$stmt->execute($params)) {
            return [];
        }

        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $rows = array_map([$this, 'normalizeMetadata'], $rows);
        if ($recursive) {
            $rows = Util::emulateDirectories($rows);
        }
        return $rows;
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function getMetadata($path)
    {
        return $this->normalizeMetadata($this->findPathData($path));
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function getSize($path)
    {
        return $this->getFileMetadataValue($path, 'size');
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function getMimetype($path)
    {
        return $this->getFileMetadataValue($path, 'mimetype');
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function getTimestamp($path)
    {
        return $this->getFileMetadataValue($path, 'timestamp');
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function getVisibility($path)
    {
        return $this->getFileMetadataValue($path, 'visibility');
    }

    /**
     * @return array|false
     */
    protected function findPathData(string $path)
    {
        $select = "SELECT * FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $stmt = $this->db->prepare($select);
        $result = $stmt->execute([
            'path' => $path,
        ]);
        if (!$result) {
            return false;
        }

        $data = $stmt->fetch(\PDO::FETCH_ASSOC);
        if ($data === false || $this->hasExpired($data)) {
            return false;
        }

        $data['path_id'] = (int)$data['path_id'];

        return $data;
    }

    /**
     * @param array|bool $data
     * @return array|bool
     */
    protected function normalizeMetadata($data)
    {
        if (!is_array($data) || empty($data) || $this->hasExpired($data)) {
            return false;
        }

        $meta = [
            'path_id' => (int)$data['path_id'],
            'type' => $data['type'],
            'path' => $data['path'],
            'timestamp' => strtotime($data['update_ts']),
        ];
        if ($this->db->getAttribute(\PDO::ATTR_DRIVER_NAME) === 'sqlite') {
            $meta['timestamp'] = strtotime($data['update_ts'] . ' +00:00');
        }
        if ($data['type'] === self::TYPE_FILE) {
            $meta['mimetype'] = $data['mimetype'];
            $meta['size'] = (int)$data['size'];
            $meta['visibility'] = $data['visibility'];
            if (isset($data['expiry'])) {
                $meta['expiry'] = $data['expiry'];
            }
        }

        if (isset($data['meta'])) {
            $meta['meta'] = json_decode($data['meta'], true);
        }

        return $meta;
    }

    protected function hasExpired(array $data): bool
    {
        if (isset($data['expiry']) &&
            !empty($data['expiry']) &&
            strtotime($data['expiry']) !== false &&
            strtotime($data['expiry']) <= time()
        ) {
            return true;
        }

        return false;
    }

    /**
     * @return array|false
     */
    protected function getFileMetadataValue(string $path, string $property)
    {
        $meta = $this->getMetadata($path);
        if ($meta['type'] !== self::TYPE_FILE || !isset($meta[$property])) {
            return false;
        }
        return [
            $property => $meta[$property],
        ];
    }

    /**
     * @param string $type self::TYPE_FILE or self::TYPE_DIR
     * @param string|null $visibility 'public' or 'private'
     * @return int|false
     */
    protected function insertPath(
        string $type,
        string $path,
        string $visibility = null,
        string $mimeType = null,
        int $size = null,
        bool $enableCompression = true,
        string $expiry = null,
        array $additional = null
    ) {
        $data = [
            'type' => $type === self::TYPE_DIRECTORY ? self::TYPE_DIRECTORY : self::TYPE_FILE,
            'path' => $path,
            'visibility' => $visibility,
            'mimetype' => $mimeType,
            'size' => $size,
            'is_compressed' => (int)$enableCompression,
        ];
        if ($expiry !== null) {
            $data['expiry'] = $expiry;
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
        if (!$stmt->execute($data)) {
            return false;
        }

        return (int)$this->db->lastInsertId();
    }

    /**
     * @param string|null $now Timestamp in expected format for query
     * @return int Number of expired files deleted
     */
    public function deleteExpired(?string $now = null): int
    {
        if ($now === null) {
            $now = date('Y-m-d H:i:s');
        }

        $select = "SELECT path_id FROM {$this->pathTable} WHERE expiry <= :now";
        $stmt = $this->db->prepare($select);
        $stmt->execute([
            'now' => $now,
        ]);

        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $this->deletePath((int)$row['path_id']);
        }

        return $stmt->rowCount();
    }

    protected function deletePath(int $pathId): bool
    {
        $delete = "DELETE FROM {$this->pathTable} WHERE path_id = :path_id";
        $stmt = $this->db->prepare($delete);
        return $stmt->execute([
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

    protected function deleteChunks(int $pathId): bool
    {
        $delete = "DELETE FROM {$this->chunkTable} WHERE path_id = :path_id";
        $stmt = $this->db->prepare($delete);
        return $stmt->execute([
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
            fwrite($resource, (string)$content);
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
