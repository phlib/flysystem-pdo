<?php

namespace Phlib\Flysystem\Pdo;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use League\Flysystem\Util;

class PdoAdapter implements AdapterInterface
{
    /**
     * @var \PDO
     */
    protected $db;

    /**
     * @var Config
     */
    protected $config;

    /**
     * @var string
     */
    protected $pathTable;

    /**
     * @var string
     */
    protected $chunkTable;

    /**
     * PdoAdapter constructor.
     * @param \PDO $db
     * @param Config $config
     */
    public function __construct(\PDO $db, Config $config = null)
    {
        $this->db = $db;

        if ($config === null) {
            $config = new Config;
        }
        $defaultPrefix = 'flysystem';
        $config->setFallback(new Config([
            'table_prefix'            => $defaultPrefix,
            'enable_compression'      => true,
            'chunk_size'              => 1048576, // 1MB chunks, in bytes
            'temp_dir'                => sys_get_temp_dir(),
            'disable_mysql_buffering' => true
        ]));
        $this->config = $config;

        $tablePrefix = trim($this->config->get('table_prefix'));
        if ($tablePrefix == '') {
            $tablePrefix = $defaultPrefix;
        }
        $this->pathTable  = "{$tablePrefix}_path";
        $this->chunkTable = "{$tablePrefix}_chunk";

        if ($config->get('disable_mysql_buffering')) {
            $this->db->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        }
    }

    /**
     * @inheritdoc
     */
    public function write($path, $contents, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $contents);

        return $this->doWrite($path, $filename, $contents, $resource, $config);
    }

    /**
     * @inheritdoc
     */
    public function writeStream($path, $resource, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $resource);

        return $this->doWrite($path, $filename, '', $resource, $config);
    }

    /**
     * @param string $path
     * @param string $filename
     * @param string $contents
     * @param resource $resource
     * @param Config $config
     * @return array|false
     */
    protected function doWrite($path, $filename, $contents, $resource, Config $config)
    {
        $enableCompression = (bool)$config->get('enable_compression', $this->config->get('enable_compression'));
        $data              = [
            'path'          => $path,
            'type'          => 'file',
            'mimetype'      => Util::guessMimeType($path, $contents),
            'visibility'    => $config->get('visibility', AdapterInterface::VISIBILITY_PUBLIC),
            'size'          => filesize($filename),
            'is_compressed' => (int)$enableCompression
        ];

        $additionalFields = $this->getConfigAdditionalFieldValues($config);
        $data['path_id'] = $this->insertPath(
            'file',
            $data['path'],
            $data['visibility'],
            $data['mimetype'],
            $data['size'],
            $enableCompression,
            $additionalFields
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
     * @inheritdoc
     */
    public function update($path, $contents, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $contents);

        return $this->doUpdate($path, $filename, $contents, $resource, $config);
    }

    /**
     * @inheritdoc
     */
    public function updateStream($path, $resource, Config $config)
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $resource);

        return $this->doUpdate($path, $filename, '', $resource, $config);
    }

    /**
     * @param string $path
     * @param string $filename
     * @param string $contents
     * @param resource $resource
     * @param Config $config
     * @return array|false
     */
    protected function doUpdate($path, $filename, $contents, $resource, Config $config)
    {
        $data = $this->findPathData($path);
        if (!is_array($data) || $data['type'] != 'file') {
            return false;
        }

        $additionalMeta   = $this->getConfigAdditionalFieldValues($config);
        $data['size']     = filesize($filename);
        $data['mimetype'] = Util::guessMimeType($data['path'], $contents);
        $data             = array_merge($data, $additionalMeta);

        $values = array_intersect_key($data, array_flip(['size', 'mimetype']));
        $setValues = implode(', ', array_map(function ($field) {
            return "{$field} = :{$field}";
        }, array_keys($values)));

        $update = "UPDATE {$this->pathTable} SET {$setValues} WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($update);
        $params = array_merge($values, ['path_id' => $data['path_id']]);
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
     * @param Config $config
     * @return array
     */
    protected function getConfigAdditionalFieldValues(Config $config)
    {
        $additionalFields = $config->get('additional_fields');
        if (!is_array($additionalFields)) {
            return [];
        }

        $meta = [];
        foreach ($additionalFields as $field => $value) {
            $meta[$field] = $value;
        }
        return $meta;
    }

    /**
     * @inheritdoc
     */
    public function rename($path, $newPath)
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $update = "UPDATE {$this->pathTable} SET path = :newpath WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($update);

        // rename the primary node first
        if (!$stmt->execute(['newpath' => $newPath, 'path_id' => $data['path_id']])) {
            return false;
        }

        // rename all children when it's directory
        if ($data['type'] == 'dir') {
            $pathLength = strlen($path);
            $listing    = $this->listContents($path, true);
            foreach ($listing as $item) {
                $newItemPath = $newPath . substr($item['path'], $pathLength);
                $stmt->execute(['newpath' => $newItemPath, 'path_id' => $item['path_id']]);
            }
        }

        $data['path'] = $newPath;
        return $this->normalizeMetadata($data);
    }

    /**
     * @inheritdoc
     */
    public function copy($path, $newPath)
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $newData = $data;
        $newData['path'] = $newPath;
        unset($newData['path_id']);
        unset($newData['update_ts']);
        $additionalFields = $this->getConfigAdditionalFieldValues(new Config($newData));

        $newData['path_id'] = $this->insertPath(
            $data['type'],
            $newData['path'],
            $data['visibility'],
            $data['mimetype'],
            $data['size'],
            $data['is_compressed'],
            $additionalFields
        );

        if ($newData['type'] == 'file') {
            $resource = $this->getChunkResource($data['path_id'], (bool)$data['is_compressed']);
            $this->insertChunks($newData['path_id'], $resource, (bool)$data['is_compressed']);
            $this->cleanupTemp($resource, '');
        }

        $newData['update_ts'] = date('Y-m-d H:i:s');
        return $this->normalizeMetadata($newData);
    }

    /**
     * @inheritdoc
     */
    public function delete($path)
    {
        $data = $this->findPathData($path);
        if (!is_array($data) || $data['type'] != 'file') {
            return false;
        }

        if (!$this->deletePath($data['path_id'])) {
            return false;
        }
        $this->deleteChunks($data['path_id']);

        return true;
    }

    /**
     * @inheritdoc
     */
    public function deleteDir($dirname)
    {
        $data = $this->findPathData($dirname);
        if (!is_array($data) || $data['type'] != 'dir') {
            return false;
        }

        $listing = $this->listContents($dirname, true);

        foreach ($listing as $item) {
            $this->deletePath($item['path_id']);
            if ($item['type'] == 'file') {
                $this->deleteChunks($item['path_id']);
            }
        }

        return $this->deletePath($data['path_id']);
    }

    /**
     * @inheritdoc
     */
    public function createDir($dirname, Config $config)
    {
        $pathId = $this->insertPath('dir', $dirname);
        if ($pathId === false) {
            return false;
        }
        return $this->normalizeMetadata([
            'type'      => 'dir',
            'path'      => $dirname,
            'path_id'   => $pathId,
            'update_ts' => date('Y-m-d H:i:s')
        ]);
    }

    /**
     * @inheritdoc
     */
    public function setVisibility($path, $visibility)
    {
        $update = "UPDATE {$this->pathTable} SET visibility = :visibility WHERE path = :path";
        $data = ['visibility' => $visibility, 'path' => $path];
        $stmt = $this->db->prepare($update);
        if (!$stmt->execute($data)) {
            return false;
        }

        return $this->getMetadata($path);
    }

    /**
     * @inheritdoc
     */
    public function has($path)
    {
        $select = "SELECT 1 FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $stmt   = $this->db->prepare($select);
        if (!$stmt->execute(['path' => $path])) {
            return false;
        }

        return (bool)$stmt->fetchColumn();
    }

    /**
     * @inheritdoc
     */
    public function read($path)
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $resource = $this->getChunkResource($data['path_id'], $data['is_compressed']);

        $metadata = $this->normalizeMetadata($data);
        $metadata['contents'] = stream_get_contents($resource);

        fclose($resource);

        return $metadata;
    }

    /**
     * @inheritdoc
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
     * @param int $pathId
     * @param bool $isCompressed
     * @return resource
     */
    protected function getChunkResource($pathId, $isCompressed)
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
     * @param int $pathId
     * @param resource $resource
     */
    protected function extractChunks($pathId, $resource)
    {
        $select = "SELECT content FROM {$this->chunkTable} WHERE path_id = :path_id ORDER BY chunk_no ASC";
        $stmt   = $this->db->prepare($select);
        $stmt->execute(['path_id' => $pathId]);
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
     * @inheritdoc
     */
    public function listContents($directory = '', $recursive = false)
    {
        $params = [];
        $select = "SELECT * FROM {$this->pathTable}";

        if (!empty($directory)) {
            $select .= " WHERE path LIKE :prefix OR path = :path";
            $params = ['prefix' => $directory . '/%', 'path' => $directory];
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
     * @inheritdoc
     */
    public function getMetadata($path)
    {
        return $this->normalizeMetadata($this->findPathData($path));
    }

    /**
     * @inheritdoc
     */
    public function getSize($path)
    {
        return $this->getFileMetadataValue($path, 'size');
    }

    /**
     * @inheritdoc
     */
    public function getMimetype($path)
    {
        return $this->getFileMetadataValue($path, 'mimetype');
    }

    /**
     * @inheritdoc
     */
    public function getTimestamp($path)
    {
        return $this->getFileMetadataValue($path, 'timestamp');
    }

    /**
     * @inheritdoc
     */
    public function getVisibility($path)
    {
        return $this->getFileMetadataValue($path, 'visibility');
    }

    /**
     * @param string $path
     * @return array|false
     */
    protected function findPathData($path)
    {
        $select = "SELECT * FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $stmt   = $this->db->prepare($select);
        if (!$stmt->execute(['path' => $path])) {
            return false;
        }

        return $stmt->fetch(\PDO::FETCH_ASSOC);
    }

    /**
     * @param array $data
     * @return array|bool
     */
    protected function normalizeMetadata($data)
    {
        if (!is_array($data) || empty($data)) {
            return false;
        }

        $meta = [
            'path_id'   => $data['path_id'],
            'type'      => $data['type'],
            'path'      => $data['path'],
            'timestamp' => strtotime($data['update_ts'])
        ];
        if ($data['type'] == 'file') {
            $meta['mimetype']   = $data['mimetype'];
            $meta['size']       = $data['size'];
            $meta['visibility'] = $data['visibility'];
        }

        $additionalFields = $this->config->get('additional_fields');
        if (is_array($additionalFields)) {
            foreach ($additionalFields as $field) {
                if (isset($data[$field])) {
                    $meta[$field] = $data[$field];
                } else {
                    $meta[$field] = null;
                }
            }
        }

        return $meta;
    }

    /**
     * @param string $path
     * @param string $property
     * @return array|false
     */
    protected function getFileMetadataValue($path, $property)
    {
        $meta = $this->getMetadata($path);
        if ($meta['type'] != 'file' || !isset($meta[$property])) {
            return false;
        }
        return [$property => $meta[$property]];
    }

    /**
     * @param string $type 'file' or 'dir'
     * @param string $path
     * @param string $visibility 'public' or 'private'
     * @param string $mimeType
     * @param int $size
     * @param bool $enableCompression
     * @param array $additionalFields
     * @return bool|string
     */
    protected function insertPath(
        $type,
        $path,
        $visibility = null,
        $mimeType = null,
        $size = null,
        $enableCompression = true,
        $additionalFields = []
    ) {
        $data = array_merge(
            [
                'type'          => $type == 'dir' ? 'dir' : 'file',
                'path'          => $path,
                'visibility'    => $visibility,
                'mimetype'      => $mimeType,
                'size'          => $size,
                'is_compressed' => (int)(bool)$enableCompression
            ],
            $additionalFields
        );

        $keys = array_keys($data);
        $fields = implode(', ', $keys);
        $values = implode(', ', array_map(function ($field) {
            return ':' . $field;
        }, $keys));

        $insert = "INSERT INTO {$this->pathTable} ({$fields}) VALUES ({$values})";
        $stmt   = $this->db->prepare($insert);
        if (!$stmt->execute($data)) {
            return false;
        }

        return $this->db->lastInsertId();
    }

    /**
     * @param int $pathId
     * @return bool
     */
    protected function deletePath($pathId)
    {
        $delete = "DELETE FROM {$this->pathTable} WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($delete);
        return (bool)$stmt->execute(['path_id' => $pathId]);
    }

    /**
     * @param int $pathId
     * @param resource $resource
     * @param bool $enableCompression
     */
    protected function insertChunks($pathId, $resource, $enableCompression)
    {
        rewind($resource);

        $compressFilter = null;
        if ($enableCompression) {
            $compressFilter = stream_filter_append($resource, 'zlib.deflate', STREAM_FILTER_READ);
        }

        $insert = "INSERT INTO {$this->chunkTable} (path_id, chunk_no, content) VALUES";
        $insert .= " (:path_id, :chunk_no, :content)";

        $stmt      = $this->db->prepare($insert);
        $chunk     = 0;
        $chunkSize = $this->config->get('chunk_size');
        while (!feof($resource)) {
            $content = stream_get_contents($resource, $chunkSize);
            // when an empty stream is compressed it produces \000
            if ($content == '' || bin2hex($content) == '0300') {
                continue;
            }
            $stmt->execute([
                'path_id'  => $pathId,
                'chunk_no' => $chunk++,
                'content'  => $content
            ]);
        }

        if (is_resource($compressFilter)) {
            stream_filter_remove($compressFilter);
        }
    }

    /**
     * @param int $pathId
     * @return bool
     */
    protected function deleteChunks($pathId)
    {
        $delete = "DELETE FROM {$this->chunkTable} WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($delete);
        return (bool)$stmt->execute(['path_id' => $pathId]);
    }

    /**
     * @return string
     */
    protected function getTempFilename()
    {
        $tempDir = $this->config->get('temp_dir');
        return tempnam($tempDir, "flysystempdo");
    }

    /**
     * @param string $filename
     * @param string|resource $content
     * @return resource
     */
    protected function getTempResource($filename, $content)
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
     * @param string $filename
     */
    protected function cleanupTemp($resource, $filename)
    {
        if (is_resource($resource)) {
            fclose($resource);
        }
        if (is_file($filename)) {
            unlink($filename);
        }
    }
}
