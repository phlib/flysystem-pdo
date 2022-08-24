<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo;

use League\Flysystem\FileAttributes as LeagueFileAttributes;
use League\Flysystem\StorageAttributes;

class FileAttributes extends LeagueFileAttributes
{
    public const ATTRIBUTE_EXPIRY = 'expiry';

    /**
     * Must not be private, so the parent's ProxyArrayAccessToProperties can access it for `$attributes['expiry']`
     */
    protected ?\DateTimeImmutable $expiry;

    public function __construct(
        string $path,
        ?int $fileSize = null,
        ?string $visibility = null,
        ?int $lastModified = null,
        ?string $mimeType = null,
        ?\DateTimeImmutable $expiry = null,
        array $extraMetadata = [],
    ) {
        parent::__construct($path, $fileSize, $visibility, $lastModified, $mimeType, $extraMetadata);
        $this->expiry = $expiry;
    }

    public function expiry(): ?\DateTimeImmutable
    {
        return $this->expiry;
    }

    public function withExpiry(?\DateTimeImmutable $expiry): self
    {
        $clone = clone $this;
        $clone->expiry = $expiry;

        return $clone;
    }

    public static function fromArray(array $attributes): StorageAttributes
    {
        return new self(
            $attributes[StorageAttributes::ATTRIBUTE_PATH],
            $attributes[StorageAttributes::ATTRIBUTE_FILE_SIZE] ?? null,
            $attributes[StorageAttributes::ATTRIBUTE_VISIBILITY] ?? null,
            $attributes[StorageAttributes::ATTRIBUTE_LAST_MODIFIED] ?? null,
            $attributes[StorageAttributes::ATTRIBUTE_MIME_TYPE] ?? null,
            $attributes[self::ATTRIBUTE_EXPIRY] ?? null,
            $attributes[StorageAttributes::ATTRIBUTE_EXTRA_METADATA] ?? [],
        );
    }

    public function jsonSerialize(): array
    {
        return parent::jsonSerialize() + [
            self::ATTRIBUTE_EXPIRY => $this->expiry,
        ];
    }
}
