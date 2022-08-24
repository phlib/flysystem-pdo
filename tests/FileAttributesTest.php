<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\StorageAttributes;
use Phlib\Flysystem\Pdo\FileAttributes;
use PHPUnit\Framework\TestCase;

class FileAttributesTest extends TestCase
{
    public function testIsFileAttributes(): void
    {
        $attributes = new FileAttributes('path/file.txt');

        static::assertInstanceOf(FileAttributes::class, $attributes);
        static::assertTrue($attributes->isFile());
    }

    public function testExpiryDefaultsToNull(): void
    {
        $attributes = new FileAttributes('path/file.txt');

        static::assertNull($attributes->expiry());
        static::assertFalse(isset($attributes['expiry']));
    }

    public function testExpiry(): void
    {
        $expiry = new \DateTimeImmutable('+1 day');
        $attributes = new FileAttributes('path/file.txt', 123, 'public', 1000, 'text/plain', $expiry, [
            'foo' => 'bar',
        ]);

        static::assertSame($expiry, $attributes->expiry());
        static::assertSame($expiry, $attributes['expiry']);
        static::assertSame(123, $attributes->fileSize());
        static::assertSame('public', $attributes->visibility());
        static::assertSame(1000, $attributes->lastModified());
        static::assertSame('text/plain', $attributes->mimeType());
        static::assertSame([
            'foo' => 'bar',
        ], $attributes->extraMetadata());
    }

    public function testWithExpiry(): void
    {
        $expiry = new \DateTimeImmutable('+1 day');
        $original = new FileAttributes('path/file.txt');
        $modified = $original->withExpiry($expiry);

        static::assertNotSame($original, $modified);
        static::assertNull($original->expiry());
        static::assertSame($expiry, $modified->expiry());
    }

    public function testFromArrayAndJsonSerialize(): void
    {
        $expiry = new \DateTimeImmutable('+1 day');
        $array = [
            StorageAttributes::ATTRIBUTE_TYPE => StorageAttributes::TYPE_FILE,
            StorageAttributes::ATTRIBUTE_PATH => 'path/file.txt',
            StorageAttributes::ATTRIBUTE_FILE_SIZE => 123,
            StorageAttributes::ATTRIBUTE_VISIBILITY => 'public',
            StorageAttributes::ATTRIBUTE_LAST_MODIFIED => 1000,
            StorageAttributes::ATTRIBUTE_MIME_TYPE => 'text/plain',
            StorageAttributes::ATTRIBUTE_EXTRA_METADATA => [],
            FileAttributes::ATTRIBUTE_EXPIRY => $expiry,
        ];

        $attributes = FileAttributes::fromArray($array);

        static::assertInstanceOf(FileAttributes::class, $attributes);
        static::assertSame($expiry, $attributes->expiry());
        static::assertSame($array, $attributes->jsonSerialize());
    }
}
