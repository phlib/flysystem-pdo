<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

use League\Flysystem\AdapterTestUtilities\FilesystemAdapterTestCase;
use Phlib\Flysystem\Pdo\PdoAdapter;
use PHPUnit\Framework\Attributes\Group;

#[Group('integration')]
class IntegrationFlysystemTest extends FilesystemAdapterTestCase
{
    use IntegrationAdapterTrait;

    protected function setUp(): void
    {
        if (!getenv('INTEGRATION_ENABLED')) {
            static::markTestSkipped();
        }

        parent::setUp();
    }

    protected static function createFilesystemAdapter(): \League\Flysystem\FilesystemAdapter
    {
        self::setUpAdapter();

        return new PdoAdapter(static::getTestDbAdapter());
    }
}
