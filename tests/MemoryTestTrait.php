<?php

declare(strict_types=1);

namespace Phlib\Flysystem\Pdo\Tests;

trait MemoryTestTrait
{
    /**
     * @var string|false
     */
    protected $previousMemoryLimit = false;

    protected function setupMemoryLimit(string $quantity): void
    {
        $this->previousMemoryLimit = false;
        $current = ini_get('memory_limit');
        if ($current !== $quantity) {
            $this->previousMemoryLimit = ini_set('memory_limit', $quantity);
        }
    }

    protected function tearDownMemoryLimit(): void
    {
        if ($this->previousMemoryLimit !== false) {
            ini_set('memory_limit', $this->previousMemoryLimit);
            $this->previousMemoryLimit = false;
        }
    }

    protected function memoryTest(\Closure $unit, int $variation = 2, string $memoryLimit = '250M'): void
    {
        $this->setupMemoryLimit($memoryLimit);

        // convert variation from megabytes to bytes
        $variation = $variation * 1048576;
        if ($variation === PHP_INT_MAX) {
            throw new \InvalidArgumentException('Specified variation exceeds PHP_INT_MAX.');
        }

        $initial = memory_get_peak_usage();
        $unit();
        $final = memory_get_peak_usage();

        $difference = $final - $initial;

        $variationInMeg = round($variation / 1024 / 1024) . 'M';
        $differenceInMeg = round(($difference - $variation) / 1024 / 1024, 1) . 'M';
        $message = "The memory was exceeded by '{$differenceInMeg}' above the '{$variationInMeg}' variation limit.";
        $this->assertLessThanOrEqual($variation, $difference, $message);

        $this->tearDownMemoryLimit();
    }
}
