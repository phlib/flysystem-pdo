<?php

/**
 * Phlib ECS config v2026-10; ECS >= v13.3
 */

declare(strict_types=1);

use Symplify\EasyCodingStandard\Config\ECSConfig;
use Symplify\EasyCodingStandard\ValueObject\Set\SetList;

return ECSConfig::configure()
    ->withPaths([
        __DIR__ . '/src',
        __DIR__ . '/tests',
    ])
    ->withRootFiles()

    ->withSets([
        SetList::COMMON,
        SetList::PER_CS,
    ])

    ->withRules([
        // Strict type preferences
        \PhpCsFixer\Fixer\Strict\DeclareStrictTypesFixer::class,
        \PhpCsFixer\Fixer\Strict\StrictComparisonFixer::class,
        \PhpCsFixer\Fixer\Strict\StrictParamFixer::class,
    ])

    ->withSkip([
        // Remove sniff, from common/spaces
        \PhpCsFixer\Fixer\Operator\NotOperatorWithSuccessorSpaceFixer::class,
    ]);
