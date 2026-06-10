import eslint from '@eslint/js'
import tseslint from 'typescript-eslint'
import prettier from 'eslint-config-prettier'

export default [
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  prettier,
  {
    files: ['src/**/*.ts'],
    rules: {
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
        },
      ],
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/explicit-function-return-type': [
        'warn',
        {
          allowExpressions: true,
        },
      ],
      'no-console': ['warn', { allow: ['warn', 'error'] }],
    },
  },
  {
    // Root-level *.js / *.mjs files are ad-hoc manual debugging scripts
    // (Playwright/site checks run directly via `node`); they are not part of
    // the build or test suite, so they are excluded from linting.
    ignores: ['dist/', 'node_modules/', '*.config.*', 'examples/', 'public/pkg/', '*.js', '*.mjs'],
  },
]
