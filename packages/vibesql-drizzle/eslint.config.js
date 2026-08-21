import eslint from '@eslint/js'
import tseslint from 'typescript-eslint'

export default [
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  {
    rules: {
      '@typescript-eslint/no-unused-vars': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      'no-useless-catch': 'off',
      'no-case-declarations': 'off',
      'prefer-const': 'off',
    },
  },
  {
    ignores: ['dist/', 'node_modules/'],
  },
]
