// ESLint flat config (ESLint 9+/10).
// The project is CommonJS, so this file uses require()/module.exports.
const js = require('@eslint/js');
const tseslint = require('typescript-eslint');

module.exports = tseslint.config(
  {
    // Build output, coverage and vendored examples are not linted.
    ignores: ['dist/**', 'coverage/**', 'node_modules/**', 'examples/**'],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['**/*.ts'],
    rules: {
      // The public EventStore API is untyped in several places; keep `any`
      // allowed rather than sprinkling disable comments through the source.
      '@typescript-eslint/no-explicit-any': 'off',
      // Allow intentionally unused params/prefixed variables.
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
    },
  },
);
