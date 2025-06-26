# .github/copilot-instructions.md

## Airbnb JavaScript Style Guide for Copilot

Follow these rules for all code generation in this workspace:

### General
- Use `const` for all references; use `let` only if reassignment is needed. Never use `var`.
- Prefer ES6+ features (arrow functions, destructuring, template strings, etc.).
- Use modules (`import`/`export`) instead of CommonJS (`require`/`module.exports`).
- Use array spreads (`...`) to copy arrays and convert iterables to arrays.
- Use object literal syntax for object creation.
- Use computed property names for dynamic object keys.
- Use strict equality (`===` and `!==`) over `==` and `!=`.
- Always use braces for multiline blocks.
- Avoid trailing spaces and multiple empty lines.
- Limit lines to 60 characters for readability.


### Naming
- Use `camelCase` for variables, functions, and instances.
- Use `PascalCase` for classes and constructors.
- Use descriptive names; avoid single-letter names.
- Acronyms and initialisms should be all uppercased or all lowercased.

### Functions
- Use arrow functions where possible.
- Always include parentheses around arrow function arguments.
- Never mutate function parameters.
- Never declare functions inside non-function blocks (e.g., `if`, `while`).
- Use rest syntax (`...args`) instead of `arguments`.
- Default parameters go last.

### Arrays & Objects
- Use array and object destructuring.
- Use `Array.from` for array-like objects.
- Use array methods (`map`, `filter`, `reduce`) for iteration.

### Classes
- Always use `class` syntax; avoid manipulating `prototype` directly.
- Use `extends` for inheritance.
- Class methods should use `this` or be static.

### Comments
- Use `//` for single-line and `/* ... */` for multiline comments.
- Place comments above the code they reference.

### Whitespace
- No spaces inside array brackets; add spaces inside object braces.
- Use consistent spacing in blocks and after keywords.

### Testing
- Write tests for all new code. Strive for high coverage.
- Add regression tests for bug fixes.

---

## Copilot Customization
- Always follow the above style guide for all code generation.
- Prefer modern, readable, and maintainable code.
- Do not use deprecated or experimental JavaScript features.
- Add comments to explain complex logic.
- Don't use code which may not be used in commercial products.

---