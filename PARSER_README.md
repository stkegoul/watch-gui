# DSL Parser - New Implementation

This document describes the new parser-based DSL compiler that replaces the regex-based approach.

## Overview

The new DSL parser provides a robust, maintainable, and extensible way to compile watch scripts into executable rules. It addresses the major limitations of the regex-based compiler while maintaining backward compatibility.

## Key Improvements

### 1. **Proper Lexical Analysis**
- **Tokenization**: Input is properly tokenized into meaningful units
- **Context-aware parsing**: Handles nested structures, quotes, and operators correctly
- **Better error detection**: Catches syntax errors early with precise location information

### 2. **Abstract Syntax Tree (AST)**
- **Structured representation**: Rules are parsed into a tree structure
- **Type safety**: Strong typing throughout the parsing process
- **Extensibility**: Easy to add new language features

### 3. **Comprehensive Error Handling**
- **Line and column numbers**: Precise error location reporting
- **Multiple error collection**: Reports all errors found, not just the first one
- **Descriptive messages**: Clear, actionable error messages

### 4. **Performance Improvements**
- **Single-pass parsing**: More efficient than multiple regex passes
- **Reduced memory allocation**: Less string manipulation and temporary objects
- **Cacheable results**: Parsed AST can be cached and reused

## Architecture

```
Input DSL Script
       ↓
   Lexer (Tokenization)
       ↓
   Parser (AST Generation)
       ↓
   AST to Rule Conversion
       ↓
   JSON Output
```

### Components

1. **Lexer** (`dsl-parser.go`): Converts input text into tokens
2. **Parser** (`dsl-parser.go`): Builds AST from tokens using recursive descent
3. **AST Nodes** (`dsl-parser.go`): Represent different parts of the rule
4. **Compiler** (`dsl-parser.go`): Converts AST to executable rule JSON

## Usage

### Basic Usage

```go
import "github.com/blnkledger/plane/pkg/query-agent/watch"

script := `rule HighValueTransaction {
    description "Detect high value transactions"
    when amount > 10000 and metadata.kyc_tier == 1
    then review
    score 0.85
    reason "High value transaction from low KYC tier"
}`

ruleName, description, ruleJSON, err := watch.CompileWatchScriptWithParser(script)
if err != nil {
    log.Fatalf("Compilation failed: %v", err)
}

fmt.Printf("Compiled rule: %s\n", ruleName)
fmt.Printf("JSON: %s\n", ruleJSON)
```

### Error Handling

```go
invalidScript := `rule InvalidRule {
    when amount > 
    then invalid_action
}`

_, _, _, err := watch.CompileWatchScriptWithParser(invalidScript)
if err != nil {
    fmt.Printf("Parse errors:\n%v", err)
    // Output includes line numbers and specific error descriptions
}
```

## Supported Syntax

### Rule Structure

```
rule RuleName {
    description "Optional description"
    when <conditions>
    then <action>
    score <number>
    reason "Optional reason"
}
```

### Conditions

#### Simple Comparisons
```
amount > 1000
metadata.kyc_tier == 1
status != "pending"
```

#### Field Paths
```
metadata.user.profile.tier == "premium"
transaction.details.amount >= 5000
```

#### Variable References
```
source == $current.source
amount > $threshold.high
timestamp <= $current.created_at + PT24H
```

#### Array Operations
```
status in ("pending", "processing", "failed")
metadata.mcc in (7995, 5912, 6051)
```

#### Multiple Conditions
```
when amount > 1000 and metadata.type == "transfer" and status != "completed"
```

### Actions

#### Basic Actions
```
then allow
then block  
then review
then approve
then deny
then alert
```

#### With Score and Reason
```
then review
score 0.85
reason "High risk transaction detected"
```

## Supported Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equal | `status == "completed"` |
| `!=` | Not equal | `type != "internal"` |
| `>` | Greater than | `amount > 1000` |
| `>=` | Greater than or equal | `amount >= 1000` |
| `<` | Less than | `amount < 1000` |
| `<=` | Less than or equal | `amount <= 1000` |
| `in` | In array | `status in ("pending", "failed")` |
| `regex` | Regex match | `description regex "(?i)gift.*card"` |
| `not_regex` | Regex not match | `description not_regex "test.*"` |

## Data Types

### Literals
- **Numbers**: `1000`, `123.45`, `0.5`
- **Strings**: `"hello world"`, `"escaped \"quotes\""`
- **Booleans**: `true`, `false`
- **Arrays**: `("value1", "value2", 123)`

### Variables
- **Current transaction**: `$current.field_name`
- **Thresholds**: `$threshold.high`
- **Custom variables**: `$watchlist_accounts`

## Error Types and Messages

### Lexical Errors
```
unterminated string at line 2, column 15
invalid character '&' at line 3, column 8
```

### Parse Errors
```
expected rule name at line 1, column 6
missing 'when' clause in rule 'TestRule'
invalid verdict: 'invalid_action' at line 4, column 10
```

### Semantic Errors
```
unsupported operator: '~=' at line 2, column 12
invalid field path: '.invalid' at line 3, column 5
```

## Performance Characteristics

### Time Complexity
- **Lexing**: O(n) where n is input length
- **Parsing**: O(n) for most cases, O(n²) worst case for deeply nested expressions
- **AST to JSON**: O(m) where m is number of AST nodes

### Memory Usage
- **Tokens**: ~2x input size during lexing
- **AST**: ~3-4x input size for parsed representation
- **Output**: Minimal additional allocation

### Benchmarks

```bash
go test -bench=. ./pkg/query-agent/watch
```

Example results:
```
BenchmarkLexer-8                     100000    12000 ns/op
BenchmarkParser-8                     50000    25000 ns/op  
BenchmarkCompileWatchScriptWithParser-8  30000    35000 ns/op
```

## Migration Guide

### From Regex Compiler

The new parser is designed to be a drop-in replacement:

```go
// Old way
ruleName, desc, json, err := CompileWatchScript(script)

// New way  
ruleName, desc, json, err := CompileWatchScriptWithParser(script)
```

### Syntax Compatibility

Most existing scripts should work without modification. Key differences:

1. **Stricter validation**: Some previously accepted invalid syntax will now be rejected
2. **Better error messages**: More specific error reporting
3. **Consistent behavior**: Edge cases are handled more predictably

### Breaking Changes

1. **Invalid regex patterns**: Now properly validated at compile time
2. **Malformed strings**: Unterminated strings are now errors
3. **Invalid operators**: Typos in operators are caught immediately

## Testing

### Running Tests

```bash
# All tests
go test ./pkg/query-agent/watch -v

# Specific test categories
go test ./pkg/query-agent/watch -v -run TestLexer
go test ./pkg/query-agent/watch -v -run TestParser
go test ./pkg/query-agent/watch -v -run TestCompileWatchScriptWithParser

# Benchmarks
go test ./pkg/query-agent/watch -bench=.
```

### Test Coverage

- **Lexer tests**: Token recognition, operators, strings, numbers
- **Parser tests**: Rule structure, conditions, actions, error cases
- **Integration tests**: Full compilation, JSON output validation
- **Error tests**: All error conditions with line numbers
- **Benchmark tests**: Performance comparison with old compiler

## Future Enhancements

### Planned Features

1. **Function calls**: `sum(amount, "PT24H") > 10000`
2. **Complex aggregates**: Time-window based calculations
3. **Variables and constants**: Reusable definitions
4. **Imports**: Include other rule files
5. **Comments**: Inline documentation support
6. **Macros**: Reusable rule fragments

### Extensibility

The parser architecture makes it easy to add new features:

1. **New tokens**: Add to `TokenType` enum and lexer
2. **New expressions**: Add AST node types and parser methods
3. **New operators**: Add to operator mapping
4. **New syntax**: Extend parser grammar

## Troubleshooting

### Common Issues

1. **"expected rule name"**: Missing or invalid rule identifier
2. **"unterminated string"**: Missing closing quote
3. **"invalid verdict"**: Typo in action name (allow/block/review)
4. **"unexpected token"**: Syntax error, check operator spelling

### Debug Tips

1. **Check line numbers**: Error messages include precise locations
2. **Validate quotes**: Ensure all strings are properly quoted
3. **Check operators**: Use `==` not `=`, `!=` not `<>`
4. **Verify structure**: Rules must have both `when` and `then` clauses

### Getting Help

1. **Run tests**: `go test -v` to see examples
2. **Check examples**: See `example_usage.go` for working scripts
3. **Enable debug logging**: Set log level to debug for detailed parsing info

## Implementation Details

### Token Types

The lexer recognizes these token types:
- Identifiers and keywords
- String and number literals  
- Operators and delimiters
- Special characters and EOF

### AST Node Types

- `RuleStatement`: Complete rule definition
- `InfixExpression`: Binary operations (field op value)
- `FieldPath`: Dot-separated field access
- `Variable`: $variable references
- `ArrayLiteral`: Array values
- `ActionExpression`: Then clause with verdict/score/reason

### Parser Strategy

Uses recursive descent parsing with:
- Operator precedence handling
- Error recovery mechanisms
- Look-ahead for disambiguation
- Context-sensitive parsing for different rule sections

This new parser provides a solid foundation for the DSL that can grow with future requirements while maintaining excellent performance and error handling.
