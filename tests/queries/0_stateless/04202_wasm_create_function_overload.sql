-- Tags: no-fasttest, no-parallel, no-msan

-- Tests that plain CREATE FUNCTION (throw_if_exists=true) correctly checks
-- the specific overload signature, not just the function name.
-- Without the fix, registering a second overload with a different signature
-- would fail with FUNCTION_ALREADY_EXISTS because has(name) returned true.

SET allow_experimental_analyzer = 1;

-- Cleanup at start (functions and modules persist on disk across test runs)
DROP FUNCTION IF EXISTS wasm_mul;
DELETE FROM system.webassembly_modules WHERE name = 'mul_i32_module';
DELETE FROM system.webassembly_modules WHERE name = 'mul_i64_module';

-- (i32, i32) -> i32 multiply module
INSERT INTO system.webassembly_modules (name, code)
    SELECT 'mul_i32_module', base64Decode('AGFzbQEAAAABBwFgAn9/AX8DAgEABwcBA211bAAACgkBBwAgACABags=');

-- (i64, i64) -> i64 multiply module
INSERT INTO system.webassembly_modules (name, code)
    SELECT 'mul_i64_module', base64Decode('AGFzbQEAAAABBwFgAn5+AX4DAgEABwcBA211bAAACgkBBwAgACABfAs=');

-- Create first overload with plain CREATE FUNCTION (throw_if_exists=true)
-- This should succeed — no overloads exist yet.
CREATE FUNCTION wasm_mul LANGUAGE WASM ABI ROW_DIRECT FROM 'mul_i32_module' :: 'mul' ARGUMENTS (Int32, Int32) RETURNS Int32;
SELECT wasm_mul(toInt32(3), toInt32(4));

-- Create second overload with a DIFFERENT signature using plain CREATE FUNCTION.
-- This is the regression test: without the fix, has("wasm_mul") returned true
-- and this would throw FUNCTION_ALREADY_EXISTS.
CREATE FUNCTION wasm_mul LANGUAGE WASM ABI ROW_DIRECT FROM 'mul_i64_module' :: 'mul' ARGUMENTS (Int64, Int64) RETURNS Int64;
SELECT wasm_mul(toInt64(3), toInt64(4));

-- Both overloads are callable
SELECT wasm_mul(toInt32(5), toInt32(6));
SELECT wasm_mul(toInt64(5), toInt64(6));

-- Creating a third overload with the SAME signature as the first should fail
CREATE FUNCTION wasm_mul LANGUAGE WASM ABI ROW_DIRECT FROM 'mul_i32_module' :: 'mul' ARGUMENTS (Int32, Int32) RETURNS Int32; -- { serverError FUNCTION_ALREADY_EXISTS }

-- Creating a third overload with the SAME signature as the second should also fail
CREATE FUNCTION wasm_mul LANGUAGE WASM ABI ROW_DIRECT FROM 'mul_i64_module' :: 'mul' ARGUMENTS (Int64, Int64) RETURNS Int64; -- { serverError FUNCTION_ALREADY_EXISTS }

-- CREATE OR REPLACE for a matching signature should replace that specific overload
CREATE OR REPLACE FUNCTION wasm_mul LANGUAGE WASM ABI ROW_DIRECT FROM 'mul_i32_module' :: 'mul' ARGUMENTS (Int32, Int32) RETURNS Int32;
SELECT wasm_mul(toInt32(7), toInt32(8));

-- IF NOT EXISTS for a matching signature should be a no-op (not throw)
CREATE FUNCTION IF NOT EXISTS wasm_mul LANGUAGE WASM ABI ROW_DIRECT FROM 'mul_i32_module' :: 'mul' ARGUMENTS (Int32, Int32) RETURNS Int32;
SELECT wasm_mul(toInt32(10), toInt32(20));

-- Cleanup
DROP FUNCTION wasm_mul;
DELETE FROM system.webassembly_modules WHERE name = 'mul_i32_module';
DELETE FROM system.webassembly_modules WHERE name = 'mul_i64_module';
