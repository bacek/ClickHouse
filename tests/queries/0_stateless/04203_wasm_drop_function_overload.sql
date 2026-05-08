-- Tags: no-fasttest, no-parallel, no-msan

-- Checks DROP FUNCTION with an explicit overload signature (Type1, Type2, ...)
-- and verifies that argless DROP FUNCTION removes all overloads at once.
-- Uses (Int32, Int32) and (Int64, Int64) overloads because they map to distinct
-- WASM value kinds (i32 vs i64) and therefore do not coerce into each other.

SET allow_experimental_analyzer = 1;

-- Cleanup at start (functions and modules persist on disk across test runs)
DROP FUNCTION IF EXISTS wasm_add;
DELETE FROM system.webassembly_modules WHERE name = 'add_i32_module';
DELETE FROM system.webassembly_modules WHERE name = 'add_i64_module';

-- (i32, i32) -> i32 add module
INSERT INTO system.webassembly_modules (name, code)
    SELECT 'add_i32_module', base64Decode('AGFzbQEAAAABBwFgAn9/AX8DAgEABwcBA2FkZAAACgkBBwAgACABags=');

-- (i64, i64) -> i64 add module
INSERT INTO system.webassembly_modules (name, code)
    SELECT 'add_i64_module', base64Decode('AGFzbQEAAAABBwFgAn5+AX4DAgEABwcBA2FkZAAACgkBBwAgACABfAs=');

-- Register two distinct overloads pointing at 'add' in their respective modules.
CREATE FUNCTION wasm_add LANGUAGE WASM ABI ROW_DIRECT FROM 'add_i32_module' :: 'add' ARGUMENTS (Int32, Int32) RETURNS Int32;
CREATE OR REPLACE FUNCTION wasm_add LANGUAGE WASM ABI ROW_DIRECT FROM 'add_i64_module' :: 'add' ARGUMENTS (Int64, Int64) RETURNS Int64;

-- Both overloads are callable
SELECT wasm_add(toInt32(3), toInt32(4));
SELECT wasm_add(toInt64(10), toInt64(5));

-- Drop the Int32 overload by explicit signature
DROP FUNCTION wasm_add(Int32, Int32);

-- Int32 overload is gone; Int64 overload still works
SELECT wasm_add(toInt32(1), toInt32(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT wasm_add(toInt64(6), toInt64(7));

-- Drop the remaining Int64 overload by explicit signature
DROP FUNCTION wasm_add(Int64, Int64);

-- All overloads gone
SELECT wasm_add(toInt64(1), toInt64(2)); -- { serverError UNKNOWN_FUNCTION }

-- IF EXISTS on a missing overload does not error
DROP FUNCTION IF EXISTS wasm_add(Int32, Int32);

-- Non-existent overload without IF EXISTS errors
DROP FUNCTION wasm_add(Int32, Int32); -- { serverError UNKNOWN_FUNCTION }

-- ---- Verify argless DROP removes ALL overloads ----

-- Re-add both overloads so there is something to drop.
CREATE FUNCTION wasm_add LANGUAGE WASM ABI ROW_DIRECT FROM 'add_i32_module' :: 'add' ARGUMENTS (Int32, Int32) RETURNS Int32;
CREATE OR REPLACE FUNCTION wasm_add LANGUAGE WASM ABI ROW_DIRECT FROM 'add_i64_module' :: 'add' ARGUMENTS (Int64, Int64) RETURNS Int64;

-- Sanity: both work
SELECT wasm_add(toInt32(2), toInt32(3));
SELECT wasm_add(toInt64(2), toInt64(3));

-- Drop with no signature — must remove every overload
DROP FUNCTION wasm_add;

-- Both overloads must be gone
SELECT wasm_add(toInt32(1), toInt32(2));   -- { serverError UNKNOWN_FUNCTION }
SELECT wasm_add(toInt64(1), toInt64(2)); -- { serverError UNKNOWN_FUNCTION }

-- IF EXISTS when all overloads are absent is fine
DROP FUNCTION IF EXISTS wasm_add;

-- Cleanup
DELETE FROM system.webassembly_modules WHERE name = 'add_i32_module';
DELETE FROM system.webassembly_modules WHERE name = 'add_i64_module';
