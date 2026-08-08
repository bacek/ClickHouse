-- Tags: no-fasttest, no-parallel, no-msan

-- Checks DROP FUNCTION with an explicit overload signature (Type1, Type2, ...)
-- and verifies that argless DROP FUNCTION removes all overloads at once.
-- While both overloads are registered, each call selects its exact signature.
-- After dropping the Int32 overload, Int32 args widen into the Int64 one --
-- single-overload calls coerce rather than reject, see 04012_wasm_type_coercion_i64.
-- This verifies per-signature DROP works correctly.

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

-- Int32 overload is gone; Int64 overload still works.
-- Int32 args coerce to Int64 (widening allowed): 1+2=3
SELECT wasm_add(toInt32(1), toInt32(2));
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
