-- Write to and query a Turso database through grainlift-turso, from SQL.
--
--   export GRAINLIFT_DRIVER=/absolute/path/to/libadbc_driver_grainlift.dylib   # .so on Linux
--   export GRAINLIFT_TOKEN=...      # the token grainlift-turso printed or was given
--   uvx haybarn-cli < examples/query.sql
--
-- The service must be running on port 8080 with a fresh database (see the
-- README). This needs adbc_scanner 2d696f8 or newer, which Haybarn 1.5.5
-- installs; FORCE INSTALL replaces an older copy.

FORCE INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

-- How to reach grainlift-turso: the driver, the server and a token.
CREATE SECRET turso (
    TYPE adbc,
    DRIVER getenv('GRAINLIFT_DRIVER'),
    URI 'http://127.0.0.1:8080',
    SCOPE 'http://127.0.0.1:8080',
    EXTRA_OPTIONS MAP {
        'grainlift.target': 'turso',
        'grainlift.auth.bearer_token': getenv('GRAINLIFT_TOKEN')
    }
);

-- Attach the Turso database. READ_WRITE lets you create tables and insert.
ATTACH 'http://127.0.0.1:8080' AS turso (TYPE adbc, SECRET 'turso', READ_WRITE);

-- Create a Turso table from a DuckDB query, then add a row.
CREATE TABLE turso.cities AS
    SELECT * FROM (VALUES
        ('Lima', 'PE', 10092000),
        ('Pune', 'IN', 7166000),
        ('Rome', 'IT', 2873000)
    ) AS v(name, country, population);

INSERT INTO turso.cities VALUES ('Oslo', 'NO', 709000);

-- Query it like any table, joined here with local data.
SELECT c.name, k.country, c.population
FROM turso.cities c
JOIN (VALUES ('PE', 'Peru'), ('IN', 'India'), ('IT', 'Italy'), ('NO', 'Norway')) AS k(code, country)
  ON c.country = k.code
ORDER BY c.population DESC;
