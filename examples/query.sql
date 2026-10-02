-- Write to and query a Turso database through grainlift-turso, from SQL.
--
--   export GRAINLIFT_DRIVER=/absolute/path/to/libadbc_driver_grainlift.dylib   # .so on Linux
--   export GRAINLIFT_TOKEN=...      # the token grainlift-turso printed or was given
--   uvx haybarn-cli < examples/query.sql
--
-- The service must be running on port 8080 (see the README). This needs
-- adbc_scanner 2d696f8 or newer, which Haybarn 1.5.5 installs.

LOAD adbc_scanner;

-- One secret holds the connection, so ATTACH and the adbc_* functions share it.
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
SET VARIABLE turso = (SELECT adbc_connect({'secret': 'turso'}));

-- Bulk-load a DuckDB query result into a new Turso table (ADBC ingestion).
SELECT * FROM adbc_insert(getvariable('turso')::BIGINT, 'cities', (
    SELECT * FROM (VALUES
        ('Lima', 'PE', 10092000),
        ('Pune', 'IN', 7166000),
        ('Rome', 'IT', 2873000),
        ('Oslo', 'NO', 709000)
    ) AS v(name, country, population)
), mode := 'replace');

-- Run any statement in Turso; the result is the number of rows it changed.
CALL adbc_execute(getvariable('turso')::BIGINT,
    'UPDATE cities SET population = population + 1000 WHERE country = ''NO''');

-- adbc_scan sends the quoted SQL to Turso and returns typed Arrow batches.
SELECT * FROM adbc_scan(getvariable('turso')::BIGINT,
    'SELECT name, population FROM cities WHERE population > ? ORDER BY population DESC',
    params := row(5000000));

-- Or attach the database and use its tables like local ones.
ATTACH 'http://127.0.0.1:8080' AS t (TYPE adbc, SECRET 'turso');

SELECT c.name, k.country, c.population
FROM t.cities c
JOIN (VALUES ('PE', 'Peru'), ('IN', 'India'), ('IT', 'Italy'), ('NO', 'Norway')) AS k(code, country)
  ON c.country = k.code
ORDER BY c.population DESC;

CALL adbc_disconnect(getvariable('turso')::BIGINT);
