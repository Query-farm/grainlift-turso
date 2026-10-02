// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The README's quick-start SQL is `examples/query.sql`, which CI runs
//! against a live service; this keeps the two from drifting apart.

#[test]
fn the_readme_quick_start_sql_is_the_tested_example() {
    let readme = include_str!("../README.md");
    let example = include_str!("../examples/query.sql");
    let start = readme
        .find("**4. Query it from DuckDB.**")
        .expect("the README has a quick-start query step");
    let block = &readme[start..];
    let block = &block[block.find("```sql\n").expect("the step shows SQL") + "```sql\n".len()..];
    let block = &block[..block.find("\n```").expect("the SQL block ends")];
    let body = &example[example
        .find("FORCE INSTALL adbc_scanner")
        .expect("the example installs adbc_scanner")..];
    assert_eq!(
        block.trim_end(),
        body.trim_end(),
        "update the README's quick-start SQL to match examples/query.sql"
    );
}
