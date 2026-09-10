//! Tests that validate that complex but self-contained queries can be executed correctly by DF

use crate::compile::{
    test::{execute_query, init_testing_logger},
    DatabaseProtocol,
};

#[tokio::test]
async fn test_timestamp_coalesce_typing() {
    use crate::compile::{test::TestContext, QueryPlan};
    use datafusion::{
        arrow::datatypes::{DataType, TimeUnit},
        dataframe::DataFrame,
        scalar::ScalarValue,
    };

    let ctx = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    let fixture = "(SELECT id, CAST(start_date AS TIMESTAMP) AS start_date, CAST(end_date AS TIMESTAMP) AS end_date FROM (VALUES (1, '2026-10-01', '2026-10-02'), (2, '2026-10-01', NULL), (3, '2026-12-01', NULL)) AS v(id, start_date, end_date)) AS t";
    for fallback in [
        "DATE '2026-11-01'",
        "TIMESTAMP '2026-11-01 00:00:00'",
        "CAST('2026-11-01' AS TIMESTAMP)",
    ] {
        let sql = format!("SELECT id, end_date, {fallback} AS fallback, COALESCE(end_date, {fallback}) AS result FROM {fixture} WHERE start_date < COALESCE(end_date, {fallback}) ORDER BY id");
        let QueryPlan::DataFusionSelect(plan, context) =
            ctx.convert_sql_to_cube_query(&sql).await.unwrap()
        else {
            panic!("expected an executable DataFusion plan");
        };
        let df = DataFrame::new(context.state, &plan);
        let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert_eq!(df.schema().field(1).data_type(), &timestamp);
        assert_eq!(df.schema().field(3).data_type(), &timestamp);
        let batches = df.collect().await.unwrap();
        let rows: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(move |row| {
                    (
                        ScalarValue::try_from_array(batch.column(0), row).unwrap(),
                        ScalarValue::try_from_array(batch.column(3), row).unwrap(),
                    )
                })
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::TimestampNanosecond(Some(1790899200000000000), None)
                ),
                (
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::TimestampNanosecond(Some(1793491200000000000), None)
                ),
            ]
        );
    }
}

#[tokio::test]
async fn test_join_with_coercion() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                WITH
                    t1 AS (
                        SELECT 1::int2 AS i1
                    ),
                    t2 AS (
                        SELECT 1::int4 AS i2
                    )
                    SELECT
                        *
                    FROM
                        t1 LEFT JOIN t2 ON (t1.i1 = t2.i2)
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn test_triple_join_with_coercion() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                WITH
                    t1 AS (
                        SELECT 1::int2 AS i1
                    ),
                    t2 AS (
                        SELECT 1::int4 AS i2
                    ),
                    t3 AS (
                        SELECT 1::int8 AS i3
                    )
                    SELECT
                        *
                    FROM
                        t1
                            LEFT JOIN t2 ON (t1.i1 = t2.i2)
                            LEFT JOIN t3 ON (t3.i3 = t2.i2)
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn union_all_alias_mismatch() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
SELECT
    foo,
    bar
FROM (
    SELECT
        'foo' as foo,
        'bar' as bar
    UNION ALL
    SELECT
        'foo' as foo,
        'bar' as qux
) t
GROUP BY
    foo, bar
;
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL,)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn union_all_ctes_with_type_coercion() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
WITH a AS (
    SELECT 1::bigint AS t LIMIT 1
), b AS (
    SELECT 2::bigint AS t LIMIT 1
), c AS (
    SELECT 3.5::float8 AS t LIMIT 1
)
SELECT 'A' AS l, t FROM a
UNION ALL
SELECT 'B' AS l, t FROM b
UNION ALL
SELECT 'C' AS l, t FROM c
;
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

/// See https://www.postgresql.org/docs/current/functions-math.html
#[tokio::test]
async fn test_round() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
SELECT
    round(42.4), -- 42
    round(42.4382, 2), -- 42.44
    round(1234.56, -1) -- 1230
;
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_date_part_interval() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            DATE_PART('day', INTERVAL '1 year 2 month 3 day 4 hour 5 minute 6 second') AS d
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_numeric_math_scalar() {
    init_testing_logger();

    // language=PostgreSQL
    let query = r#"
        SELECT
            a % 2::numeric AS m
        FROM (
            SELECT
                5::numeric AS a
            UNION ALL
            SELECT
                3.5::numeric AS a
        ) AS t
        "#;

    insta::assert_snapshot!(
        execute_query(query.to_string(), DatabaseProtocol::PostgreSQL)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_case_with_heterogeneous_then_types() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                SELECT
                    i,
                    CASE WHEN i > 1 THEN 0 WHEN i > 0 THEN i END AS c
                FROM (
                    SELECT 0::int4 AS i
                    UNION ALL
                    SELECT 1::int4 AS i
                    UNION ALL
                    SELECT 2::int4 AS i
                ) AS t
                ORDER BY i
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

/// The common type has to be picked across all branches, not from the first one: an int4
/// THEN followed by an int8 THEN must widen to int8, or the int8 value gets truncated.
#[tokio::test]
async fn test_case_with_heterogeneous_then_types_widening() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                SELECT
                    i,
                    CASE WHEN i > 1 THEN i WHEN i > 0 THEN 3000000000::int8 END AS c
                FROM (
                    SELECT 0::int4 AS i
                    UNION ALL
                    SELECT 1::int4 AS i
                    UNION ALL
                    SELECT 2::int4 AS i
                ) AS t
                ORDER BY i
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn test_case_with_heterogeneous_then_types_over_pg_catalog() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                SELECT
                    t.typname,
                    CASE WHEN t.typtype = 'd' THEN 0
                         WHEN t.typtype = 'b' THEN t.typbasetype
                         ELSE 0 END AS base
                FROM pg_catalog.pg_type t
                WHERE t.typname IN ('int4', 'text')
                ORDER BY t.typname
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn test_case_with_null_then_before_typed_then() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                SELECT
                    i,
                    CASE WHEN i > 1 THEN NULL WHEN i > 0 THEN i END AS c
                FROM (
                    SELECT 0::int4 AS i
                    UNION ALL
                    SELECT 1::int4 AS i
                    UNION ALL
                    SELECT 2::int4 AS i
                ) AS t
                ORDER BY i
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap());
}

/// Branches that have no common type have to fail to plan, rather than reach execution
/// and panic there.
#[tokio::test]
async fn test_case_with_uncoercible_then_types() {
    init_testing_logger();

    insta::assert_snapshot!(execute_query(
        // language=PostgreSQL
        r#"
                SELECT
                    CASE WHEN i > 1 THEN true ELSE DATE '2022-01-01' END AS c
                FROM (SELECT 1::int4 AS i) AS t
                "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap_err()
    .to_string());
}
