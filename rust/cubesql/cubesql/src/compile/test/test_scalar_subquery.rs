use super::TestContext;
use crate::compile::{DatabaseProtocol, QueryPlan};
use datafusion::{arrow::datatypes::DataType, dataframe::DataFrame, scalar::ScalarValue};

async fn check(sql: &str, names: &[&str], values: &[Option<i64>]) {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    check_with_context(&context, sql, names, values).await;
}

async fn check_with_context(
    context: &TestContext,
    sql: &str,
    names: &[&str],
    values: &[Option<i64>],
) {
    let query = context.convert_sql_to_cube_query(sql).await.unwrap();
    let (plan, ctx) = match query {
        QueryPlan::DataFusionSelect(plan, ctx) => (plan, ctx),
        _ => panic!("Expected DataFusion plan"),
    };
    let df = DataFrame::new(ctx.state, &plan);
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), names.len());
    for (i, (name, value)) in names.iter().zip(values).enumerate() {
        assert_eq!(batch.schema().field(i).name(), name);
        assert_eq!(batch.schema().field(i).data_type(), &DataType::Int64);
        assert_eq!(
            ScalarValue::try_from_array(batch.column(i), 0).unwrap(),
            ScalarValue::Int64(*value)
        );
    }
}

#[tokio::test]
async fn scalar_subquery_cube_counts() {
    use cubeclient::models::{V1LoadRequestQuery, V1LoadRequestQueryFilterItem};
    use serde_json::json;
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    // Synthetic populations of three A rows and one B row.
    for (gender, count) in [("A", 3), ("B", 1)].iter() {
        context
            .add_cube_load_mock(
                V1LoadRequestQuery {
                    measures: Some(vec!["KibanaSampleDataEcommerce.count".into()]),
                    dimensions: Some(vec![]),
                    segments: Some(vec![]),
                    order: Some(vec![]),
                    filters: Some(vec![V1LoadRequestQueryFilterItem {
                        member: Some("KibanaSampleDataEcommerce.customer_gender".into()),
                        operator: Some("equals".into()),
                        values: Some(vec![gender.to_string()]),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
                crate::compile::tests::simple_load_response(
                    vec!["KibanaSampleDataEcommerce.count"],
                    vec![vec![json!(count)]],
                ),
            )
            .await;
    }
    check_with_context(&context, "SELECT (SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE customer_gender = 'A') AS a_count, (SELECT COUNT(*) FROM KibanaSampleDataEcommerce WHERE customer_gender = 'B') AS b_count", &["a_count", "b_count"], &[Some(3), Some(1)]).await;
    assert_eq!(context.load_calls().await.len(), 2);
}

#[tokio::test]
async fn scalar_subquery_nested_and_outer_column() {
    check("SELECT CAST(7 AS BIGINT) AS first, (SELECT (SELECT COUNT(*) FROM (VALUES (1), (2)) t(id))) AS nested, CAST(9 AS BIGINT) AS last", &["first", "nested", "last"], &[Some(7), Some(2), Some(9)]).await;
    check("SELECT id AS outer_id, (SELECT COUNT(*) FROM (VALUES (1), (2)) t(x)) AS total FROM (VALUES (7)) t(id)", &["outer_id", "total"], &[Some(7), Some(2)]).await;
}

#[tokio::test]
async fn scalar_subquery_single_count() {
    check(
        "SELECT (SELECT COUNT(*) FROM (VALUES (1), (2)) t(id)) AS total",
        &["total"],
        &[Some(2)],
    )
    .await;
}

#[tokio::test]
async fn scalar_subquery_distinct_populations() {
    check("WITH data AS (SELECT * FROM (VALUES (1), (1), (2), (3)) t(id)), a AS (SELECT id FROM data WHERE id < 3), b AS (SELECT id FROM data WHERE id = 1) SELECT (SELECT COUNT(*) FROM a) AS total, (SELECT COUNT(DISTINCT id) FROM b) AS unique_total", &["total", "unique_total"], &[Some(3), Some(1)]).await;
}

#[tokio::test]
async fn scalar_subquery_empty_counts() {
    check("WITH data AS (SELECT * FROM (VALUES (1)) t(id) WHERE id = 2) SELECT (SELECT COUNT(*) FROM data) AS total, (SELECT COUNT(DISTINCT id) FROM data) AS unique_total", &["total", "unique_total"], &[Some(0), Some(0)]).await;
}

#[tokio::test]
async fn scalar_subquery_nullable() {
    check("SELECT (SELECT MAX(id) FROM (VALUES (1)) t(id) WHERE id = 2) AS maximum, (SELECT CAST(NULL AS BIGINT)) AS missing", &["maximum", "missing"], &[None, None]).await;
}

#[tokio::test]
async fn scalar_subquery_multirow() {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    let error = context
        .execute_query("SELECT (SELECT id FROM (VALUES (1), (2)) t(id)) AS invalid")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Sub query should return no more than one row"),
        "{}",
        error
    );
}
