use super::*;
use crate::compile::rewrite::rewriter::Rewriter;
use datafusion::{arrow::array::StringArray, execution::context::SessionContext};
use serde_json::Value;
use std::collections::HashSet;

// Two related cubes deliberately expose the same short member names.
fn alias_meta() -> Arc<MetaContext> {
    let cubes = ["Orders", "Buyers"]
        .iter()
        .copied()
        .map(|name| CubeMeta {
            name: name.to_string(),
            dimensions: [
                "id",
                "name",
                "abcdefghijklmnop_left",
                "abcdefghijklmnop_right",
            ]
            .iter()
            .copied()
            .map(|member| CubeMetaDimension::new(format!("{name}.{member}"), "string".to_string()))
            .collect(),
            joins: if name == "Orders" {
                Some(vec![CubeMetaJoin {
                    name: "Buyers".to_string(),
                    relationship: "belongsTo".to_string(),
                }])
            } else {
                None
            },
            measures: vec![],
            segments: vec![],
            description: None,
            title: None,
            r#type: V1CubeMetaType::Cube,
            folders: None,
            nested_folders: None,
            hierarchies: None,
            meta: None,
        })
        .collect();
    get_test_tenant_ctx_with_meta(cubes)
}

#[tokio::test]
async fn duplicate_wrapper_aliases_joined_members() {
    check_joined_members(false).await;
}

#[tokio::test]
#[ignore = "Initial planning loses the source qualifier under grouped explicit aliases before wrapper generation"]
async fn duplicate_wrapper_aliases_grouped_user_aliases() {
    init_testing_logger();
    check_joined_members(true).await;
}

async fn check_joined_members(known_limit: bool) {
    assert!(Rewriter::sql_push_down_enabled());
    let meta = alias_meta();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for grouped in [true, false] {
        for (buyer_member, order_member) in [
            ("name", "name"),
            ("abcdefghijklmnop_left", "abcdefghijklmnop_right"),
        ] {
            for user_aliases in [false, true] {
                if (grouped && user_aliases && buyer_member == order_member) != known_limit {
                    continue;
                }
                let buyer_alias = if user_aliases { " AS current_name" } else { "" };
                let order_alias = if user_aliases { " AS stored_name" } else { "" };
                let extra = if grouped {
                    "MIN(Orders.id)"
                } else {
                    "LOWER(Orders.id)"
                };
                let group = if grouped { "GROUP BY 1, 2, 3" } else { "" };
                let inner = format!(
                    "SELECT Orders.id, Buyers.{buyer_member}{buyer_alias}, Orders.{order_member}{order_alias}, {extra} AS extra \
                     FROM Orders CROSS JOIN Buyers {group}"
                );
                let query = if user_aliases {
                    format!("SELECT id, current_name, stored_name, extra FROM ({inner}) q ORDER BY stored_name")
                } else {
                    format!("{inner} ORDER BY Orders.{order_member}")
                };
                let plan = convert_sql_to_cube_query(&query, meta.clone(), session.clone())
                    .await
                    .unwrap();
                let node = plan.as_logical_plan().find_cube_scan_wrapped_sql_deep();
                let sql = render_fixture_sql(&node.wrapped_sql.sql);
                let ctx = SessionContext::new();
                let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                let mut rows = vec![];
                for batch in batches {
                    let columns = node
                        .member_fields
                        .iter()
                        .take(3)
                        .map(|field| {
                            let MemberField::Member(member) = field else {
                                panic!("unexpected literal")
                            };
                            batch
                                .column(batch.schema().index_of(&member.field_name).unwrap())
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    for row in 0..batch.num_rows() {
                        rows.push(
                            columns
                                .iter()
                                .map(|col| col.value(row).to_string())
                                .collect::<Vec<_>>(),
                        );
                    }
                }
                assert_eq!(
                    rows,
                    vec![
                        vec!["2", "Current Z", "Stored A"],
                        vec!["1", "Current A", "Stored Z"],
                    ],
                    "query: {query}\nSQL: {sql}"
                );
            }
        }
    }
}

// The standard transport serializes its inputs in place of schema-compiler SQL.
// Render just this fixture's member expressions over inline joined data, then
// execute the real wrapper SQL above it. This checks value and ORDER BY binding
// as well as the aliases sent across the schema-compiler boundary.
fn render_fixture_sql(sql: &str) -> String {
    let marker = "SELECT * FROM ";
    let start = sql.find(&format!("{marker}{{")).unwrap();
    let json_start = start + marker.len();
    let mut stream = serde_json::Deserializer::from_str(&sql[json_start..]).into_iter::<Value>();
    let input = stream.next().unwrap().unwrap();
    let end = json_start + stream.byte_offset();
    let query = &input["query"];
    assert_eq!(
        query["joinHints"],
        serde_json::json!([["Orders", "Buyers"]])
    );
    let mut aliases = HashSet::new();
    let mut dimensions = vec![];
    let mut columns = vec![];
    for kind in ["dimensions", "measures"] {
        for member in query[kind].as_array().into_iter().flatten() {
            let member = member.as_str().unwrap();
            let (expr, alias) = if member.starts_with('{') {
                let definition: Value = serde_json::from_str(member).unwrap();
                (
                    definition["expr"]["sql"].as_str().unwrap().to_string(),
                    definition["alias"].as_str().unwrap().to_string(),
                )
            } else {
                (
                    format!("${{{member}}}"),
                    input["member_to_alias"][member]
                        .as_str()
                        .unwrap()
                        .to_string(),
                )
            };
            assert!(
                aliases.insert(alias.clone()),
                "duplicate alias {}: {}",
                alias,
                sql
            );
            let expr = regex::Regex::new(r"\$\{(Orders|Buyers)\.([^}]+)\}")
                .unwrap()
                .replace_all(&expr, |caps: &regex::Captures| {
                    let column = if &caps[2] == "id" {
                        "id"
                    } else if &caps[1] == "Orders" {
                        "stored_value"
                    } else {
                        "current_value"
                    };
                    format!("\"{}\".\"{column}\"", &caps[1])
                })
                .to_string();
            if kind == "dimensions" {
                dimensions.push(expr.clone());
            }
            columns.push(format!("{expr} AS \"{alias}\""));
        }
    }
    let group = if query["ungrouped"] == true {
        String::new()
    } else {
        format!(" GROUP BY {}", dimensions.join(", "))
    };
    let fixture = format!(
        "SELECT {} FROM \
         (VALUES ('1', 'Stored Z'), ('2', 'Stored A')) AS \"Orders\"(id, stored_value) \
         LEFT JOIN (VALUES ('1', 'Current A'), ('2', 'Current Z')) AS \"Buyers\"(id, current_value) \
         ON \"Orders\".id = \"Buyers\".id{group}", columns.join(", ")
    );
    format!("{}{}{}", &sql[..start], fixture, &sql[end..])
}
