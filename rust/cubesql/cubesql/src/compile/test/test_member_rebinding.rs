use super::*;

fn role_meta() -> Arc<MetaContext> {
    get_test_tenant_ctx_with_meta(
        ["Orders", "Inspectors", "Buyers"]
            .iter()
            .map(|name| CubeMeta {
                name: name.to_string(),
                dimensions: [
                    "id",
                    "name",
                    "abcdefghijklmnop_left",
                    "abcdefghijklmnop_right",
                ]
                .iter()
                .map(|member| {
                    CubeMetaDimension::new(format!("{name}.{member}"), "string".to_string())
                })
                .collect(),
                joins: if *name == "Orders" {
                    Some(
                        ["Inspectors", "Buyers"]
                            .iter()
                            .map(|role| CubeMetaJoin {
                                name: role.to_string(),
                                relationship: "belongsTo".to_string(),
                            })
                            .collect(),
                    )
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
            .collect(),
    )
}

// The fake transport serializes schema-compiler requests. Render those requests
// over distinct role values, including subquery joins, then execute the real
// wrapper SQL. Production schema-compiler execution is covered by provider tests.
fn fixture_sql(sql: &str, members: &mut Vec<String>) -> String {
    use serde_json::Value;
    let mut result = sql.to_string();
    while let Some(start) = result.find("SELECT * FROM {") {
        let json_start = start + "SELECT * FROM ".len();
        let mut stream =
            serde_json::Deserializer::from_str(&result[json_start..]).into_iter::<Value>();
        let input = stream.next().unwrap().unwrap();
        let end = json_start + stream.byte_offset();
        let query = &input["query"];
        let expand = |expr: &str, members: &mut Vec<String>| {
            regex::Regex::new(r"\$\{(Orders|Inspectors|Buyers)\.([^}]+)\}")
                .unwrap()
                .replace_all(expr, |caps: &regex::Captures| {
                    members.push(format!("{}.{}", &caps[1], &caps[2]));
                    let name = if &caps[2] == "id" { "id" } else { "name" };
                    format!("\"{}\".\"{}\"", &caps[1], name)
                })
                .to_string()
        };
        let mut columns = vec![];
        let mut dimensions = vec![];
        for kind in ["dimensions", "measures"] {
            for member in query[kind].as_array().into_iter().flatten() {
                let member = member.as_str().unwrap();
                let (expr, alias) = if member.starts_with('{') {
                    let value: Value = serde_json::from_str(member).unwrap();
                    (
                        value["expr"]["sql"].as_str().unwrap().to_string(),
                        value["alias"].as_str().unwrap().to_string(),
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
                let expr = expand(&expr, members);
                if kind == "dimensions" {
                    dimensions.push(expr.clone());
                }
                columns.push(format!("{expr} AS \"{alias}\""));
            }
        }
        let mut joins = String::new();
        for join in query["subqueryJoins"].as_array().into_iter().flatten() {
            let inner = fixture_sql(join["sql"].as_str().unwrap(), members);
            let on: Value = serde_json::from_str(join["on"].as_str().unwrap()).unwrap();
            let on = expand(on["expr"]["sql"].as_str().unwrap(), members);
            joins.push_str(&format!(
                " {} JOIN ({}) AS {} ON {}",
                join["joinType"].as_str().unwrap(),
                inner,
                join["alias"].as_str().unwrap(),
                on
            ));
        }
        let group = if query["ungrouped"] == true || dimensions.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", dimensions.join(", "))
        };
        let fixture = format!("SELECT {} FROM
            (VALUES ('1', 'Stored Z'), ('2', 'Stored A')) AS \"Orders\"(id, name)
            LEFT JOIN (VALUES ('1', 'Inspector A'), ('2', 'Inspector Z')) AS \"Inspectors\"(id, name) ON \"Orders\".id = \"Inspectors\".id
            LEFT JOIN (VALUES ('1', 'Buyer Z'), ('2', 'Buyer A')) AS \"Buyers\"(id, name) ON \"Orders\".id = \"Buyers\".id
            {joins}{group}", columns.join(", "));
        result.replace_range(start..end, &fixture);
    }
    result
}

async fn check_roles(grouped: bool) {
    use datafusion::{arrow::array::StringArray, execution::context::SessionContext};
    let meta = role_meta();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    for join in ["LEFT", "INNER"] {
        for (first, second, expected_first) in [
            ("i.name", "c.name", ["Inspector A", "Inspector Z"]),
            ("w.name", "c.name", ["Stored Z", "Stored A"]),
            (
                "i.abcdefghijklmnop_left",
                "c.abcdefghijklmnop_right",
                ["Inspector A", "Inspector Z"],
            ),
        ] {
            let group = if grouped {
                format!("GROUP BY w.id, {first}, {second}")
            } else {
                String::new()
            };
            let query = format!(
                "WITH role_names AS (
                SELECT w.id, {first} AS inspector_name, {second} AS buyer_name
                FROM Orders w LEFT JOIN Inspectors i ON w.__cubeJoinField = i.__cubeJoinField
                LEFT JOIN Buyers c ON w.__cubeJoinField = c.__cubeJoinField {group}
            ), keys AS (SELECT w.id FROM Orders w GROUP BY w.id)
            SELECT r.id, r.inspector_name, r.buyer_name FROM role_names r
            {join} JOIN keys k ON r.id = k.id"
            );
            let plan = convert_sql_to_cube_query(&query, meta.clone(), session.clone())
                .await
                .unwrap();
            let node = plan.as_logical_plan().find_cube_scan_wrapped_sql_deep();
            let mut members = vec![];
            let sql = fixture_sql(&node.wrapped_sql.sql, &mut members);
            let first_member = first.replace("i.", "Inspectors.").replace("w.", "Orders.");
            let second_member = second.replace("c.", "Buyers.");
            assert!(members.contains(&first_member), "{}: {:?}", query, members);
            assert!(members.contains(&second_member), "{}: {:?}", query, members);
            if first.starts_with("i.") {
                assert!(!members.contains(&"Orders.name".to_string()));
            }
            let batches = SessionContext::new()
                .sql(&sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let mut rows = vec![];
            for batch in batches {
                let columns = node
                    .member_fields
                    .iter()
                    .map(|field| {
                        let MemberField::Member(member) = field else {
                            panic!("literal field")
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
                            .map(|c| c.value(row).to_string())
                            .collect::<Vec<_>>(),
                    );
                }
            }
            rows.sort();
            assert_eq!(
                rows,
                vec![
                    vec!["1", expected_first[0], "Buyer Z"],
                    vec!["2", expected_first[1], "Buyer A"]
                ],
                "{}\n{}",
                query,
                sql
            );
        }
    }
}

#[tokio::test]
async fn member_rebinding_grouped() {
    check_roles(true).await;
}
#[tokio::test]
async fn member_rebinding_ungrouped() {
    check_roles(false).await;
}

#[tokio::test]
async fn member_rebinding_missing_field() {
    let meta = role_meta();
    let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
    let error = convert_sql_to_cube_query("SELECT i.missing FROM Orders w LEFT JOIN Inspectors i ON w.__cubeJoinField = i.__cubeJoinField", meta, session).await.err().unwrap();
    assert!(error.to_string().contains("missing"), "{}", error);
}
