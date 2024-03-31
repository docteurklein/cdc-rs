fn debezium(db, table, op, before, after, ts) {
	#{
		payload: #{
			before: before,
			after: after,
			op: switch op {
				"Insert" => "c",
				"Update" => "u",
				"Delete" => "d",
				_ => changeType,
			},
			source: #{
				ts_ms: ts,
				db: db,
				table: table,
			},
			ts_ms: ts,
		}
	}
}

fn bigquery(db, table, op, before, after) {
	let fields = if op == "Delete" { before } else { after };
	fields._CHANGE_TYPE = switch op {
		"Insert" => "UPSERT",
		"Update" => "UPSERT",
		"Delete" => "DELETE",
		_ => changeType,
	};
	fields
}
fn transform(db, table, op, before, after, ts) {
	before.tenant = db;
	after.tenant = db;
	print(after);
	debezium(db, table, op, before, after, ts)
}

fn topic(db, table) {
	`projects/my-project-id/topics/all_pims.${table}`
}

