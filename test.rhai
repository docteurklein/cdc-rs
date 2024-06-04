fn debezium(db, table, op, before, after, ts) {
	before.tenant = db; // @TODO handle null case
	after.tenant = db;
	#{
		payload: #{
			before: before,
			after: after,
			op: switch op {
				"Backfill" => "c",
				"Insert" => "c",
				"Update" => "u",
				"Delete" => "d",
				_ => op,
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

fn bigquery(db, table, op, before, after, ts) {
	let fields = if op == "Delete" { before } else { after };
	fields._CHANGE_TYPE = switch op {
		"Backfill" => "UPSERT",
		"Insert" => "UPSERT",
		"Update" => "UPSERT",
		"Delete" => "DELETE",
		_ => op,
	};
	fields.tenant = db;
	fields
}

fn transform(db, table, op, before, after, ts) {
	let res = bigquery(db, table, op, before, after, ts);
	print(res);
	print(`${db}.${table}`);
	res
}

fn topic(db, table) {
	`projects/my-project-id/topics/all_pims.${table}`
}
