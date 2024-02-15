const colMap = #{
	pim_catalog_product: #{
        "@0": "uuid",
        "@1": "family_id",
        "@2": "product_model_id",
        "@3": "family_variant_id",
        "@4": "id",
        "@5": "is_enabled",
        "@6": "identifier",
        "@7": "raw_values",
        "@8": "created",
        "@9": "updated",
    },
	pim_catalog_product_model: #{
        "@0": "id",
        "@1": "code",
        "@2": "parent_id",
        "@3": "family_variant_id",
        "@4": "raw_values",
        "@5": "created",
        "@6": "updated",
    },
};

fn rename(table, fields) {
	let t = global::colMap[table];
	if t == () {
		return fields;
	}
	let to = #{};
	for k in fields.keys() {
		let nk = t[k] ?? k;
		to.set(nk, fields[k]);
	}
	to
}

fn debezium(db, table, changeType, before, after, ts) {
	#{
		payload: #{
			before: before,
			after: after,
			op: switch changeType {
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

fn bigquery(db, table, changeType, before, after) {
	let fields = if changeType == "Delete" { before } else { after };
	fields._CHANGE_TYPE = switch changeType {
		"Insert" => "UPSERT",
		"Update" => "UPSERT",
		"Delete" => "DELETE",
		_ => changeType,
	};
	fields
}
fn transform(db, table, changeType, before, after, ts) {
	before.tenant = db;
	after.tenant = db;
	bigquery(db, table, changeType, rename(table, before), rename(table, after))
	// bigquery(db, table, changeType, before, after)
}

fn topic(db, table) {
	`all_pims.${table}`
}

