# what?

A rust-based mysql replication client that emits a pubsub message for each changed mysql row.  
Replication position state is stored client-side using a disk-based sqlite db for now.

# why?

Because debezium is bad to tweak.  
Instead of using debezium SingleMessageTransformers, this instead uses an embedded scripting language (rhai) to transfom messages and destination topics.

This enables easier CDC use-cases, for example: mysql -> cdc-rs -> pubsub topic -> bigquery subscription -> CDC-enabled bigquery table.


# how?

## nix flake

```
nix run github:docteurklein/cdc-rs -- --help
```

## OCI image

```
nix run .#oci-image.copyTo -- docker://docker.io/docteurklein/cdc-rs:latest
docker run --rm -it -v $PWD:$PWD -w $PWD docteurklein/cdc-rs:latest --help
```

## kubernetes

```
kubectl apply -f $(nix build --no-link --print-out-paths .#kube)
```


## example usage

```sh
export SOURCE="mysql://replica:$PASSWORD@127.0.0.1:3306"
cdc-rs \
  --server-id 1 \
  --regex '^pim.*\.pim_catalog_product' \
  --state cdc-rs.sqlite \
  --script script.rhai
```

Where `script.rhai` contains:

```
fn debezium(db, table, changeType, before, after) {
	let ts = timestamp();
	#{
		payload: #{
			before: before,
			after: after,
			op: switch changeType {
				"INSERT" => "c",
				"UPDATE" => "u",
				"DELETE" => "d",
				_ => changeType,
			},
			source: #{
				ts_ms: `${ts}`,
				db: db,
				table: table,
			},
			ts_ms: `${ts}`,
		}
	}
}

fn transform(db, table, changeType, before, after) {
	before.tenant = db;
	after.tenant = db;
	debezium(db, table, changeType, before, after)
}

fn topic(db, table) {
  `prefix.${table}`
}
```
