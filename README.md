# what?

A rust-based mysql replication client that emits a pubsub message for each changed mysql row.  
Replication position state is stored client-side using a disk-based sqlite db for now.

# why?

Because debezium is bad to tweak.  
Instead of using debezium SingleMessageTransformers, this instead uses an embedded scripting language (rhai) to transfom messages and destination topics.

This enables easier CDC use-cases, for example: mysql -> cdc-rs -> pubsub topic -> bigquery subscription -> CDC-enabled bigquery table.


# how?

```sh
cdc-rs \
  --source 'mysql://root:root@127.0.0.1:3306' \
  --server-id 1 \
  --regex '^pim.*\.pim_catalog_product' \
  --state cdc-rs.sqlite \
  --script script.rhai
```

Where `script.rhai` contains:

```
fn transform(db, table, changeType, before, after) {
  after.db = db;
  after._CHANGE_TYPE = changeType;
  after
}

fn topic(db, table) {
  `prefix.${table}`
}
```
