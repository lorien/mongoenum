use bson::document::Document;
use bson::Bson;
use core::time::Duration;
use mongodb::{
    bson::doc,
    options::{ClientOptions, ServerAddress},
    sync::Client,
};

struct InfoDatabase {
    name: String,
    total_storage_size: i64,
    collections: Vec<InfoCollection>,
    data_storage_size: i64,
    index_storage_size: i64,
}

struct InfoCollection {
    name: String,
    total_storage_size: i64,
    data_storage_size: i64,
    index_storage_size: i64,
    data_size: i64,
    count: i64,
    avg_object_size: i64,
}

fn build_client() -> Client {
    Client::with_options(
        ClientOptions::builder()
            .hosts(vec![ServerAddress::parse("localhost").unwrap()])
            .connect_timeout(Duration::new(1, 0))
            .server_selection_timeout(Duration::new(1, 0))
            .build(),
    )
    .expect("Could not create client instance")
}

fn check_server_connection(client: &Client) {
    client
        .database("local")
        .run_command(doc! {"ping": "1"})
        .run()
        .expect("Could not connect to mongodb server");
}

fn get_int_field(doc: &Document, name: &str) -> Result<i64, String> {
    match doc.get(name) {
        Some(&Bson::Int32(data)) => Ok(data as i64),
        Some(&Bson::Double(data)) => Ok(data.floor() as i64),
        Some(data) => Err(format!("Field name has unexpected type: {:?}", data)),
        None => Err(format!("Field {} does not exist", name)),
    }
}

fn get_int_field_or(doc: &Document, name: &str, default: i64) -> Result<i64, String> {
    match doc.get(name) {
        Some(&Bson::Int32(data)) => Ok(data as i64),
        Some(&Bson::Double(data)) => Ok(data.floor() as i64),
        Some(data) => Err(format!("Field name has unexpected type: {}", data)),
        None => Ok(default),
    }
}

fn build_info_collection(col_name: &String, res: Document) -> Result<InfoCollection, String> {
    Ok(InfoCollection {
        name: col_name.to_string(),
        total_storage_size: (get_int_field(&res, "storageSize")?
            + get_int_field(&res, "totalIndexSize")?),
        data_storage_size: get_int_field(&res, "storageSize")?,
        index_storage_size: get_int_field(&res, "totalIndexSize")?,
        data_size: get_int_field(&res, "size")?,
        count: get_int_field(&res, "count")?,
        avg_object_size: get_int_field_or(&res, "avgObjSize", 0)?,
    })
}

fn collect_stat(client: &Client, failed_cols: &mut Vec<(String, String)>) -> Vec<InfoDatabase> {
    let mut stat: Vec<InfoDatabase> = vec![];
    for db_info in client.list_databases().run().unwrap() {
        //println!("{}", db_name);
        let mut cols: Vec<InfoCollection> = vec![];
        let db = client.database(&db_info.name);
        for col_name in db.list_collection_names().run().unwrap() {
            //println!("Processing collection: {}.{}", db_name, col_name);
            match build_info_collection(
                &col_name,
                db.run_command(doc! {"collstats": &col_name}).run().unwrap(),
            ) {
                Ok(res) => cols.push(res),
                Err(error) => {
                    failed_cols.push((
                        format!("{}.{}", db_info.name, col_name),
                        format!("{}", error),
                    ));
                }
            }
        }
        cols.sort_by_key(|x| -1 * x.total_storage_size);
        stat.push(InfoDatabase {
            name: db_info.name,
            total_storage_size: db_info.size_on_disk as i64,
            data_storage_size: cols.iter().map(|x| x.data_storage_size).sum(),
            index_storage_size: cols.iter().map(|x| x.index_storage_size).sum(),
            collections: cols,
        });
    }
    stat.sort_by_key(|x| -1 * x.total_storage_size);
    stat
}

fn format_bytes_amount(amount: i64) -> String {
    let mut suffix_idx = 0;
    let mut res_amount: f64 = amount as f64;
    let suffixes = vec![("b", 0), ("KB", 0), ("MB", 0), ("GB", 0), ("TB", 1)];
    while res_amount > 1000.0 && suffix_idx < suffixes.len() {
        res_amount = res_amount / 1000.0;
        suffix_idx += 1;
    }
    let amount_str = format!("{1:.0$}", suffixes[suffix_idx].1, res_amount);
    format!("{} {}", amount_str, suffixes[suffix_idx].0)
}

fn format_count_amount(amount: i64) -> String {
    let mut suffix_idx = 0;
    let mut res_amount: f64 = amount as f64;
    let suffixes = vec!["", "K", "M", "B"];
    while res_amount > 1000.0 && suffix_idx < suffixes.len() {
        res_amount = res_amount / 1000.0;
        suffix_idx += 1;
    }
    let amount_str = res_amount.round().to_string();
    format!("{}{}", amount_str, suffixes[suffix_idx])
}

fn main() {
    let client = build_client();
    check_server_connection(&client);
    let mut failed_cols: Vec<(String, String)> = vec![];
    let stat = collect_stat(&client, &mut failed_cols);
    for db in stat {
        println!(
            "[{}: {} = {} + {}]",
            db.name,
            format_bytes_amount(db.total_storage_size),
            format_bytes_amount(db.data_storage_size),
            format_bytes_amount(db.index_storage_size)
        );
        for col in &db.collections {
            println!(
                " - {}: {} = {} + {} / data: {} / items: {} / object: {}",
                col.name,
                format_bytes_amount(col.total_storage_size),
                format_bytes_amount(col.data_storage_size),
                format_bytes_amount(col.index_storage_size),
                format_bytes_amount(col.data_size),
                format_count_amount(col.count),
                format_bytes_amount(col.avg_object_size),
            );
        }
    }
    if failed_cols.len() > 0 {
        println!("Failed to process collections:");
        for (col, msg) in &failed_cols {
            println!(" - {}: {}", col, msg);
        }
    }
}
