use bson::document::{Document, ValueAccessError};
use core::time::Duration;
use mongodb::{
    bson::doc,
    options::{ClientOptions, ServerAddress},
    sync::Client,
};

fn main() {
    let client = Client::with_options(
        ClientOptions::builder()
            .hosts(vec![ServerAddress::parse("localhost").unwrap()])
            .connect_timeout(Duration::new(1, 0))
            .server_selection_timeout(Duration::new(1, 0))
            .build(),
    )
    .expect("Could not create client instance");
    let res = client
        .database("tme")
        .run_command(doc! {"collstats": "archive_url"})
        .run()
        .unwrap();
    let num: f64 = res.get_f64("size").unwrap() + 0.5;
    println!("{:?}", num);
    println!("{:?}", num.floor() as i64);
}
