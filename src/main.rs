use postgres::{Client, Error};
use std::collections::HashMap;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::env;

#[derive(Debug)]
struct Nodebalancer {
    _id: i32,
    ip_address: String,
    port: i32,
}

fn create_connector() -> MakeTlsConnector {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
    builder.set_ca_file("/tmp/ca.cert").expect("unable to load ca.cert");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    connector
} 

fn create_client() -> Client {
    let connector = create_connector();
    let password = env::var("DB_PASSWORD");

    let url = format!("postgresql://akmadmin:{}@172.237.152.37:25079/defaultdb", password.expect("Password ENV var DB_PASSWORD not set."));
    let mut connection = Client::connect(&url, connector).expect("failed to create tls postgres connection");

    connection

}

fn main() -> Result<(), Error> {

    let mut connection = create_client();
    let _ = connection.batch_execute("
        CREATE TABLE IF NOT EXISTS nodebalancer (
            id              SERIAL PRIMARY KEY,
            ip_address      VARCHAR NOT NULL,
            port            INTEGER NOT NULL
            );
    ");

    let _ = connection.batch_execute("
        CREATE TABLE IF NOT EXISTS node  (
            id              SERIAL PRIMARY KEY,
            node            VARCHAR NOT NULL,
            port            INTEGER NOT NULL,
            state           VARCHAR NOT NULL,
            nodebalancer_id INTEGER NOT NULL REFERENCES nodebalancer 
            );
    ");

    let result = connection.query_one("select 10", &[]).expect("failed to execute select 10 to postgres");
    let value: i32 = result.get(0);
    println!("result of query_one call: {}", value);

    let mut nodebalancers = HashMap::new();
    nodebalancers.insert("1.2.3.4", 1234);

    
    for (key, value) in &nodebalancers {
        let mut raw_v: i32 = *value as i32;

        let nodebalancer = Nodebalancer {
            _id: 0,
            ip_address: key.to_string(),
            port: raw_v,
        };

        println!("{:?}", nodebalancer);

        connection.execute(
                "INSERT INTO nodebalancer (ip_address, port) VALUES ($1, $2)",
                &[&nodebalancer.ip_address, &nodebalancer.port],
        )?;
    }

    for row in connection.query("SELECT id, ip_address, port FROM nodebalancer", &[])? {
        let nodebalancer = Nodebalancer {
            _id: row.get(0),
            ip_address: row.get(1),
            port: row.get(2),
        };
        println!("ID {} NB {} port {}", nodebalancer._id, nodebalancer.ip_address, nodebalancer.port);
    }

    Ok::<(), Error>(())
} 
