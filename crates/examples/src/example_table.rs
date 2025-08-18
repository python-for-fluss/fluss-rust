// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use clap::Parser;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath, DatabaseDescriptor};
use fluss::row::{GenericRow, InternalRow};
use std::time::Duration;
use tokio::try_join;

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut config = Config::parse();
    config.bootstrap_server = Some("127.0.0.1:9123".to_string());

    let conn = FlussConnection::new(config).await?;
    
    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("c1", DataTypes::int())
                .column("c2", DataTypes::string())
                .build()?,
        )
        .build()?;

    let table_path = TablePath::new("fluss".to_owned(), "rust_test".to_owned());

    let admin = conn.get_admin().await?;

    let database_descriptor = DatabaseDescriptor::builder()
        .comment("Test database created from Rust client")
        .custom_property("env", "test")
        .custom_property("created_by", "rust_client")
        .build()?;
    
    // list all databases
    println!("Listing all databases...");
    let databases = admin.list_databases().await?;
    println!("Found {} databases:", databases.len());
    for db_name in &databases {
        println!("  - {}", db_name);
    }
    
    // check if database exists
    let test_db_name = "testing";
    let db_exists = admin.database_exists(test_db_name).await?;
    println!("Database '{}' exists: {}", test_db_name, db_exists);
    
    // drop the testing database if it exists.
    // Be Careful here.
    if db_exists {
        println!("Dropping existing database '{}'...", test_db_name);
        admin.drop_database(test_db_name, true, false).await?;
        println!("Database '{}' dropped successfully!", test_db_name);
    }
    
    // create a new database
    println!("Creating database '{}'...", test_db_name);
    admin.create_database(test_db_name, true, Some(&database_descriptor)).await?;
    println!("Database '{}' created successfully!", test_db_name);
    
    // get database info
    println!("Getting database info for '{}'...", test_db_name);
    let db_info = admin.get_database_info(test_db_name).await?;
    println!("Database info:");
    println!("  Name: {}", db_info.database_name());
    println!("  Created time: {}", db_info.created_time());
    println!("  Modified time: {}", db_info.modified_time());
    println!("  Descriptor: {:?}", db_info.database_descriptor());
    
    // list databases after creation
    println!("Listing databases after creation...");
    let databases_after = admin.list_databases().await?;
    println!("Now we have {} databases:", databases_after.len());
    for db_name in &databases_after {
        println!("  - {}", db_name);
    }
    
    // drop the old table if exists. Would not happen since we just created it.
    if admin.table_exists(&table_path).await? {
        println!("Dropping existing table at path: {}", table_path);
        admin.drop_table(&table_path, true).await?;
    }

    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    // List tables in the database
    println!("Listing tables in database 'fluss'...");
    let tables = admin.list_tables("fluss").await?;
    println!("Found {} tables:", tables.len());
    for table_name in &tables {
        println!("  - {}", table_name);
    }

    // 2: get the table
    let table_info = admin.get_table(&table_path).await?;
    print!("Get created table:\n {table_info}\n");

    // write row
    let mut row = GenericRow::new();
    row.set_field(0, 22222);
    row.set_field(1, "t2t");

    let table = conn.get_table(&table_path).await?;
    let append_writer = table.new_append()?.create_writer();
    let f1 = append_writer.append(row);
    row = GenericRow::new();
    row.set_field(0, 233333);
    row.set_field(1, "tt44");
    let f2 = append_writer.append(row);
    try_join!(f1, f2, append_writer.flush())?;

    // scan rows
    let log_scanner = table.new_scan().create_log_scanner();
    log_scanner.subscribe(0, 0).await?;

    loop {
        let scan_records = log_scanner.poll(Duration::from_secs(10)).await?;
        println!("Start to poll records......");
        for record in scan_records {
            let row = record.row();
            println!(
                "{{{}, {}}}@{}",
                row.get_int(0),
                row.get_string(1),
                record.offset()
            );
        }
    }
}
