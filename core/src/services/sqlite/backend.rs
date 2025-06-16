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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::pin::Pin;
use std::str::FromStr;
use std::task::Context;
use std::task::Poll;

use futures::stream::BoxStream;
use futures::Stream;
use futures::StreamExt;
use ouroboros::self_referencing;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::SqlitePool;
use tokio::sync::OnceCell;

use crate::raw::oio;
use crate::raw::*;
use crate::services::SqliteConfig;
use crate::*;

impl Configurator for SqliteConfig {
    type Builder = SqliteBuilder;
    fn into_builder(self) -> Self::Builder {
        SqliteBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SqliteBuilder {
    config: SqliteConfig,
}

impl Debug for SqliteBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SqliteBuilder");

        ds.field("config", &self.config);
        ds.finish()
    }
}

impl SqliteBuilder {
    /// Set the connection_string of the sqlite service.
    ///
    /// This connection string is used to connect to the sqlite service. There are url based formats:
    ///
    /// ## Url
    ///
    /// This format resembles the url format of the sqlite client:
    ///
    /// - `sqlite::memory:`
    /// - `sqlite:data.db`
    /// - `sqlite://data.db`
    ///
    /// For more information, please visit <https://docs.rs/sqlx/latest/sqlx/sqlite/struct.SqliteConnectOptions.html>.
    pub fn connection_string(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.connection_string = Some(v.to_string());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set the table name of the sqlite service to read/write.
    pub fn table(mut self, table: &str) -> Self {
        if !table.is_empty() {
            self.config.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the sqlite service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(mut self, key_field: &str) -> Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the sqlite service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(mut self, value_field: &str) -> Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for SqliteBuilder {
    const SCHEME: Scheme = Scheme::Sqlite;
    type Config = SqliteConfig;

    fn build(self) -> Result<impl Access> {
        let conn = match self.config.connection_string {
            Some(v) => v,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "connection_string is required but not set",
                )
                .with_context("service", Scheme::Sqlite));
            }
        };

        let config = SqliteConnectOptions::from_str(&conn).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "connection_string is invalid")
                .with_context("service", Scheme::Sqlite)
                .set_source(err)
        })?;

        let table = match self.config.table {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Sqlite));
            }
        };

        let key_field = self.config.key_field.unwrap_or_else(|| "key".to_string());

        let value_field = self
            .config
            .value_field
            .unwrap_or_else(|| "value".to_string());

        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        Ok(SqliteAccessor::new(SqliteCore {
            pool: OnceCell::new(),
            config,
            table,
            key_field,
            value_field,
        })
        .with_normalized_root(root))
    }
}

#[derive(Debug, Clone)]
pub struct SqliteCore {
    pool: OnceCell<SqlitePool>,
    config: SqliteConnectOptions,

    table: String,
    key_field: String,
    value_field: String,
}

impl SqliteCore {
    async fn get_client(&self) -> Result<&SqlitePool> {
        self.pool
            .get_or_try_init(|| async {
                let pool = SqlitePool::connect_with(self.config.clone())
                    .await
                    .map_err(parse_sqlite_error)?;
                Ok(pool)
            })
            .await
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let pool = self.get_client().await?;

        let value: Option<Vec<u8>> = sqlx::query_scalar(&format!(
            "SELECT `{}` FROM `{}` WHERE `{}` = $1 LIMIT 1",
            self.value_field, self.table, self.key_field
        ))
        .bind(path)
        .fetch_optional(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(value.map(Buffer::from))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(&format!(
            "INSERT OR REPLACE INTO `{}` (`{}`, `{}`) VALUES ($1, $2)",
            self.table, self.key_field, self.value_field,
        ))
        .bind(path)
        .bind(value.to_vec())
        .execute(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let pool = self.get_client().await?;

        sqlx::query(&format!(
            "DELETE FROM `{}` WHERE `{}` = $1",
            self.table, self.key_field
        ))
        .bind(path)
        .execute(pool)
        .await
        .map_err(parse_sqlite_error)?;

        Ok(())
    }
}

//#[self_referencing]
//pub struct SqliteScanner {
//    pool: SqlitePool,
//    query: String,
//
//    #[borrows(pool, query)]
//    #[covariant]
//    stream: BoxStream<'this, Result<String>>,
//}
//
//impl Stream for SqliteScanner {
//    type Item = Result<String>;
//
//    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//        self.with_stream_mut(|s| s.poll_next_unpin(cx))
//    }
//}
//
//unsafe impl Sync for SqliteScanner {}
//
//impl kv::Scan for SqliteScanner {
//    async fn next(&mut self) -> Result<Option<String>> {
//        <Self as StreamExt>::next(self).await.transpose()
//    }
//}
//
//impl kv::Adapter for Adapter {
//    type Scanner = SqliteScanner;
//
//    fn info(&self) -> kv::Info {
//        kv::Info::new(
//            Scheme::Sqlite,
//            &self.table,
//            Capability {
//                read: true,
//                write: true,
//                delete: true,
//                list: true,
//                shared: false,
//                ..Default::default()
//            },
//        )
//    }
//
//
//    async fn scan(&self, path: &str) -> Result<Self::Scanner> {
//        let pool = self.get_client().await?;
//        let stream = SqliteScannerBuilder {
//            pool: pool.clone(),
//            query: format!(
//                "SELECT `{}` FROM `{}` WHERE `{}` LIKE $1",
//                self.key_field, self.table, self.key_field
//            ),
//            stream_builder: |pool, query| {
//                sqlx::query_scalar(query)
//                    .bind(format!("{path}%"))
//                    .fetch(pool)
//                    .map(|v| v.map_err(parse_sqlite_error))
//                    .boxed()
//            },
//        }
//        .build();
//
//        Ok(stream)
//    }
//}

fn parse_sqlite_error(err: sqlx::Error) -> Error {
    let is_temporary = matches!(
        &err,
        sqlx::Error::Database(db_err) if db_err.code().is_some_and(|c| c == "5" || c == "6")
    );

    let message = if is_temporary {
        "database is locked or busy"
    } else {
        "unhandled error from sqlite"
    };

    let mut error = Error::new(ErrorKind::Unexpected, message).set_source(err);
    if is_temporary {
        error = error.set_temporary();
    }
    error
}

/// SqliteAccessor implements Access trait directly
#[derive(Debug, Clone)]
pub struct SqliteAccessor {
    core: std::sync::Arc<SqliteCore>,
    root: String,
    info: std::sync::Arc<AccessorInfo>,
}

impl SqliteAccessor {
    fn new(core: SqliteCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Sqlite);
        info.set_name(&core.table);
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            write: true,
            delete: true,
            stat: true, //? Set for redis, do we need?
            write_can_empty: true, //? Set for redis, do we need?
            list: true,
            shared: false,
            ..Default::default()
        });

        Self {
            core: std::sync::Arc::new(core),
            root: "/".to_string(),
            info: std::sync::Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

// TODO: finish refactor
impl Access for SqliteAccessor {
    type Reader = Buffer;
    type Writer = RedisWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<RedisDeleter>;

    fn info(&self) -> std::sync::Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "key not found in redis")),
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), RedisWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(RedisDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let _ = build_abs_path(&self.root, path);
        // Redis doesn't support listing keys, return empty list
        // TODO: Sqlite does, I think via the SqliteScanner commented above
        Ok((RpList::default(), ()))
    }
}

pub struct RedisWriter {
    core: std::sync::Arc<RedisCore>,
    path: String,
    buffer: oio::QueueBuf,
}

impl RedisWriter {
    fn new(core: std::sync::Arc<RedisCore>, path: String) -> Self {
        Self {
            core,
            path,
            buffer: oio::QueueBuf::new(),
        }
    }
}

impl oio::Write for RedisWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.buffer.push(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let buf = self.buffer.clone().collect();
        let length = buf.len() as u64;
        self.core.set(&self.path, buf).await?;

        let meta = Metadata::new(EntryMode::from_path(&self.path)).with_content_length(length);
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer.clear();
        Ok(())
    }
}

pub struct RedisDeleter {
    core: std::sync::Arc<RedisCore>,
    root: String,
}

impl RedisDeleter {
    fn new(core: std::sync::Arc<RedisCore>, root: String) -> Self {
        Self { core, root }
    }
}

impl oio::OneShotDelete for RedisDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let p = build_abs_path(&self.root, &path);
        self.core.delete(&p).await?;
        Ok(())
    }
}

// TODO: add tests


