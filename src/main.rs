use chrono::Utc;
use rand::{Rng, SeedableRng};
use rand::rngs::{StdRng};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use std::sync::{Arc};
use std::error::Error as StdError;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use tokio_postgres::{NoTls, Client};
use futures::future::join_all;
use log::{error as log_error, info as log_info, LevelFilter};
use env_logger::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {

    // 初始化日志
    Builder::new()
        .filter_level(LevelFilter::Info)
        .init();

    // PostgreSQL连接初始化
    let pg_url = "host=172.20.19.27 user=buste password=passw0rd dbname=kafka_example";
    let (pg_client, pg_connection) = tokio_postgres::connect(pg_url, NoTls).await?;

    // Redis连接初始化
    let redis_client = redis::Client::open("redis://:passw0rd@172.20.19.27:6379/")?;
    let redis_conn = redis_client.get_multiplexed_async_connection().await?;

    // 启动数据库连接任务
    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            log_error!("PostgreSQL connection error: {}", e);
        }
    });

    // 将pg_client包装成Arc
    let pg_client = Arc::new(pg_client);
    let redis_conn = Arc::new(Mutex::new(redis_conn));

    // 初始化5个客户端
    let total_clients = 5;
    // 计数器，用于记录每个客户端的扣费次数
    let counters_times = Arc::new(Mutex::new(vec![0; total_clients]));
    let counters_values = Arc::new(Mutex::new(vec![0; total_clients]));

    // 模拟多个客户端竞争修改数据库
    let tasks: Vec<_> = (0..5)
        .map(|client_id| {
            let pg_client = Arc::clone(&pg_client);
            let redis_conn = Arc::clone(&redis_conn);
            let counters_times = Arc::clone(&counters_times);
            let counters_values = Arc::clone(&counters_values);
            let mut rng = StdRng::from_entropy();
            
            tokio::spawn(async move {
                // 每个客户端不断尝试扣费，直到余额不足
                let mut current_amount = Decimal::from_f64(10.0).expect("Failed to convert 10.0 to Decimal"); // 初始扣费金额为 10.0
                let deduct_type;
                if (client_id.to_i32().unwrap() as usize) <= total_clients/2 {
                    deduct_type = 1;
                } else { 
                    deduct_type = -1;
                }
                loop {
                    match deduct_balance(pg_client.clone(), redis_conn.clone(), client_id, current_amount, deduct_type).await {
                        Ok(true) => {
                            // 如果扣费成功，累加随机金额
                            let random_increment: f64 = rng.gen_range(1.0..=5.0);
                            
                            // 更新扣费次数计数器
                            let mut counters_times = counters_times.lock().await;
                            let mut counters_values = counters_values.lock().await;
                            counters_times[client_id as usize] += 1;
                            counters_values[client_id as usize] += current_amount.to_i32().unwrap();
                            if counters_times[client_id as usize] == total_clients.to_i32().unwrap() {
                                break
                            }
                            // 累加随机金额
                            current_amount += Decimal::from_f64(random_increment.ceil()).unwrap();
                            continue;
                        }
                        Ok(false) => break, // 余额不足，退出循环
                        Err(e) => {
                            log_error!("Client {} failed: {}", client_id, e);
                            break;
                        }
                    }
                }
            })
        })
        .collect();

    // 等待所有任务完成
    join_all(tasks).await;

    // 输出每个客户端的扣费次数及总扣费次数
    let counters_times = counters_times.lock().await;
    let counters_values = counters_values.lock().await;
    let total_times: i32 = counters_times.iter().sum();
    let mut total_values: i32 = 0;
    for (client_id, value_count) in counters_values.iter().enumerate() {
        // 如果客户端ID是前3个(0,1,2)，执行减法操作
        if client_id < 3 {
            total_values -= value_count;
        } else {
            // 如果客户端ID是后2个(3,4)，执行加法操作
            total_values += value_count;
        }
    }
    for (client_id, time_count) in counters_times.iter().enumerate() {
        log_info!("Client {}: Deductions made times: {}", client_id, time_count);
    }
    log_info!("Total time deductions by all clients: {}", total_times);
    for (client_id, value_count) in counters_values.iter().enumerate() {
        log_info!("Client {}: Deductions made values: {}", client_id, value_count);
    }
    log_info!("Total value deductions by all clients: {}", total_values);

    Ok(())
}

/// 扣减账户余额（使用Redis缓存和乐观锁机制）
async fn deduct_balance(
    pg_client: Arc<Client>,
    redis_conn: Arc<Mutex<redis::aio::MultiplexedConnection>>,
    client_id: i32,
    amount: Decimal,
    deduct_type: i32,
) -> Result<bool, Box<dyn StdError + Send + Sync>> {
    loop {
        let mut redis_conn = redis_conn.lock().await;

        // 从Redis中读取balance和version
        let balance_key = "account:1:balance";
        let version_key = "account:1:version";

        let balance: Option<Decimal> = redis_conn.get(balance_key).await?;
        let version: Option<String> = redis_conn.get(version_key).await?;

        let (mut balance, mut version) = if let (Some(balance), Some(version)) = (balance, version) {
            (balance, version.parse::<i32>()?)
        } else {
            // 如果Redis缓存失效，则从PostgreSQL初始化
            let row = pg_client
                .query_one("SELECT balance, version FROM accounts WHERE id = $1", &[&1])
                .await?;
            log_info!("Client {}: Redis cache expired. Reinitialized with {:?}", client_id, row);
            let balance: Decimal = row.get(0);
            let version: i32 = row.get(1);

            // 将balance和version存入Redis
            let _: () = redis_conn.set(balance_key, balance).await?;
            let _: () = redis_conn.set(version_key, version.to_string()).await?;

            (balance, version)
        };

        let amount_change: Decimal = amount * Decimal::from_i32(deduct_type).unwrap();
        
        // 检查余额是否足够
        if deduct_type == 1 && balance < amount {
            log_info!(
                "Client {}: Insufficient balance. Needed {}, but has {}",
                client_id, amount, balance
            );
            return Ok(false); // 余额不足，结束循环
        }

        // 使用乐观锁更新PostgreSQL中的数据
        let updated_rows = pg_client
            .execute(
                "UPDATE accounts SET balance = balance - $1, version = version + 1 WHERE id = $2 AND version = $3",
                &[&amount_change, &1, &version],
            )
            .await?;

        if updated_rows == 1 {
            // 更新成功，更新Redis中的缓存
            balance -= amount_change;
            version += 1;

            let _: () = redis_conn.set(balance_key, balance.to_string()).await?;
            let _: () = redis_conn.set(version_key, version.to_string()).await?;

            let current_time = Utc::now();
            let deduct_type_str = if deduct_type == 1 { "deducted" } else { "added" };
            log_info!(
                "Client {}: Balance {} by {} at {}. Remaining balance: {}",
                client_id, deduct_type_str, amount, current_time, balance
            );
            return Ok(true);
        } else {
            // 版本号冲突，重试，重新从PostgreSQL获取最新的balance和version
            log_info!("Client {}: Version conflict, retrying...", client_id);
            // 清除 Redis 缓存的 balance_key 和 version_key
            let _: () = redis_conn.del(balance_key).await?;
            let _: () = redis_conn.del(version_key).await?;

            // 等待50毫秒后重试
            time::sleep(Duration::from_millis(50)).await;
        }
    }
}
