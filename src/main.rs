use chrono::Utc;
use rand::Rng;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use std::sync::{Arc, Mutex};
use tokio::time::{self, Duration};
use tokio_postgres::{NoTls, Client};
use futures::future::join_all;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // PostgreSQL连接初始化
    let pg_url = "host=192.168.123.16 user=buste password=passw0rd dbname=kafka_example";
    let (pg_client, pg_connection) = tokio_postgres::connect(pg_url, NoTls).await?;

    // 启动数据库连接任务
    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    // 将pg_client包装成Arc
    let pg_client = Arc::new(pg_client);

    // 初始化5个客户端
    let total_clients = 5;
    // 计数器，用于记录每个客户端的扣费次数
    let counters_times = Arc::new(Mutex::new(vec![0; total_clients]));
    let counters_values = Arc::new(Mutex::new(vec![0; total_clients]));

    // 模拟多个客户端竞争修改数据库
    let tasks: Vec<_> = (0..5)
        .map(|client_id| {
            let pg_client = Arc::clone(&pg_client);
            let counters_times = Arc::clone(&counters_times);
            let counters_values = Arc::clone(&counters_values);
            tokio::spawn(async move {
                // 每个客户端不断尝试扣费，直到余额不足
                let mut current_amount = Decimal::from_f64(10.0).unwrap(); // 初始扣费金额为 10.0
                let deduct_type;
                if (client_id.to_i32().unwrap() as usize) <= total_clients/2 {
                    deduct_type = 1;
                } else { 
                    deduct_type = -1;
                }
                loop {
                    match deduct_balance(pg_client.clone(), client_id, current_amount, deduct_type).await {
                        Ok(true) => {
                            // 如果扣费成功，累加随机金额
                            let mut rng = rand::thread_rng();
                            let random_increment: f64 = rng.gen_range(1.0..=5.0);
                            
                            // 更新扣费次数计数器
                            let mut counters_times = counters_times.lock().unwrap();
                            let mut counters_values = counters_values.lock().unwrap();
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
                            eprintln!("Client {} failed: {}", client_id, e);
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
    let counters_times = counters_times.lock().unwrap();
    let counters_values = counters_values.lock().unwrap();
    let total_times: i32 = counters_times.iter().sum();
    let total_values: i32 = counters_values.iter().sum();
    for (client_id, time_count) in counters_times.iter().enumerate() {
        println!("Client {}: Deductions made times: {}", client_id, time_count);
    }
    println!("Total time deductions by all clients: {}", total_times);
    for (client_id, value_count) in counters_values.iter().enumerate() {
        println!("Client {}: Deductions made values: {}", client_id, value_count);
    }
    println!("Total value deductions by all clients: {}", total_values);

    Ok(())
}

/// 扣减账户余额（使用乐观锁机制）
async fn deduct_balance(
    pg_client: Arc<Client>,
    client_id: i32,
    amount: Decimal,
    deduct_type: i32,
) -> Result<bool, Box<dyn std::error::Error>> {
    loop {
        // 读取当前余额和版本号
        let row = pg_client
            .query_one("SELECT balance, version FROM accounts WHERE id = $1", &[&1])
            .await?;

        let balance: Decimal = row.get(0);
        let version: i32 = row.get(1);
        let amount_change: Decimal = amount * Decimal::from_i32(deduct_type).unwrap();
        
        // 检查余额是否足够
        if deduct_type == 1 && balance < amount {
            println!(
                "Client {}: Insufficient balance. Needed {}, but has {}",
                client_id, amount, balance
            );
            return Ok(false); // 余额不足，结束循环
        }

        // 尝试更新余额，带版本号检查
        let updated_rows = pg_client
            .execute(
                "UPDATE accounts SET balance = balance - $1, version = version + 1 WHERE id = $2 AND version = $3",
                &[&amount_change, &1, &version],
            )
            .await?;

        if updated_rows == 1 {
            // 更新成功，记录日志并打印客户端ID和时间
            let current_time = Utc::now();
            let deduct_type_str = if deduct_type == 1 { "deducted" } else { "added" };
            println!(
                "Client {}: Balance {} by {} at {}. Remaining balance: {}",
                client_id, deduct_type_str, amount, current_time, balance - amount
            );
            return Ok(true);
        } else {
            // 更新失败，可能版本号不匹配，重试
            println!("Client {}: Version conflict, retrying...", client_id);
            time::sleep(Duration::from_millis(50)).await;
        }
    }
}
