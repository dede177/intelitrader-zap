use core::error;
use std::fmt::format;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use redis::{Client, streams};



const DB: &str = "/data/user/0/com.whatsapp/databases/msgstore.db";
const STREAM: &str = "whatsapp:messages";
const CONTACT_STREAM: &str = "android:contacts";
const CURSOR: &str = "/data/local/tmp/android-agent.cursor";
const CONTACT_CURSOR: &str = "/data/local/tmp/android-agent.contacts.cursor";



struct Message {
    id: u64, //_id
    timestamp: String,
    from_me: bool,
    text: String,
    chat_jid: String,
    sender_jid: String,
}

struct ContactCommand {
    id: String,
    number: String,
    name: String,
}


// DECODE SQLITE HEX OUTPUT, SO MESSAGE CANNOT BREAK THE ROW FORMAT

fn decode_hex(value: &str) -> Result<String, String> {
    if value.len() % 2 != 0 {
        return Err("Invalid hex string length".to_string());
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).map_err(|_| "Invalid UTF-8 in hex string")?;
            u8::from_str_radix(pair, 16).map_err(|_| "Invalid hex digit")
        })
        .collect::<Result<Vec<_>, &_>>()?;
    Ok(String::from_utf8(bytes).map_err(|_| "Invalid UTF-8")?)
}


fn parse_row(line: &str) -> Result<Message, String> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() != 6 {
        return Err("Invalid row format".to_string());
    }
    if fields[1] != "0" && fields[1] != "1" {
        return Err("Invalid from_me value".into());
    }
    Ok(Message {
        id: fields[0].parse::<u64>().map_err(|e| e.to_string())?,
        timestamp: fields[2].to_string(),
        from_me: fields[1] == "1",
        text: decode_hex(fields[3])?,
        chat_jid: decode_hex(fields[4])?,
        sender_jid: decode_hex(fields[5])?,
    })
}

// OPEN SQL LIVE DB IN READ ONLY, POLL 100 ROWS.

fn read_messages(after: u64) -> Result<Vec<Message>, String> {
    let sql = format!(
        "SELECT m._id,m.from_me,COALESCE(m.timestamp,0),hex(COALESCE(m.text_data,'')),\
        hex(COALESCE(cj.raw_string,cj.user||'@'||cj.server, '')),\
        hex(COALESCE(sj.raw_string,sj.user||'@'||sj.server,\
        cj.raw_string,cj.user||'@'||cj.server, '')) \
        FROM message m LEFT JOIN chat c ON c._id=m.chat_row_id \
        LEFT JOIN jid cj ON cj._id=c.jid_row_id \
        LEFT JOIN jid sj ON sj._id=m.sender_jid_row_id \
        WHERE m._id>{after} ORDER BY m._id LIMIT 100"
    );
    let output = Command::new("/system/bin/sqlite3")
        .args(["-readonly", "-tabs", DB, &sql])
        .output()
        .map_err(|e| format!("start sqlite3: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "sqlite3: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let output = String::from_utf8(output.stdout).map_err(|e| e.to_string())?;
    output.lines().map(parse_row).collect()

}


// NORMALIZE PHONE NNUMBER

fn normalize_phone(number: &str) -> Option<String> {
    let number = number.trim();
    let digits = number.strip_prefix('+').unwrap_or(number);
    if !(7..=15).contains(&digits.len()) || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    Some(format!("+{digits}"))
}

fn content (args: &[&str]) -> Result<String, String> {
    let output = Command::new("/system/bin/content")
        .args(args)
        .output()
        .map_err(|e| format!("Start content: {e}"))?;
    if !output.status.success() || !output.stderr.is_empty() {
        return Err(format!(
            "content: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| e.to_string())
}


fn contact_not_empty(name: &str, number: &str) -> Result<(), String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("contact name is empty".into())
    }
    let phone = normalize_phone(number).ok_or("invalid number")?;
    let phones = content(&[
        "query",
        "--uri",
        "content://com.android.contacts/data/phones",
        "--projection",
        "data1",
    ])?;
    if phones 
        .lines()
        .any(|line| line.ends_with(&format!("data1={phone}")))
        {
            return Ok(());
        }

        let source = format!("android-agent-{}", &phone[1..]);
        let raw = content(&[
            "query",
            "--uri",
            "content://com.android.contacts/raw_contacts",
            "--projection",
            "_id:sourceid",
        ])?;
// aqui crio um raw_id sem conta associada em 'content --bind' <- indica NULL 
        let mut raw_id = find_raw_id(&raw, &source);
        if raw_id.is_none() {
            let binding = format!("sourceid:s:{source}");
            content(&[
                "insert",
                "--uri",
                "content://com.android.contacts/raw_contacts",
                "--bind",
                "account_name:n:",
                "--bind",
                "account_type:n:",
                "--bind",
                &binding,

            ])?;

            let raw = content(&[
                "query",
                "--uri",
                "content://com.android.contacts/raw_contacts",
                "--projection",
                "_id:sourceid",
            ])?;
            raw_id = find_raw_id(&raw, &source);
        }

        let raw_id = raw_id.ok_or("contact inserted was not found")?;
        content(&[
            "insert",
            "--uri",
            "content://com.android.contacts/data",
            "--bind",
            &format!("raw_contact_id:i:{raw_id}"),
            "--bind",
            "mimetype:s:vnd.android.cursor.item/name",
            "--bind",
            &format!("data1:s:{name}"),
        ])?;

        content(&[
            "insert",
            "--uri",
            "content://com.android.contacts/data",
            "--bind",
            &format!("raw_contact_id:i:{raw_id}"),
            "--bind",
            "mimetype:s:vnd.android.cursor.item/phone_v2",
            "--bind",
            &format!("data1:s:{phone}"),            
        ])?;
        Ok(())
}


// IDENTIFY CREATED RAW CONTACT BY TABLE SOURCE
fn find_raw_id(rows: &str, source: &str) -> Option<u64> {
    rows.lines().find_map(|line| {
        if !line.ends_with(&format!("sourceid={source}")) {
            return None;
        }
        line.split("_id=")
            .nth(1)?
            .split(',')
            .next()?
            .parse::<u64>()
            .ok()
    })
}


fn readis_connection(adress: &str) -> Result<redis::Connection, String> {
    let client = redis::Client::open(format!("redis://{adress}"))
        .map_err(|e| format!("Redis adress: {e}"))?;
    let connection = client
        .get_connection_with_timeout(Duration::from_secs(2))
        .map_err(|e| format!("connect redis: {e}"))?;
    connection
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    connection
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| e.to_string())?;
    Ok(connection)
}

fn publish(adress: &str, message: &Message) -> Result<(), String> {
    let mut connection = readis_connection(adress)?;
    message_commando(message)
        .query::<String>(&mut connection)
        .map(|_| ())
        .map_err(|e| format!("Redis XADD: {e}"))
}

fn decode_contact_commands(
    response: redis::streams::StreamReadReply,
) -> Result<Vec<ContactCommand>, String> {
    let mut commands = Vec::new();
    for stream in response.keys {
        if stream.key != CONTACT_STREAM {
            return Err("Unexpected stream reply".into());
        }
        for entry in stream.ids {
            let mut fields = entry.map;
            let name = fields
                .remove("name")
                .ok_or("redis contact missing name")?;
            let number = fields
                .remove("number")
                .ok_or("redis contact command is missing number")?;
            commands.push(ContactCommand {
                id: entry.id,
                name: redis::from_redis_value(name).map_err(|e| e.to_string())?,
                number: redis::from_redis_value(number).map_err(|e| e.to_string())?,

            });
        }
    }
    Ok(commands)
}

fn read_contact_commands(
    connection: &mut redis::Connection,
    position: &str,
) -> Result<Vec<ContactCommand>, String> {
    let response: Option<redis::streams::StreamReadReply> = redis::cmd("XREAD")
        .arg("COUNT")
        .arg(10)
        .arg("BLOCK")
        .arg(1000)
        .arg("STREAMS")
        .arg(CONTACT_STREAM)
        .arg(position)
        .query(connection)
        .map_err(|e| format!("Redis XREAD: {e}"))?;
    response
        .map(decode_contact_commands)
        .unwrap_or(Ok(Vec::new()))
}

fn load_contact_cursor() -> Result<String, String> {
    if Path::new(CONTACT_CURSOR).exists() {
        let value = fs::read_to_string(CONTACT_CURSOR).map_err(|e| e.to_string())?;
        let value = value.trim();
        if value.is_empty() {
            return Err("empty contact stream cursor".into());
        }
        Ok(value.to_string())
    } else {
        Ok("0-0".into())
    }
}

fn save_cursor_file(path: &str, value: &str) -> Result<(), String> {
    let temporary = format!("{path}.tmp");
    let mut file = File::create(&temporary).map_err(|e| e.to_string())?;
    writeln!(file, "{value}").map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    fs::rename(temporary, path).map_err(|e| e.to_string())
}

fn save_contact_cursor(value: &str) -> Result<(), String> {
    save_cursor_file(CONTACT_CURSOR, value)
}

fn save_cursor(value: u64) -> Result<(), String> {
    save_cursor_file(CURSOR, &value.to_string())
}


fn contact_loop(adress: String) {
    let mut position = match load_contact_cursor() {
        Ok(position) => position,
        Err(error) => {
            eprintln!("contact cursor: {error}");
            "0-0".into()
        }
    };
    loop {
        let mut connection = match readis_connection(&adress) {
            Ok(connection) => connection,
            Err(error) => {
                eprintln!("contact stream: {error}");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        loop {
            match read_contact_commands(&mut connection, &position) {
                Ok(commands) => {
                    let mut failed = false;
                    for command in commands {
                        if let Err(error) = contact_not_empty(&command.name, &command.number)
                            .and_then(|_| save_contact_cursor(&command.id))
                        {
                            eprintln!("contact command {}: {error}", command.id);
                            failed = true;
                            break;
                        }

                        position = command.id;
                        println!("inserted contact command_id={position}");
                    }

                    if failed {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
                Err(error) => {
                    eprintln!("contact stream: {error}");
                    std::thread::sleep(Duration::from_secs(1));
                    break;
                }
            }
        }
    }

}

fn process_messages(
    messages: Vec<Message>,
    cursor: &mut u64,
    mut publish_message: impl FnMut(&Message) -> Result<(), String>,
    mut checkpoint: impl FnMut(u64) -> Result<(), String>,
) -> Result<usize, String> {
    let count = messages.len();
    let mut saved = *cursor;
    for message in messages {
        if !message.from_me && !message.text.is_empty() {
            if let Err(error) = publish_message(&message).and_then(|_| checkpoint(message.id)) {
                *cursor = saved;
                return Err(error);
            }
            saved = message.id;
            println!("published message_id={}", message.id);
        }
        *cursor = message.id;
    }
    Ok(count)
}

fn poll(adress: &str, cursor: &mut u64) -> Result<usize, String> {
    process_messages(
        read_messages(*cursor)?,
        cursor,
        |message| publish(adress, message),
        save_cursor,

    )
}


fn main() -> Result<(), String> {
    let args: Vec<_> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--help") {
        println!(
            "modo de uso: android-agent [--once] [redis-host:port]\n defualt Redis: 10.0.2.2:6379"
        );
        return Ok(());
    }

    let once = args.iter().any(|arg| arg == "--once");
    let adress = args
        .iter()
        .skip(1)
        .find(|arg| !arg.starts_with('-'))
        .map(String::as_str)
        .unwrap_or("10.0.2.2:6379");

    if !once {
        let contact_adress = adress.to_string();
        std::thread::spawn(move || contact_loop(contact_adress));

    }

    let mut cursor = if Path::new(CURSOR).exists() {
        fs::read_to_string(CURSOR)
            .map_err(|e| e.to_string())?
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("cursor invalid: {e}"))?
    } else {
        0
    };
    loop {
        match poll(adress, &mut cursor) {
            Ok(100) => continue,
            Ok(_) if once => return Ok(()),
            Ok(_) => (),
            Err(error) if once => return Err(error),
            Err(error) => eprintln!("poll: {error}"),
        }

        std::thread::sleep(Duration::from_millis(500));
    }
}
