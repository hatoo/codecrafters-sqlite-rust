use anyhow::{bail, Result};
use regex::RegexBuilder;
use std::fmt::{self, Display};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::vec;

#[derive(Debug)]
#[allow(dead_code)]
struct Table {
    ty: String,
    name: String,
    tbl_name: String,
    rootpage: u32,
    sql: String,
}

#[derive(Debug, PartialEq, PartialOrd)]
enum Column {
    Integer(i64),
    Text(String),
    NULL,
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Column::Integer(i) => write!(f, "{}", i),
            Column::Text(s) => write!(f, "{}", s),
            Column::NULL => write!(f, "NULL"),
        }
    }
}

type Row = Vec<Column>;

fn variant(buf: &[u8]) -> (u64, &[u8]) {
    let mut i = 0;
    let mut v = 0;
    loop {
        let byte = buf[i];
        v = (v << 7) | (byte & 0x7f) as u64;
        if byte & 0x80 == 0 {
            break;
        }
        i += 1;
    }
    (v, &buf[i + 1..])
}

fn tables(first_page: &[u8]) -> Vec<Table> {
    assert_eq!(first_page[100], 0x0d);
    let number_of_cells = u16::from_be_bytes([first_page[103], first_page[104]]);

    let cell_indices = (0..number_of_cells as usize)
        .map(|i| u16::from_be_bytes([first_page[100 + 8 + 2 * i], first_page[100 + 8 + 2 * i + 1]]))
        .collect::<Vec<_>>();

    cell_indices
        .into_iter()
        .map(|i| {
            let cell = &first_page[i as usize..];
            let (_payload_length, cell) = variant(cell);
            let (_row_id, cell) = variant(cell);
            // assume header length is 1 byte
            let header_length = cell[0];
            let header = &cell[1..header_length as usize];
            let mut cell = &cell[header_length as usize..];

            // type text
            let (t, header) = variant(header);
            assert!(t >= 13 && t % 2 == 1);
            let length = ((t - 13) / 2) as usize;
            let ty = std::str::from_utf8(&cell[..length]).unwrap();
            cell = &cell[length..];

            // name text
            let (t, header) = variant(header);
            assert!(t >= 13 && t % 2 == 1);
            let length = ((t - 13) / 2) as usize;
            let name = std::str::from_utf8(&cell[..length]).unwrap();
            cell = &cell[length..];

            // tbl_name text
            let (t, header) = variant(header);
            assert!(t >= 13 && t % 2 == 1);
            let length = ((t - 13) / 2) as usize;
            let tbl_name = std::str::from_utf8(&cell[..length]).unwrap();
            cell = &cell[length..];

            // rootpage integer
            let (t, header) = variant(header);
            assert_eq!(t, 1);
            let rootpage = cell[0] as u32;
            cell = &cell[1..];

            // sql text
            let (t, _header) = variant(header);
            assert!(t >= 13 && t % 2 == 1);
            let length = ((t - 13) / 2) as usize;
            let sql = std::str::from_utf8(&cell[..length]).unwrap();

            Table {
                ty: ty.to_string(),
                name: name.to_string(),
                tbl_name: tbl_name.to_string(),
                rootpage,
                sql: sql.to_string(),
            }
        })
        .collect()
}

fn rows(page: &[u8]) -> Vec<Row> {
    // Assume leaf page
    let number_of_cells = u16::from_be_bytes([page[3], page[4]]);

    let cell_indices = (0..number_of_cells as usize)
        .map(|i| u16::from_be_bytes([page[8 + 2 * i], page[8 + 2 * i + 1]]))
        .collect::<Vec<_>>();

    cell_indices
        .into_iter()
        .map(|i| {
            let cell = &page[i as usize..];

            let (_payload_length, cell) = variant(cell);
            let (_row_id, cell) = variant(cell);
            // assume header length is 1 byte
            let header_length = cell[0];
            let mut header = &cell[1..header_length as usize];
            let mut cell = &cell[header_length as usize..];

            let mut row = vec![];

            while !header.is_empty() {
                let (t, header_) = variant(header);
                header = header_;

                match t {
                    0 => row.push(Column::NULL),
                    1 => {
                        row.push(Column::Integer(cell[0] as i64));
                        cell = &cell[1..];
                    }
                    t if t >= 13 && t % 2 == 1 => {
                        let length = ((t - 13) / 2) as usize;
                        let text = std::str::from_utf8(&cell[..length]).unwrap();
                        row.push(Column::Text(text.to_string()));
                        cell = &cell[length..];
                    }
                    _ => unimplemented!("type {}", t),
                }
            }

            row
        })
        .collect()
}

fn sql_column_names(sql: &str) -> Vec<String> {
    let inner_bracket: String = sql
        .chars()
        .skip_while(|c| *c != '(')
        .skip(1)
        .take_while(|c| *c != ')')
        .collect();
    inner_bracket
        .split(',')
        .map(|s| s.trim().split_whitespace().next().unwrap().to_string())
        .collect()
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let mut file = File::open(&args[1])?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    let page_size = u16::from_be_bytes([header[16], header[17]]);

    let mut first_page = vec![0; page_size as usize - 100];
    file.read_exact(&mut first_page)?;
    let first_page = first_page;

    let number_of_cells = u16::from_be_bytes([first_page[3], first_page[4]]);

    // Parse command and act accordingly
    let command = &args[2];
    if command == ".dbinfo" {
        println!("database page size: {}", page_size);
        println!("number of tables: {}", number_of_cells);
    } else if command == ".tables" {
        let mut first_page = vec![0; page_size as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut first_page)?;
        let first_page = first_page;

        let tables = tables(&first_page);
        println!(
            "{}",
            tables
                .into_iter()
                .filter(|t| t.name != "sqlite_sequence")
                .map(|t| t.name)
                .collect::<Vec<_>>()
                .join(" ")
        );
    } else if command.to_uppercase().starts_with("SELECT COUNT(*) FROM") {
        let table_name = command.split_whitespace().nth(3).unwrap();

        let mut first_page = vec![0; page_size as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut first_page)?;
        let first_page = first_page;

        let tables = tables(&first_page);

        let root_page = tables
            .into_iter()
            .find(|t| t.name == table_name)
            .unwrap()
            .rootpage;

        let mut page = vec![0; page_size as usize];
        file.seek(SeekFrom::Start((root_page - 1) as u64 * page_size as u64))?;
        file.read_exact(&mut page)?;
        let page = page;
        let number_of_cells = u16::from_be_bytes([page[3], page[4]]);

        println!("{}", number_of_cells);
    } else if let Some(captures) = RegexBuilder::new(r"SELECT (.+) FROM (\w+)( WHERE (.+))?")
        .case_insensitive(true)
        .build()
        .unwrap()
        .captures(command)
    {
        let table_name = captures.get(2).unwrap().as_str();
        let column_names = captures.get(1).unwrap().as_str();
        let column_names = column_names
            .split(',')
            .map(|s| s.trim())
            .collect::<Vec<_>>();
        let where_clause = captures.get(4).map(|m| m.as_str());

        let mut first_page = vec![0; page_size as usize];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut first_page)?;
        let first_page = first_page;

        let tables = tables(&first_page);

        let table = tables.into_iter().find(|t| t.name == table_name).unwrap();

        let sql_coumn_names = sql_column_names(table.sql.as_str());

        let indices: Vec<usize> = column_names
            .into_iter()
            .map(|c| sql_coumn_names.iter().position(|s| s == &c).unwrap())
            .collect();

        let mut page = vec![0; page_size as usize];
        file.seek(SeekFrom::Start(
            (table.rootpage - 1) as u64 * page_size as u64,
        ))?;
        file.read_exact(&mut page)?;

        let equals = if let Some(where_clause) = where_clause {
            let mut equals = Vec::new();
            let mut iter = where_clause.split('=');
            let column_name = iter.next().unwrap().trim();
            let value = iter
                .next()
                .unwrap()
                .trim()
                .trim_start_matches('\'')
                .trim_end_matches('\'');
            let column_index = sql_coumn_names
                .iter()
                .position(|s| s == &column_name)
                .unwrap();
            equals.push((column_index, value));
            equals
        } else {
            Vec::new()
        };

        let rows = rows(&page).into_iter().filter(|row| {
            equals
                .iter()
                .all(|(column_index, value)| row[*column_index] == Column::Text(value.to_string()))
        });

        for row in rows {
            println!(
                "{}",
                indices
                    .iter()
                    .map(|&i| row[i].to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            );
        }
    } else {
        bail!("Missing or invalid command passed: {}", command)
    }

    Ok(())
}
