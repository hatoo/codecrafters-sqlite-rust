use anyhow::{bail, Result};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

struct Table {
    ty: String,
    name: String,
    tbl_name: String,
    rootpage: u32,
    sql: String,
}

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
            let mut header = &cell[1..header_length as usize];
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
    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", page_size);
            println!("number of tables: {}", number_of_cells);
        }
        ".tables" => {
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
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
