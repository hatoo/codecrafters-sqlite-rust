use anyhow::{bail, Result};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

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
            assert_eq!(first_page[0], 0x0d);
            let cell_indices = (0..number_of_cells as usize)
                .map(|i| u16::from_be_bytes([first_page[8 + 2 * i], first_page[8 + 2 * i + 1]]))
                .collect::<Vec<_>>();

            let mut first_page = vec![0; page_size as usize];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut first_page)?;
            let first_page = first_page;

            let mut table_names = vec![];

            for i in cell_indices {
                let cell = &first_page[i as usize..];
                let (_payload_length, cell) = variant(cell);
                let (_row_id, cell) = variant(cell);
                // assume header length is 1 byte
                let header_length = cell[0];
                let mut header = &cell[1..header_length as usize];
                let mut cell = &cell[header_length as usize..];

                let mut i = 0;

                while !header.is_empty() {
                    let (t, header_) = variant(header);
                    header = header_;
                    match t {
                        1 => {
                            cell = &cell[1..];
                        }
                        t => {
                            assert!(t >= 13);
                            let length = ((t - 13) / 2) as usize;
                            let text = std::str::from_utf8(&cell[..length]).unwrap();
                            if i == 1 && text != "sqlite_sequence" {
                                table_names.push(text);
                            }
                            cell = &cell[length..];
                        }
                    }
                    i += 1;
                }
            }
            println!("{}", table_names.join(" "));
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
