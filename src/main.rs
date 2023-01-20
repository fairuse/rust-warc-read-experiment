#[macro_use]
extern crate tantivy;

/* this is very rough code. It is an experiment to see if we can stream a warc archive, as stored by
   archive.org ArchiveTeam, which contains web requests and responses. We want to decode all the responses,
   run the relevant responses through an HTML parser, use XPath to extract relevant fields, and index these
   into a full text index with tantivi.
 */

use std::fs;
use std::io::prelude::*;
use std::io::{BufReader, Cursor};
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::ReloadPolicy;
use warc::WarcReader;
use warc::WarcHeader;

fn warctest() {
    let f = fs::File::open("c:\\temp\\telegram_20221103181246_61f581b9.1658771457.megawarc.warc.zst").expect("file not found");
    let mut r = BufReader::new(f);

    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).expect("unable to read file header");
    // let i = i32::from_le_bytes(buf); // .try_into().unwrap() );
    println!("magic={:?}", buf); // should [93, 42, 77, 24], magic header

    r.read_exact(&mut buf).expect("could not read header");
    let dictsize = i32::from_le_bytes(buf); // .try_into().unwrap() );
    println!("dict size = {}", dictsize);

    let mut dictbuf = vec![0u8; dictsize as usize];
    r.read_exact(&mut dictbuf).expect("could not read dictionary");

    let is_normal_dict =
        dictbuf[0] == 0x37 && dictbuf[1] == 0xA4 && dictbuf[2] == 0x30 && dictbuf[3] == 0xEC;
    let is_comp_dict =
        dictbuf[0] == 0x28 && dictbuf[1] == 0xB5 && dictbuf[2] == 0x2F && dictbuf[3] == 0xFD;

    println!(
        "normal dict: {}, comp dict: {}",
        is_normal_dict, is_comp_dict
    );
    if is_comp_dict {
        println!(
            "decompressing dict.. compressed dict len = {}",
            dictbuf.len()
        );
        // let's decompress the dictionary first.
        let dictreader = Cursor::new(dictbuf.clone());
        dictbuf.clear();
        let mut dictdecomp = zstd::Decoder::new(dictreader).expect("unable to decompress dict");
        dictdecomp
            .read_to_end(&mut dictbuf)
            .expect("failed to write decompressed dictionary");
        println!(
            "decompressing dict.. decompressed dict len = {}",
            dictbuf.len()
        );
        println!(
            "dictmagic={:#x} {:#x} {:#x} {:#x}",
            dictbuf[0], dictbuf[1], dictbuf[2], dictbuf[3]
        ); // should [93, 42, 77, 24], magic header
    }

    r.rewind().expect("could not rewind file");
    let mut br = zstd::Decoder::with_dictionary(r, &dictbuf).expect("failed to construct decoder");

    let mut wr = WarcReader::new(BufReader::new(br));


    let mut count = 0;
    let mut skipped = 0;
    let mut stream_iter = wr.stream_records();
    while let Some(record) = stream_iter.next_item() {
        let record = record.expect("read of headers ok");
        count += 1;
        match record.header(WarcHeader::TargetURI).map(|s| s.to_string()) {
            _ => {
                let buffered = record.into_buffered().expect("read of record ok");
                println!(
                    "Found record. Data:\n{}",
                    String::from_utf8_lossy(buffered.body()).len()
                );
            }
        }
    }

    println!("Total records: {}\nSkipped records: {}", count, skipped);
    //
    // let mut strm = wr.stream_records();
    // for n in 0..10 {
    //     println!("going to pick up an item from the stream of warc records...");
    //     {
    //         let record = strm
    //             .next_item()
    //             .unwrap()
    //             .unwrap()
    //             .into_buffered()
    //             .unwrap();
    //         println!("record id: {}", record.warc_id());
    //         println!("warc version: {}", record.warc_version());
    //         let q = record.body();
    //         println!("body: {:?}", q);
    //     }
    //
    // let item = strm.next_item();
    // println!("got an item, checking if it is nice");
    // match item {
    //     None => {
    //         println!("we got nothing. oh no!");
    //         break
    //     },
    //     Some(Ok(x)) => {
    //         println!("{}",x.warc_id());
    //     }
    //     Some(Err(_)) => {
    //         println!("terrible things happened");
    //         break
    //     }
    // }
// }

// let mut jsonbuf = vec![0u8; 100000];
// let err = br.read_exact(&mut jsonbuf).expect("could not read data");
// println!("got it: {} bytes read from stream", jsonbuf.len())
}

fn main() -> tantivy::Result<()> {
    warctest();

    println!("Hello, world!");
    let index_path = "./index";
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();
    let index_dir = MmapDirectory::open(&index_path)?;
    let index = Index::open_or_create(index_dir, schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;

    index_writer.add_document(doc!(
    title => "Of Mice and Men",
    body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
            bank and runs deep and green. The water is warm too, for it has slipped twinkling \
            over the yellow sands in the sunlight before reaching the narrow pool. On one \
            side of the river the golden foothill slopes curve up to the strong and rocky \
            Gabilan Mountains, but on the valley side the water is lined with trees—willows \
            fresh and green with every spring, carrying in their lower leaf junctures the \
            debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
            limbs and branches that arch over the pool"
    ));

    index_writer.commit()?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;

    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    let query = query_parser.parse_query("pool")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())
}
