#[macro_use]
extern crate tantivy;
//
// #[experimental]
// extern crate zstd;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::ReloadPolicy;
use zstd::Decoder;
use std::fs;
use std::io::prelude::*;
use std::io::{BufReader, Cursor};

fn warctest() {
    let f = fs::File::open("c:\\temp\\test.warc.zst").expect("file not found");
    let mut r = BufReader::new(f);

    let mut buf = [0u8; 4];
    let err = r.read_exact(&mut buf);
    // let i = i32::from_le_bytes(buf); // .try_into().unwrap() );
    println!("magic={:?}", buf); // should [93, 42, 77, 24], magic header

    let err = r.read_exact(&mut buf).expect("could not read header");
    let dictsize = i32::from_le_bytes(buf); // .try_into().unwrap() );
    println!("dict size = {}", dictsize);

    let mut dictbuf = vec![0u8; dictsize as usize];
    let err = r.read_exact(&mut dictbuf).expect("could not read dictionary");

    let is_normal_dict = dictbuf[0] == 0x37 && dictbuf[1] == 0xA4 && dictbuf[2] == 0x30 && dictbuf[3] == 0xEC;
    let is_comp_dict = dictbuf[0] == 0x28 && dictbuf[1] == 0xB5 && dictbuf[2] == 0x2F && dictbuf[3] == 0xFD;

    println!("normal dict: {}, comp dict: {}", is_normal_dict, is_comp_dict);
    if is_comp_dict {
        println!("decompressing dict.. compressed dict len = {}", dictbuf.len());
        // let's decompress the dictionary first.
        let dictreader = Cursor::new(dictbuf.clone());
        dictbuf.clear();
        let mut dictdecomp = zstd::Decoder::new(dictreader).expect("unable to decompress dict");
        dictdecomp.read_to_end(&mut dictbuf).expect("failed to write decompressed dictionary");
        println!("decompressing dict.. decompressed dict len = {}", dictbuf.len());
        println!("dictmagic={:#x} {:#x} {:#x} {:#x}", dictbuf[0],dictbuf[1],dictbuf[2],dictbuf[3]); // should [93, 42, 77, 24], magic header
    }

    r.rewind().expect("could not rewind file");
    let mut br = zstd::Decoder::with_dictionary(r, &dictbuf).expect("failed to construct decoder");

    // br.include_magicbytes(false).expect("could not disable including magic bytes?");
    // HOW DO I GET THIS TO WORK? EXPERIMENTAL: br.include_magic_bytes(false);
    let mut jsonbuf = vec!(0u8; 100000);
    let err = br.read_exact(&mut jsonbuf).expect("could not read data");
    println!("got it: {} bytes read from stream", jsonbuf.len() )
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
