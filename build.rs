fn main() {
    let files = &[
        "src/tpch-dbgen/build.c",
        "src/tpch-dbgen/driver.c",
        "src/tpch-dbgen/bm_utils.c",
        "src/tpch-dbgen/rnd.c",
        "src/tpch-dbgen/print.c",
        "src/tpch-dbgen/load_stub.c",
        "src/tpch-dbgen/bcd2.c",
        "src/tpch-dbgen/speed_seed.c",
        "src/tpch-dbgen/text.c",
        "src/tpch-dbgen/permute.c",
        "src/tpch-dbgen/rng64.c",
    ];
    for file in files {
        println!("cargo:rerun-if-changed={}", file);
    }
    println!("cargo:rerun-if-changed=build.rs");

    cc::Build::new()
        .compiler("clang")
        .files(files)
        .include("src/tpch-dbgen")
        .define("DBNAME", Some("dss"))
        .define("LINUX", None)
        .define("VECTORWISE", None)
        .define("TPCH", None)
        .define("RNG_TEST", None)
        .define("_FILE_OFFSET_BITS", Some("64"))
        .flag("-g")
        .flag("-MJcompile_commands.json")
        .flag("-Wno-unused-parameter")
        .flag("-Wno-unused-variable")
        .flag("-Wno-missing-field-initializers")
        .flag("-Wno-unused-but-set-variable")
        .flag("-Wno-dangling-else")
        .flag("-Wno-format")
        .flag("-Wno-misleading-indentation")
        .flag("-Wno-deprecated-non-prototype")
        .flag("-Wno-string-plus-int")
        .compile("dbgen");
}
