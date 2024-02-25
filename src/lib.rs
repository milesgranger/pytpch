use anyhow::Result;
use arrow::pyarrow::PyArrowType;
use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use std::{collections::HashMap, io::Cursor, io::Read, sync::Arc};

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::PyResult;

mod ffi;

const DISTS_DSS: &[u8] = include_bytes!("./tpch-dbgen/dists.dss");
pub type ArrowTable = Vec<RecordBatch>;
pub type ArrowTables = HashMap<String, ArrowTable>;

#[pymodule]
fn pytpch(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(dbgen_py, m)?)?;
    m.add_class::<Table>()?;
    m.add("QUERY_1", QUERY_1)?;
    m.add("QUERY_2", QUERY_2)?;
    m.add("QUERY_3", QUERY_3)?;
    m.add("QUERY_4", QUERY_4)?;
    m.add("QUERY_5", QUERY_5)?;
    m.add("QUERY_6", QUERY_6)?;
    m.add("QUERY_7", QUERY_7)?;
    m.add("QUERY_8", QUERY_8)?;
    m.add("QUERY_9", QUERY_9)?;
    m.add("QUERY_10", QUERY_10)?;
    m.add("QUERY_11", QUERY_11)?;
    m.add("QUERY_12", QUERY_12)?;
    m.add("QUERY_13", QUERY_13)?;
    m.add("QUERY_14", QUERY_14)?;
    m.add("QUERY_15", QUERY_15)?;
    m.add("QUERY_16", QUERY_16)?;
    m.add("QUERY_17", QUERY_17)?;
    m.add("QUERY_18", QUERY_18)?;
    m.add("QUERY_19", QUERY_19)?;
    m.add("QUERY_20", QUERY_20)?;
    m.add("QUERY_21", QUERY_21)?;
    m.add("QUERY_22", QUERY_22)?;
    Ok(())
}

/// Generate the TPC-H dataset, either as a whole, by table, and/or in steps.
///
/// NOTE: This function is NOT thread-safe. The underlying C library uses a lot of global and static function
/// variables thus attempting to call this from different threads will corrupt the data generation state.
/// And anyway, right now we also temporarily change the working directory during the call and that too, isn't
/// great for threads.
///
/// Consider calling it in different processes if you want parallelism. For example using the multiprocessing module
/// or concurrent.futures.ProcessPoolExecutor to call this function.
#[pyfunction(name = "dbgen")]
pub fn dbgen_py(
    py: Python,
    sf: Option<usize>,
    table: Option<Table>,
    n_steps: Option<usize>,
    nth_step: Option<usize>,
) -> PyResult<PyObject> {
    let table_batches = py
        .allow_threads(|| dbgen(sf.unwrap_or(1), nth_step, n_steps, table))
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let pyarrow = py.import("pyarrow")?;
    let pyarrow_table_class = pyarrow.getattr("Table")?;

    // Convert to a python dict of str -> pyarrow.Table
    let mut tables = HashMap::new();
    for (name, batches) in table_batches {
        let pybatches = PyArrowType(batches).into_py(py);
        let table = pyarrow_table_class.call_method("from_batches", (pybatches,), None)?;
        tables.insert(name, table);
    }
    Ok(tables.to_object(py))
}

macro_rules! as_ptr {
    ($item:ident) => {
        $item.as_ref().map(|v| v as _).unwrap_or(std::ptr::null()) as *const _
    };
}

pub fn dbgen(
    scale: usize,
    step: Option<usize>,
    n_steps: Option<usize>, // analogous to 'children' in libdbgen
    table: Option<Table>,
) -> Result<ArrowTables> {
    // Invariants
    if let Some(n_steps) = n_steps {
        if let Some(step) = step {
            if step > n_steps {
                return Err(anyhow::Error::msg(format!(
                    "Trying to set nth_step={} and n_steps={}; nth_step must be <= n_steps",
                    step, n_steps
                )));
            }
        }
    }

    // Tempdir to write out dists.dss and write out generated data
    let current_dir = std::env::current_dir()?;
    let tempdir = tempfile::tempdir()?;
    let dists = tempdir.path().join("dists.dss");
    std::fs::write(dists, DISTS_DSS)?;

    // Call dbgen inside this temp working directory
    std::env::set_current_dir(&tempdir)?;
    let ret = unsafe {
        ffi::dbgen(
            &(scale as _),
            as_ptr!(step),
            as_ptr!(n_steps),
            as_ptr!(table),
        )
    };
    std::env::set_current_dir(current_dir)?;

    if ret != 0 {
        return Err(anyhow::Error::msg(format!(
            "Failed to generate, exit code was {}. Check stderr for dbgen errors",
            ret
        )));
    }

    let tables: ArrowTables = read_tables(&tempdir)?;

    // tempdir *should* be cleaned up on drop, but will hide any errors
    // so we'll surface them here right away instead of silently failing.
    tempdir.close()?;

    Ok(tables)
}

#[pyclass]
#[derive(Debug, Clone, Copy)]
#[repr(C)]
/*
    // Taken from dss.h in libdbgen
    #define NONE -1
    #define PART 0
    #define PSUPP 1
    #define SUPP 2
    #define CUST 3
    #define ORDER 4
    #define LINE 5
    #define ORDER_LINE 6
    #define PART_PSUPP 7
    #define NATION 8
    #define REGION 9
*/
pub enum Table {
    Part = 0isize,
    PartSupp = 1,
    Supplier = 2,
    Customer = 3,
    Orders = 4,
    Lineitem = 5,
    OrderLineitem = 6,
    PartPartSupp = 7,
    Nation = 8,
    Region = 9,
}

impl ToString for Table {
    fn to_string(&self) -> String {
        match self {
            Self::Part => "part",
            Self::PartSupp => "partsupp",
            Self::Supplier => "supplier",
            Self::Customer => "customer",
            Self::Orders => "orders",
            Self::Lineitem => "lineitem",
            Self::OrderLineitem => "order-lineitem",
            Self::PartPartSupp => "part-partsupp",
            Self::Nation => "nation",
            Self::Region => "region",
        }
        .to_string()
    }
}

fn read_tables<P: AsRef<std::path::Path>>(dir: P) -> Result<HashMap<String, Vec<RecordBatch>>> {
    let mut tables: HashMap<String, Vec<RecordBatch>> = HashMap::new();

    // List all table names in this directory
    for entry in std::fs::read_dir(dir.as_ref())? {
        let entry = entry?;

        // ie "lineitem.tbl.1"
        let name = entry.file_name().into_string().unwrap();
        if name.contains(".tbl") {
            let name = name.split_once(".tbl").unwrap().0;
            tables.entry(name.to_string()).or_default();
        }
    }

    // Format of output files
    let format = arrow_csv::reader::Format::default()
        .with_header(false)
        .with_delimiter('|' as u8);

    // for each table name, gather files to that table and add to output
    for (name, records) in tables.iter_mut() {
        let mut schema = None;
        println!("Table name: {}", &name);

        // Read in files that match this table name
        for entry in std::fs::read_dir(dir.as_ref())? {
            let entry = entry?;
            let filename = entry.file_name().into_string().unwrap();
            if filename.contains(&format!("{}.tbl", &name)) {
                let mut data = {
                    let mut file = std::fs::File::open(entry.path())?;
                    let mut data = vec![];
                    file.read_to_end(&mut data)?;
                    Cursor::new(data)
                };
                // Set schema for this table name
                if schema.is_none() {
                    schema = Some(format.infer_schema(&mut data, Some(100))?.0);
                    data.set_position(0);
                }
                let csv = ReaderBuilder::new(Arc::new(schema.clone().unwrap()))
                    .with_format(format.clone())
                    .build(&mut data)?;
                for batch in csv {
                    records.push(batch?);
                }
            }
        }
    }
    Ok(tables)
}

macro_rules! load_query {
    ($name:ident, $query_number:literal) => {
        pub const $name: &'static str = include_str!(concat!(
            "./tpch-dbgen/queries/",
            stringify!($query_number),
            ".sql"
        ));
    };
}
load_query!(QUERY_1, 1);
load_query!(QUERY_2, 2);
load_query!(QUERY_3, 3);
load_query!(QUERY_4, 4);
load_query!(QUERY_5, 5);
load_query!(QUERY_6, 6);
load_query!(QUERY_7, 7);
load_query!(QUERY_8, 8);
load_query!(QUERY_9, 9);
load_query!(QUERY_10, 10);
load_query!(QUERY_11, 11);
load_query!(QUERY_12, 12);
load_query!(QUERY_13, 13);
load_query!(QUERY_14, 14);
load_query!(QUERY_15, 15);
load_query!(QUERY_16, 16);
load_query!(QUERY_17, 17);
load_query!(QUERY_18, 18);
load_query!(QUERY_19, 19);
load_query!(QUERY_20, 20);
load_query!(QUERY_21, 21);
load_query!(QUERY_22, 22);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_1_full_gen() {
        let scale = 1;
        let step = None;
        let n_chunks = None;
        let table = None;

        let tables = dbgen(scale, step, n_chunks, table).unwrap();
        assert_eq!(tables.keys().len(), 8);

        // Verify it can run twice in a row
        let tables = dbgen(scale, step, n_chunks, table).unwrap();
        assert_eq!(tables.keys().len(), 8);
    }

    macro_rules! test_single_table_step {
        ($name:ident, $table:expr) => {
            #[test]
            fn $name() {
                let scale = 1;
                let step = Some(5);
                let n_chunks = Some(10);
                let table = Some($table);

                let tables = dbgen(scale, step, n_chunks, table).unwrap();
                assert_eq!(tables.keys().len(), 1);
                assert!(tables.get(&$table.to_string()).is_some());

                // Verify it can run twice in a row
                let tables = dbgen(scale, step, n_chunks, table).unwrap();
                assert_eq!(tables.keys().len(), 1);
                assert!(tables.get(&$table.to_string()).is_some());
            }
        };
    }

    test_single_table_step!(scale_1_single_step_table_part, Table::Part);
    test_single_table_step!(scale_1_single_step_table_partsupp, Table::PartSupp);
    test_single_table_step!(scale_1_single_step_table_orders, Table::Orders);
    test_single_table_step!(scale_1_single_step_table_customer, Table::Customer);
    test_single_table_step!(scale_1_single_step_table_region, Table::Region);
    test_single_table_step!(scale_1_single_step_table_nation, Table::Nation);
    test_single_table_step!(scale_1_single_step_table_supplier, Table::Supplier);
    test_single_table_step!(scale_1_single_step_table_lineitem, Table::Lineitem);
}
