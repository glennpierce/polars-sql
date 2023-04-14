use std::collections::HashMap;

use arrow2::array::{Array, Int32Array, Int64Array, Int16Array, Float32Array, Float64Array, 
                    Utf8Array, StructArray, BooleanArray, ListArray};

              
                    //  Date32Array, TimestampNanosecondArray

use arrow2::offset::OffsetsBuffer;
use polars_core::prelude::ArrayRef;

use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::buffer::Buffer;
// use arrow_data::{ArrayData, ArrayDataBuilder};
// use arrow2::array::{ArrayData, ArrayDataBuilder};
//use arrow2::array::mutable::{ArrayData, ArrayDataBuilder};
//use arrow2::array::data::{ArrayData, ArrayDataBuilder};
use polars::prelude::*;

use tokio_postgres::{Client, NoTls, Error, ToStatement};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::{Row, Column};

use polars_core::chunked_array::ChunkedArray; // ::from_chunks
use polars_core::datatypes::TimeUnit;

use chrono::{DateTime, Utc};

async fn postgres_to_polars(rows: &[Row]) -> std::result::Result<DataFrame, PolarsError> {

    let mut fields: Vec<(String, polars_core::datatypes::DataType)> = Vec::new();
    let column_count = rows[0].len();
    for i in 0..column_count {
        let field_name = rows[0].columns()[i].name().to_string();
        let col_type = rows[0].columns()[i].type_();
        let data_type: polars_core::datatypes::DataType = match col_type {
            &Type::BOOL => polars_core::datatypes::DataType::Boolean,
            &Type::VARCHAR => polars_core::datatypes::DataType::Utf8,
            &Type::INT2 => polars_core::datatypes::DataType::Int16,
            &Type::INT4 => polars_core::datatypes::DataType::Int32,
            &Type::INT8 => polars_core::datatypes::DataType::Int64,
            &Type::FLOAT4 => polars_core::datatypes::DataType::Float32,
            &Type::FLOAT8 => polars_core::datatypes::DataType::Float64,
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => polars_core::datatypes::DataType::Datetime(TimeUnit::Nanoseconds, Some("Utc".to_string())),
            _ => {

                // check oid
                match col_type.oid() {
                    28237 => polars_core::datatypes::DataType::List(Box::new(polars_core::datatypes::DataType::Utf8)),
                    28386 => polars_core::datatypes::DataType::Utf8,  // Enum
                    _ => unimplemented!("Unhandled column name: {} type: {:?}", field_name, col_type),
                }
            },
        };

        fields.push((field_name, data_type));
    }

    let first_row = rows.first().unwrap();

    let mut arrow_arrays: Vec<Vec<ArrayRef>> = vec![];
    
    for (col_index, column) in first_row.columns().iter().enumerate() {

        let mut array_data: Vec<ArrayRef> = vec![];

        for (_row_index, row) in rows.iter().enumerate() {
  
            let array: ArrayRef = match column.type_() {
                &Type::BOOL => Box::new(BooleanArray::from(vec![Some(row.try_get(col_index).unwrap())])),
                &Type::VARCHAR => {
                    let value = row.try_get::<usize, Option<String>>(col_index).expect("error getting column");
                    match value {
                        Some(v) => Box::new(Utf8Array::<i64>::from(vec![Some(v)])),
                        None => Box::new(Utf8Array::<i64>::new_null(DataType::LargeUtf8, 1)),
                    }
                },
                &Type::INT2 => Box::new(Int16Array::from(vec![Some(row.try_get(col_index).unwrap())])),

                &Type::INT4 => {
                    let value = row.try_get::<usize, Option<i32>>(col_index).expect("error getting column");
                    match value {
                        Some(v) => Box::new(Int32Array::from(vec![Some(v)])),
                        None => Box::new(Int32Array::new_null(DataType::Int32, 1)),
                    }
                },
                &Type::INT8 => Box::new(Int64Array::from(vec![Some(row.try_get(col_index).unwrap())])),
                &Type::FLOAT4 => Box::new(Float32Array::from(vec![Some(row.try_get(col_index).unwrap())])),
                &Type::FLOAT8 => Box::new(Float64Array::from(vec![Some(row.try_get(col_index).unwrap())])),

                &Type::TIMESTAMP | &Type::TIMESTAMPTZ => {
                    let nanos: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            let ts: DateTime<Utc> = row.try_get(col_index).unwrap();
                            let ns_since_epoch = ts.timestamp_nanos();
                            Some(ns_since_epoch)
                        })
                        .collect();

                    Box::new(Int64Array::from(nanos))

                    // let timestamp_array = Int64Array::from_vec(nanos).unwrap();
                    // Box::new(timestamp_array) as ArrayRef
                },

                _ => {
                 
                    // check oid
                    match column.type_().oid() {
                        28237 => {

                            let value: Option<HashMap<String, Option<String>>> = row.try_get::<usize, Option<HashMap<String, Option<String>>>>(col_index).expect("error getting column");
                            
                            match value {
                                Some(hm) => {

                                    let key_value_strings: Vec<Option<String>> = hm.iter().map(|(key, value)| match value {
                                                                                                                                    Some(v) => Some(format!("{} => {}", key, v)),
                                                                                                                                    None => Some(format!("{} => NULL", key)),
                                                                                                                                })
                                                                                                                                .collect();

                                    let utf8_array = Box::new(Utf8Array::<i64>::from(&key_value_strings));

                                    let data = ArrayRef::from(utf8_array);

                                    let mut offsets: Vec<i32> = Vec::with_capacity(key_value_strings.len());
                                    for i in 0..key_value_strings.len() {
                                        offsets.push(i as i32);
                                    }                                                                                            

                                    unsafe {
                                        let mut buffer: Buffer<i32> = offsets.into();
                                        let offsets_buffer = OffsetsBuffer::new_unchecked(buffer);

                                        //Box::new(ListArray::try_new(DataType::Utf8, offsets_buffer, data, None).unwrap())

                                        Box::new(Utf8Array::<i64>::new_null(DataType::LargeUtf8, 1))
                                    }
                                },
                                None => {

                                    Box::new(Utf8Array::<i64>::new_null(DataType::LargeUtf8, 1))
                                    // Box::new(ListArray::<i64>::new_null(DataType::LargeUtf8, 1))

                                },
                            }
                        },
                        28386 => {  // Enum
                            Box::new(Utf8Array::<i64>::from(vec![Some(row.try_get::<usize, String>(col_index).unwrap())]))
                        },
                        _ => unimplemented!("Unhandled column type: {:?}",  column.type_()),
                    }
                }
            };
  
            array_data.push(array);
  
        }

        arrow_arrays.push(array_data);
    }

    let mut series: Vec<Series> = vec![];

    for (array, field) in arrow_arrays.iter().zip(fields.iter()) {

        unsafe {

            let s = Series::from_chunks_and_dtype_unchecked(
                &field.0,
                array.to_vec(),
                &field.1
            );

            series.push(s);
        }
    }
    
    let df: PolarsResult<DataFrame> = DataFrame::new(series);

    df
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test1() {
        use super::*;


        let (client, connection) =
         tokio_postgres::connect("postgres://*****:password@127.0.0.1:5432/dbname", NoTls).await.unwrap();


        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let rows = client.query("select id, name, namespace, haystack_tags from sensors", &[]).await.unwrap();

        let result = postgres_to_polars(&rows).await;

        println!("{:?}", result);
    }
}



    
