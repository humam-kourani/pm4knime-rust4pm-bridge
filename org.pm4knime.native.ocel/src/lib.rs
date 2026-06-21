use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use jni::objects::{JByteArray, JClass};
use jni::sys::{jbyteArray, jint, jlong};
use jni::JNIEnv;
use once_cell::sync::Lazy;
use process_mining::ocel::ocel_struct::{OCELAttributeValue, OCELType};
use process_mining::{
    import_ocel_json_from_path, import_ocel_sqlite_from_path, import_ocel_xml_file, OCEL,
};
use serde::{Deserialize, Serialize};

const TABLE_EVENT_TYPES: &str = "event_types";
const TABLE_OBJECT_TYPES: &str = "object_types";
const TABLE_EVENTS: &str = "events";
const TABLE_OBJECTS: &str = "objects";
const TABLE_EVENT: &str = TABLE_EVENTS;
const TABLE_OBJECT: &str = TABLE_OBJECTS;
const TABLE_EVENT_OBJECT: &str = "event_object";
const TABLE_OBJECT_OBJECT: &str = "object_object";
const TABLE_EVENT_MAP_TYPE: &str = "event_map_type";
const TABLE_OBJECT_MAP_TYPE: &str = "object_map_type";

const COL_ID: &str = "id";
const COL_NAME: &str = "name";
const COL_TYPE: &str = "type";
const COL_TIME: &str = "time";
const COL_ATTRIBUTES: &str = "attributes";
const COL_RELATIONSHIPS: &str = "relationships";
const COL_OCEL_ID: &str = COL_ID;
const COL_OCEL_TYPE: &str = COL_TYPE;
const COL_OCEL_TIME: &str = COL_TIME;
const COL_OCEL_CHANGED_FIELD: &str = "ocel_changed_field";
const COL_OCEL_EVENT_ID: &str = "ocel_event_id";
const COL_OCEL_OBJECT_ID: &str = "ocel_object_id";
const COL_OCEL_QUALIFIER: &str = "ocel_qualifier";
const COL_OCEL_SOURCE_ID: &str = "ocel_source_id";
const COL_OCEL_TARGET_ID: &str = "ocel_target_id";
const COL_OCEL_TYPE_MAP: &str = "ocel_type_map";

static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);
static REGISTRY: Lazy<Mutex<HashMap<i64, NativeOcel>>> = Lazy::new(|| Mutex::new(HashMap::new()));

struct NativeOcel {
    ocel: OCEL,
    metadata: Metadata,
    load_nanos: u64,
}

#[derive(Clone)]
struct Metadata {
    event_types: Vec<TypeDefinition>,
    object_types: Vec<TypeDefinition>,
    event_table_by_type: Vec<(String, String)>,
    object_table_by_type: Vec<(String, String)>,
    tables: Vec<TableDefinition>,
}

#[derive(Clone, Serialize)]
struct TypeDefinition {
    name: String,
    attributes: Vec<AttributeDefinition>,
}

#[derive(Clone, Serialize)]
struct AttributeDefinition {
    name: String,
    value_type: String,
}

#[derive(Clone)]
struct TableDefinition {
    name: String,
    kind: String,
    row_count: usize,
    columns: Vec<ColumnDefinition>,
}

#[derive(Clone, Serialize)]
struct ColumnDefinition {
    name: String,
    #[serde(rename = "type")]
    logical_type: String,
}

#[derive(Serialize)]
struct SpecPayload<'a> {
    backend: &'a str,
    load_nanos: u64,
    tables: Vec<TablePayload<'a>>,
}

#[derive(Serialize)]
struct TablePayload<'a> {
    name: &'a str,
    kind: &'a str,
    row_count: usize,
    columns: &'a [ColumnDefinition],
}

#[derive(Serialize)]
struct RowsPayload {
    rows: Vec<Vec<Option<String>>>,
}

#[derive(Deserialize)]
struct QueryRequest {
    operation: Option<String>,
    table: String,
}

#[derive(Serialize)]
struct OperationCatalog {
    operations: Vec<OperationDefinition>,
}

#[derive(Serialize)]
struct OperationDefinition {
    name: &'static str,
    description: &'static str,
    input: &'static str,
    output: &'static str,
}

#[no_mangle]
pub extern "system" fn Java_org_pm4knime_portobject_NativeOCELBridge_loadNative(
    mut env: JNIEnv,
    _class: JClass,
    path_bytes: JByteArray,
    format_bytes: JByteArray,
) -> jlong {
    match load_native(&mut env, path_bytes, format_bytes) {
        Ok(handle) => handle as jlong,
        Err(message) => {
            throw_runtime_exception(&mut env, &message);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_pm4knime_portobject_NativeOCELBridge_freeNative(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle <= 0 {
        return;
    }
    if let Ok(mut registry) = REGISTRY.lock() {
        registry.remove(&(handle as i64));
    }
}

#[no_mangle]
pub extern "system" fn Java_org_pm4knime_portobject_NativeOCELBridge_specJsonNative(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jbyteArray {
    match with_native(handle, |native| spec_json(native)) {
        Ok(bytes) => to_java_bytes(&mut env, &bytes),
        Err(message) => {
            throw_runtime_exception(&mut env, &message);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_pm4knime_portobject_NativeOCELBridge_operationCatalogJsonNative(
    mut env: JNIEnv,
    _class: JClass,
) -> jbyteArray {
    match operation_catalog_json() {
        Ok(bytes) => to_java_bytes(&mut env, &bytes),
        Err(message) => {
            throw_runtime_exception(&mut env, &message);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_pm4knime_portobject_NativeOCELBridge_queryArrowIpcNative(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_bytes: JByteArray,
) -> jbyteArray {
    let query_json = match read_utf8(&mut env, query_bytes) {
        Ok(value) => value,
        Err(message) => {
            throw_runtime_exception(&mut env, &message);
            return std::ptr::null_mut();
        }
    };
    match with_native(handle, |native| query_arrow_ipc(native, &query_json)) {
        Ok(bytes) => to_java_bytes(&mut env, &bytes),
        Err(message) => {
            throw_runtime_exception(&mut env, &message);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_pm4knime_portobject_NativeOCELBridge_tableRowsJsonNative(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    table_name_bytes: JByteArray,
    offset: jlong,
    limit: jint,
) -> jbyteArray {
    let table_name = match read_utf8(&mut env, table_name_bytes) {
        Ok(value) => value,
        Err(message) => {
            throw_runtime_exception(&mut env, &message);
            return std::ptr::null_mut();
        }
    };
    if offset < 0 || limit < 0 {
        throw_runtime_exception(&mut env, "Offset and limit must be non-negative.");
        return std::ptr::null_mut();
    }
    match with_native(handle, |native| {
        table_rows_json(native, &table_name, offset as usize, limit as usize)
    }) {
        Ok(bytes) => to_java_bytes(&mut env, &bytes),
        Err(message) => {
            throw_runtime_exception(&mut env, &message);
            std::ptr::null_mut()
        }
    }
}

fn load_native(
    env: &mut JNIEnv,
    path_bytes: JByteArray,
    format_bytes: JByteArray,
) -> Result<i64, String> {
    let path = read_utf8(env, path_bytes)?;
    let format = read_utf8(env, format_bytes)?;
    let start = Instant::now();
    let ocel = load_ocel(&path, &format)?;
    let load_nanos = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
    let metadata = build_metadata(&ocel);
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    let native = NativeOcel {
        ocel,
        metadata,
        load_nanos,
    };
    REGISTRY
        .lock()
        .map_err(|_| "Native OCEL registry is poisoned.".to_string())?
        .insert(handle, native);
    Ok(handle)
}

fn load_ocel(path: &str, format: &str) -> Result<OCEL, String> {
    let path_ref = Path::new(path);
    let lower = format.to_ascii_lowercase();
    match lower.as_str() {
        "json" | "jsonocel" => import_ocel_json_from_path(path_ref)
            .map_err(|err| format!("Rust OCEL JSON import failed: {err}")),
        "xml" | "xmlocel" => catch_unwind(AssertUnwindSafe(|| import_ocel_xml_file(path_ref)))
            .map_err(|_| "Rust OCEL XML import panicked.".to_string()),
        "sqlite" | "sqlite3" | "db" | "db3" => import_ocel_sqlite_from_path(path_ref)
            .map_err(|err| format!("Rust OCEL SQLite import failed: {err}")),
        _ => Err(format!(
            "Unsupported OCEL format for Rust native reader: {format}"
        )),
    }
}

fn with_native<T, F>(handle: jlong, f: F) -> Result<T, String>
where
    F: FnOnce(&NativeOcel) -> Result<T, String>,
{
    if handle <= 0 {
        return Err("Invalid native OCEL handle.".to_string());
    }
    let registry = REGISTRY
        .lock()
        .map_err(|_| "Native OCEL registry is poisoned.".to_string())?;
    let native = registry
        .get(&(handle as i64))
        .ok_or_else(|| format!("Native OCEL handle {handle} is not available."))?;
    f(native)
}

fn spec_json(native: &NativeOcel) -> Result<Vec<u8>, String> {
    let tables = native
        .metadata
        .tables
        .iter()
        .map(|table| TablePayload {
            name: &table.name,
            kind: &table.kind,
            row_count: table.row_count,
            columns: &table.columns,
        })
        .collect();
    serde_json::to_vec(&SpecPayload {
        backend: "Rust process_mining 0.3.19 JNI",
        load_nanos: native.load_nanos,
        tables,
    })
    .map_err(|err| format!("Could not serialize native OCEL metadata: {err}"))
}

fn operation_catalog_json() -> Result<Vec<u8>, String> {
    serde_json::to_vec(&OperationCatalog {
        operations: vec![
            OperationDefinition {
                name: "ocel.load",
                description: "Load an OCEL file into a native process_mining::OCEL handle.",
                input: "{path, format}",
                output: "native OCEL handle",
            },
            OperationDefinition {
                name: "ocel.query.table",
                description: "Return one native OCEL top-level collection as an Arrow IPC stream.",
                input: "{handle, table}",
                output: "Arrow IPC stream bytes",
            },
        ],
    })
    .map_err(|err| format!("Could not serialize native operation catalog: {err}"))
}

fn query_arrow_ipc(native: &NativeOcel, query_json: &str) -> Result<Vec<u8>, String> {
    let request: QueryRequest = serde_json::from_str(query_json)
        .map_err(|err| format!("Could not parse native OCEL query JSON: {err}"))?;
    if let Some(operation) = request.operation.as_deref() {
        if operation != "ocel.query.table" {
            return Err(format!(
                "Unsupported native OCEL query operation: {operation}"
            ));
        }
    }
    table_arrow_ipc(native, &request.table)
}

fn table_rows_json(
    native: &NativeOcel,
    table_name: &str,
    offset: usize,
    limit: usize,
) -> Result<Vec<u8>, String> {
    let table = native
        .metadata
        .tables
        .iter()
        .find(|table| table.name == table_name)
        .ok_or_else(|| format!("Native OCEL table '{table_name}' is not available."))?;
    let rows = rows_for_table(native, table, offset, limit)?;
    serde_json::to_vec(&RowsPayload { rows })
        .map_err(|err| format!("Could not serialize native OCEL table rows: {err}"))
}

fn build_metadata(ocel: &OCEL) -> Metadata {
    let event_types = event_type_definitions(ocel);
    let object_types = object_type_definitions(ocel);
    let mut tables = Vec::new();

    tables.push(TableDefinition {
        name: TABLE_EVENT_TYPES.to_string(),
        kind: "native OCEL collection".to_string(),
        row_count: event_types.len(),
        columns: vec![string_column(COL_NAME), string_column(COL_ATTRIBUTES)],
    });
    tables.push(TableDefinition {
        name: TABLE_OBJECT_TYPES.to_string(),
        kind: "native OCEL collection".to_string(),
        row_count: object_types.len(),
        columns: vec![string_column(COL_NAME), string_column(COL_ATTRIBUTES)],
    });
    tables.push(TableDefinition {
        name: TABLE_EVENTS.to_string(),
        kind: "native OCEL collection".to_string(),
        row_count: ocel.events.len(),
        columns: vec![
            string_column(COL_ID),
            string_column(COL_TYPE),
            string_column(COL_TIME),
            string_column(COL_ATTRIBUTES),
            string_column(COL_RELATIONSHIPS),
        ],
    });
    tables.push(TableDefinition {
        name: TABLE_OBJECTS.to_string(),
        kind: "native OCEL collection".to_string(),
        row_count: ocel.objects.len(),
        columns: vec![
            string_column(COL_ID),
            string_column(COL_TYPE),
            string_column(COL_ATTRIBUTES),
            string_column(COL_RELATIONSHIPS),
        ],
    });

    Metadata {
        event_types,
        object_types,
        event_table_by_type: Vec::new(),
        object_table_by_type: Vec::new(),
        tables,
    }
}

fn event_type_definitions(ocel: &OCEL) -> Vec<TypeDefinition> {
    let mut definitions = type_definitions(&ocel.event_types);
    for event in &ocel.events {
        let definition = ensure_definition(&mut definitions, &event.event_type);
        for attribute in &event.attributes {
            add_attribute_if_missing(definition, &attribute.name, "string");
        }
    }
    definitions
}

fn object_type_definitions(ocel: &OCEL) -> Vec<TypeDefinition> {
    let mut definitions = type_definitions(&ocel.object_types);
    for object in &ocel.objects {
        let definition = ensure_definition(&mut definitions, &object.object_type);
        for attribute in &object.attributes {
            add_attribute_if_missing(definition, &attribute.name, "string");
        }
    }
    definitions
}

fn type_definitions(types: &[OCELType]) -> Vec<TypeDefinition> {
    let mut result = Vec::new();
    for item in types {
        if result
            .iter()
            .any(|existing: &TypeDefinition| existing.name == item.name)
        {
            continue;
        }
        result.push(TypeDefinition {
            name: item.name.clone(),
            attributes: item
                .attributes
                .iter()
                .map(|attribute| AttributeDefinition {
                    name: attribute.name.clone(),
                    value_type: attribute.value_type.clone(),
                })
                .collect(),
        });
    }
    result
}

fn ensure_definition<'a>(
    definitions: &'a mut Vec<TypeDefinition>,
    name: &str,
) -> &'a mut TypeDefinition {
    if let Some(index) = definitions
        .iter()
        .position(|definition| definition.name == name)
    {
        return &mut definitions[index];
    }
    definitions.push(TypeDefinition {
        name: name.to_string(),
        attributes: Vec::new(),
    });
    definitions
        .last_mut()
        .expect("definition was just inserted")
}

fn add_attribute_if_missing(definition: &mut TypeDefinition, name: &str, value_type: &str) {
    if !definition
        .attributes
        .iter()
        .any(|attribute| attribute.name == name)
    {
        definition.attributes.push(AttributeDefinition {
            name: name.to_string(),
            value_type: value_type.to_string(),
        });
    }
}

fn type_table_names(
    prefix: &str,
    definitions: &[TypeDefinition],
    used_names: &mut HashSet<String>,
) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for definition in definitions {
        let preferred = format!("{prefix}_{}", sanitize_type_name(&definition.name));
        let table_name = unique_name(&preferred, used_names);
        used_names.insert(table_name.clone());
        result.push((definition.name.clone(), table_name));
    }
    result
}

fn table_name_for_type(table_by_type: &[(String, String)], type_name: &str) -> String {
    table_by_type
        .iter()
        .find(|(candidate, _)| candidate == type_name)
        .map(|(_, table)| table.clone())
        .unwrap_or_else(|| sanitize_type_name(type_name))
}

fn sanitize_type_name(label: &str) -> String {
    let mut value = String::new();
    let mut previous_was_separator = false;
    for ch in label.trim().to_ascii_lowercase().chars() {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            value.push(ch);
            previous_was_separator = false;
        } else if !previous_was_separator && !value.is_empty() {
            value.push('_');
            previous_was_separator = true;
        }
    }
    while value.ends_with('_') {
        value.pop();
    }
    if value.is_empty() {
        value = "type".to_string();
    }
    if value.len() > 80 {
        value.truncate(80);
    }
    value
}

fn unique_name(preferred: &str, used_names: &HashSet<String>) -> String {
    if !used_names.contains(preferred) {
        return preferred.to_string();
    }
    let mut suffix = 1;
    loop {
        let candidate = format!("{preferred}_{suffix}");
        if !used_names.contains(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

fn attribute_columns(attributes: &[AttributeDefinition]) -> Vec<ColumnDefinition> {
    let mut used = HashSet::new();
    let mut columns = Vec::new();
    for attribute in attributes {
        let preferred = attribute.name.clone();
        let name = unique_name(&preferred, &used);
        used.insert(name.clone());
        columns.push(ColumnDefinition {
            name,
            logical_type: logical_type(&attribute.value_type),
        });
    }
    columns
}

fn string_column(name: &str) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        logical_type: "string".to_string(),
    }
}

fn logical_type(declared_type: &str) -> String {
    let lowered = declared_type.to_ascii_lowercase();
    if lowered.contains("bool") {
        "boolean"
    } else if lowered.contains("int") || lowered.contains("long") {
        "integer"
    } else if lowered.contains("float")
        || lowered.contains("double")
        || lowered.contains("number")
        || lowered.contains("real")
    {
        "float"
    } else {
        "string"
    }
    .to_string()
}

fn table_arrow_ipc(native: &NativeOcel, table_name: &str) -> Result<Vec<u8>, String> {
    match table_name {
        TABLE_EVENT_TYPES => arrow_for_type_definitions(&native.metadata.event_types),
        TABLE_OBJECT_TYPES => arrow_for_type_definitions(&native.metadata.object_types),
        TABLE_EVENTS => arrow_for_events(native),
        TABLE_OBJECTS => arrow_for_objects(native),
        _ => Err(format!(
            "Native OCEL table '{table_name}' is not available. Available native collections: {}, {}, {}, {}.",
            TABLE_EVENT_TYPES, TABLE_OBJECT_TYPES, TABLE_EVENTS, TABLE_OBJECTS
        )),
    }
}

fn arrow_for_type_definitions(definitions: &[TypeDefinition]) -> Result<Vec<u8>, String> {
    let names = definitions
        .iter()
        .map(|definition| definition.name.clone())
        .collect::<Vec<_>>();
    let attributes = definitions
        .iter()
        .map(|definition| json_string(&definition.attributes))
        .collect::<Result<Vec<_>, _>>()?;
    write_arrow_ipc(
        vec![COL_NAME, COL_ATTRIBUTES],
        vec![string_array(names), string_array(attributes)],
    )
}

fn arrow_for_events(native: &NativeOcel) -> Result<Vec<u8>, String> {
    let ids = native
        .ocel
        .events
        .iter()
        .map(|event| event.id.clone())
        .collect::<Vec<_>>();
    let types = native
        .ocel
        .events
        .iter()
        .map(|event| event.event_type.clone())
        .collect::<Vec<_>>();
    let times = native
        .ocel
        .events
        .iter()
        .map(|event| event.time.to_rfc3339())
        .collect::<Vec<_>>();
    let attributes = native
        .ocel
        .events
        .iter()
        .map(|event| json_string(&event.attributes))
        .collect::<Result<Vec<_>, _>>()?;
    let relationships = native
        .ocel
        .events
        .iter()
        .map(|event| json_string(&event.relationships))
        .collect::<Result<Vec<_>, _>>()?;
    write_arrow_ipc(
        vec![
            COL_ID,
            COL_TYPE,
            COL_TIME,
            COL_ATTRIBUTES,
            COL_RELATIONSHIPS,
        ],
        vec![
            string_array(ids),
            string_array(types),
            string_array(times),
            string_array(attributes),
            string_array(relationships),
        ],
    )
}

fn arrow_for_objects(native: &NativeOcel) -> Result<Vec<u8>, String> {
    let ids = native
        .ocel
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect::<Vec<_>>();
    let types = native
        .ocel
        .objects
        .iter()
        .map(|object| object.object_type.clone())
        .collect::<Vec<_>>();
    let attributes = native
        .ocel
        .objects
        .iter()
        .map(|object| json_string(&object.attributes))
        .collect::<Result<Vec<_>, _>>()?;
    let relationships = native
        .ocel
        .objects
        .iter()
        .map(|object| json_string(&object.relationships))
        .collect::<Result<Vec<_>, _>>()?;
    write_arrow_ipc(
        vec![COL_ID, COL_TYPE, COL_ATTRIBUTES, COL_RELATIONSHIPS],
        vec![
            string_array(ids),
            string_array(types),
            string_array(attributes),
            string_array(relationships),
        ],
    )
}

fn string_array(values: Vec<String>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

fn write_arrow_ipc(column_names: Vec<&str>, arrays: Vec<ArrayRef>) -> Result<Vec<u8>, String> {
    let fields = column_names
        .into_iter()
        .map(|name| Field::new(name, DataType::Utf8, false))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
        .map_err(|err| format!("Could not create native OCEL Arrow record batch: {err}"))?;
    let mut bytes = Vec::new();
    {
        let cursor = Cursor::new(&mut bytes);
        let mut writer = StreamWriter::try_new(cursor, &schema)
            .map_err(|err| format!("Could not create native OCEL Arrow IPC writer: {err}"))?;
        writer
            .write(&batch)
            .map_err(|err| format!("Could not write native OCEL Arrow IPC batch: {err}"))?;
        writer
            .finish()
            .map_err(|err| format!("Could not finish native OCEL Arrow IPC stream: {err}"))?;
    }
    Ok(bytes)
}

fn json_string<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value)
        .map_err(|err| format!("Could not serialize native OCEL nested value: {err}"))
}

fn rows_for_table(
    native: &NativeOcel,
    table: &TableDefinition,
    offset: usize,
    limit: usize,
) -> Result<Vec<Vec<Option<String>>>, String> {
    if limit == 0 || offset >= table.row_count {
        return Ok(Vec::new());
    }
    match table.name.as_str() {
        TABLE_EVENT_TYPES => Ok(slice_rows(
            native.metadata.event_types.iter().map(|definition| {
                vec![
                    some(definition.name.clone()),
                    json_string(&definition.attributes).ok(),
                ]
            }),
            offset,
            limit,
        )),
        TABLE_OBJECT_TYPES => Ok(slice_rows(
            native.metadata.object_types.iter().map(|definition| {
                vec![
                    some(definition.name.clone()),
                    json_string(&definition.attributes).ok(),
                ]
            }),
            offset,
            limit,
        )),
        TABLE_EVENTS => Ok(slice_rows(
            native.ocel.events.iter().map(|event| {
                vec![
                    some(event.id.clone()),
                    some(event.event_type.clone()),
                    some(event.time.to_rfc3339()),
                    json_string(&event.attributes).ok(),
                    json_string(&event.relationships).ok(),
                ]
            }),
            offset,
            limit,
        )),
        TABLE_OBJECTS => Ok(slice_rows(
            native.ocel.objects.iter().map(|object| {
                vec![
                    some(object.id.clone()),
                    some(object.object_type.clone()),
                    json_string(&object.attributes).ok(),
                    json_string(&object.relationships).ok(),
                ]
            }),
            offset,
            limit,
        )),
        _ => Err(format!(
            "Native OCEL table '{}' is not recognized.",
            table.name
        )),
    }
}

fn rows_for_type_table(
    native: &NativeOcel,
    table: &TableDefinition,
    offset: usize,
    limit: usize,
) -> Result<Vec<Vec<Option<String>>>, String> {
    if let Some((type_name, _)) = native
        .metadata
        .event_table_by_type
        .iter()
        .find(|(_, table_name)| table_name == &table.name)
    {
        let definition = native
            .metadata
            .event_types
            .iter()
            .find(|definition| definition.name == *type_name)
            .ok_or_else(|| format!("Missing event type definition for '{type_name}'."))?;
        return Ok(slice_rows(
            native
                .ocel
                .events
                .iter()
                .filter(move |event| event.event_type == *type_name)
                .map(move |event| {
                    let mut row = vec![some(event.id.clone()), some(event.time.to_rfc3339())];
                    for attribute in &definition.attributes {
                        row.push(
                            event
                                .attributes
                                .iter()
                                .find(|candidate| candidate.name == attribute.name)
                                .map(|candidate| attribute_value_to_string(&candidate.value)),
                        );
                    }
                    row
                }),
            offset,
            limit,
        ));
    }
    if let Some((type_name, _)) = native
        .metadata
        .object_table_by_type
        .iter()
        .find(|(_, table_name)| table_name == &table.name)
    {
        let definition = native
            .metadata
            .object_types
            .iter()
            .find(|definition| definition.name == *type_name)
            .ok_or_else(|| format!("Missing object type definition for '{type_name}'."))?;
        return Ok(slice_rows(
            native
                .ocel
                .objects
                .iter()
                .filter(move |object| object.object_type == *type_name)
                .flat_map(move |object| {
                    object.attributes.iter().map(move |changed_attribute| {
                        let mut row = vec![
                            some(object.id.clone()),
                            some(changed_attribute.time.to_rfc3339()),
                            some(changed_attribute.name.clone()),
                        ];
                        for attribute in &definition.attributes {
                            if attribute.name == changed_attribute.name {
                                row.push(some(attribute_value_to_string(&changed_attribute.value)));
                            } else {
                                row.push(None);
                            }
                        }
                        row
                    })
                }),
            offset,
            limit,
        ));
    }
    Err(format!(
        "Native OCEL table '{}' is not recognized.",
        table.name
    ))
}

fn slice_rows<I>(rows: I, offset: usize, limit: usize) -> Vec<Vec<Option<String>>>
where
    I: Iterator<Item = Vec<Option<String>>>,
{
    rows.skip(offset).take(limit).collect()
}

fn attribute_value_to_string(value: &OCELAttributeValue) -> String {
    match value {
        OCELAttributeValue::Time(value) => value.to_rfc3339(),
        OCELAttributeValue::Integer(value) => value.to_string(),
        OCELAttributeValue::Float(value) => value.to_string(),
        OCELAttributeValue::Boolean(value) => value.to_string(),
        OCELAttributeValue::String(value) => value.clone(),
        OCELAttributeValue::Null => String::new(),
    }
}

fn some(value: String) -> Option<String> {
    Some(value)
}

fn read_utf8(env: &mut JNIEnv, array: JByteArray) -> Result<String, String> {
    let bytes = env
        .convert_byte_array(array)
        .map_err(|err| format!("Could not read Java byte array: {err}"))?;
    String::from_utf8(bytes).map_err(|err| format!("Expected UTF-8 data from Java: {err}"))
}

fn to_java_bytes(env: &mut JNIEnv, bytes: &[u8]) -> jbyteArray {
    match env.byte_array_from_slice(bytes) {
        Ok(array) => array.into_raw(),
        Err(err) => {
            throw_runtime_exception(env, &format!("Could not create Java byte array: {err}"));
            std::ptr::null_mut()
        }
    }
}

fn throw_runtime_exception(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", message);
}
