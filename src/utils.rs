use foundationdb::tuple::{unpack, Element};

#[allow(dead_code)]
pub fn key_to_plain_text(key: &[u8]) -> String {
    let elements: Vec<Element> = unpack(key).unwrap();

    elements
        .iter()
        .map(|element| match element {
            Element::Nil => "nil".to_string(),
            Element::Bytes(bytes) => format!("{:?}", bytes),
            Element::String(str) => str.to_string(),
            Element::Int(value) => i64::to_string(value),
            Element::Float(value) => f32::to_string(value),
            Element::Double(value) => f64::to_string(value),
            Element::Bool(value) => bool::to_string(value),
            _ => "NA".to_string(),
        })
        .collect::<Vec<String>>()
        .join("/")
}
