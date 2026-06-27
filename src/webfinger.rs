use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JrdLink {
    pub rel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub titles: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JrdResource {
    #[serde(rename = "subject")]
    pub subject: Option<String>,
    #[serde(rename = "aliases", default)]
    pub aliases: Vec<String>,
    #[serde(rename = "properties", default)]
    pub properties: HashMap<String, String>,
    #[serde(rename = "links", default)]
    pub links: Vec<JrdLink>,
}

impl JrdResource {
    pub fn empty() -> Self {
        Self {
            subject: None,
            aliases: Vec::new(),
            properties: HashMap::new(),
            links: Vec::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum Error {
    #[error("invalid JRD format")]
    InvalidFormat,
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn parse_jrd(bytes: &[u8]) -> Result<JrdResource, Error> {
    let resource: JrdResource = serde_json::from_slice(bytes)?;
    Ok(resource)
}

pub fn merge_jrd(responses: Vec<(u16, JrdResource)>) -> JrdResource {
    let mut result = JrdResource::empty();
    let mut seen_links: HashSet<(String, Option<String>)> = HashSet::new();

    for (_, mut resp) in responses {
        if result.subject.is_none() {
            result.subject = resp.subject.take();
        }

        result.aliases.extend(resp.aliases);
        result.properties.extend(resp.properties);

        for link in resp.links {
            let key = (link.rel.clone(), link.href.clone());
            if seen_links.insert(key) {
                result.links.push(link);
            }
        }
    }

    result
}

pub fn to_json_bytes(resource: &JrdResource) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(resource).map_err(Error::Json)
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn test_parse_activitypub_response() {
        let json = r#"{"subject":"acct:user@example.com","aliases":["https://social.example.com/users/user","https://social.example.com/@user"],"links":[{"rel":"http://webfinger.net/rel/profile-page","type":"text/html","href":"https://social.example.com/@user"},{"rel":"self","type":"application/activity+json","href":"https://social.example.com/users/user"}]}"#;
        let jrd = parse_jrd(json.as_bytes()).expect("should parse ActivityPub response");
        assert_eq!(jrd.subject, Some("acct:user@example.com".to_string()));
        assert_eq!(jrd.aliases.len(), 2);
        assert_eq!(jrd.links.len(), 2);
        assert!(jrd.properties.is_empty());
    }

    #[test]
    fn test_url_construction_with_resource() {
        let base = url::Url::parse("http://social.example.svc.cluster.local:8080").unwrap();
        let url = base.join(".well-known/webfinger").unwrap();
        let resource = "acct:user@example.com";
        let url = url.join(&format!("?resource={resource}")).unwrap();
        assert_eq!(
            url.as_str(),
            "http://social.example.svc.cluster.local:8080/.well-known/webfinger?resource=acct:user@example.com"
        );
    }

    #[test]
    fn test_parse_response_without_properties() {
        let json = r#"{"subject":"acct:registry@example.com","aliases":["https://registry.example.com/ap/actor"],"links":[{"rel":"self","type":"application/activity+json","href":"https://registry.example.com/ap/actor"}]}"#;
        let jrd = parse_jrd(json.as_bytes()).expect("should parse response missing properties field");
        assert_eq!(jrd.subject, Some("acct:registry@example.com".to_string()));
        assert_eq!(jrd.aliases.len(), 1);
        assert_eq!(jrd.links.len(), 1);
        assert!(jrd.properties.is_empty());
    }
}
